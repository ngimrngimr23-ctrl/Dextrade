"""
Пул резидентных HTTP-прокси с ротацией и поадресными кулдаунами.

Зачем пул, а не один прокси. Лимиты и баны на площадках считаются ПО АДРЕСУ:
CSFloat даёт 200 запросов в час на IP, Steam банит IP целиком при 429. С одним
адресом это жёсткий потолок, из-за которого скан приходилось растягивать на
пять минут между прогонами. Шесть адресов — это шесть независимых бюджетов, и
дальше упирается уже не в лимит, а в здравый смысл.

Ключевая деталь, ради которой всё и написано: когда адрес получает 429, мы
помечаем кулдаун ИМЕННО ЕМУ и продолжаем с другого, а не останавливаем работу
целиком. Прежний код умел только одно — уснуть на час после первого же отказа.

Формат переменной окружения — один прокси или несколько через запятую,
пробел или перевод строки:

    http://логин:пароль@хост:порт
    http://u1:p1@host:7001, http://u2:p2@host:7002

ВАЖНО: в строках лежат пароли. Наружу (логи, /status) они уходят ТОЛЬКО через
describe()/mask() — не печатать сырые значения.
"""

from __future__ import annotations

import logging
import re
import time

import envcfg
from typing import NamedTuple
from urllib.parse import urlsplit, urlunsplit

log = logging.getLogger("steam_bot.proxy")

# Разделители: запятая, точка с запятой, пробелы и переводы строк. Пробел в
# списке — самая вероятная опечатка при вставке в поле Render, поэтому режем
# по любому из них, а не только по запятой.
_SPLIT_RE = re.compile(r"[\s,;]+")

# Схемы, которые умеет aiohttp. SOCKS5 требует отдельного пакета aiohttp-socks,
# которого в requirements нет, — молча купить SOCKS-порт слишком легко.
_SUPPORTED_SCHEMES = ("http", "https")


def mask(url: str) -> str:
    """Адрес прокси без логина и пароля — для логов и /status."""
    try:
        parsed = urlsplit(url)
        if not parsed.hostname:
            return "адрес не разобрался (ожидается http://логин:пароль@хост:порт)"
        if parsed.username or parsed.password:
            return urlunsplit((parsed.scheme, f"***@{parsed.hostname}:{parsed.port}", "", "", ""))
        return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
    except Exception:
        return "адрес не разобрался (ожидается http://логин:пароль@хост:порт)"


def validate(url: str) -> str | None:
    """Что не так с адресом прокси, если не так. None — всё в порядке."""
    parsed = urlsplit(url)
    if parsed.scheme not in _SUPPORTED_SCHEMES:
        return (
            f"схема {parsed.scheme or 'не указана'!r} не поддерживается — aiohttp умеет "
            "только http/https. Возьми у провайдера HTTP-порт (обычно тот же хост, "
            "другой номер порта)"
        )
    if not parsed.hostname or not parsed.port:
        return "не разобрать хост и порт — нужен формат http://логин:пароль@хост:порт"
    return None


class AddResult(NamedTuple):
    """
    Итог добавления адресов. Три исхода различаются намеренно: «добавлено»,
    «уже были» и «не приняты» требуют от пользователя совершенно разных
    действий, а сведённые в одно число они выглядят как невесть откуда взявшийся
    лимит.
    """

    added: int
    duplicates: int
    rejected: list[tuple[str, str]]

    @property
    def seen(self) -> int:
        """Сколько адресов вообще разобрано из присланного текста."""
        return self.added + self.duplicates + len(self.rejected)


class ProxyPool:
    """
    Набор равноправных прокси с перебором по кругу.

    Кулдаун держится отдельно для каждого адреса: исчерпанная квота на одном
    ничего не говорит про остальные. Пока есть хоть один свободный, работа
    продолжается.
    """

    def __init__(self, raw: str, name: str = "proxy"):
        self.name = name
        self.proxies: list[str] = []
        self.problems: list[tuple[str, str]] = []  # (замаскированный адрес, что не так)
        self._cooldowns: dict[str, float] = {}
        # Разные логины, отказавшие подряд с прошлого успеха. См.
        # note_gateway_refusal.
        self._gateway_strikes: set[str] = set()
        self.dead: dict[str, str] = {}
        # Когда мёртвому адресу дать шанс снова. См. DEAD_RETRY_SECONDS.
        self._dead_until: dict[str, float] = {}
        # Отказы подряд по каждому адресу — см. mark_refused/mark_ok.
        self._refusals: dict[str, int] = {}
        self._cursor = 0

        for candidate in _SPLIT_RE.split(raw or ""):
            candidate = candidate.strip()
            if not candidate:
                continue
            problem = validate(candidate)
            if problem:
                # Плохой адрес не роняет весь пул: остальные рабочие, а про
                # этот честно сообщаем в /status. Иначе одна опечатка в списке
                # из шести выключала бы прокси целиком и молча.
                self.problems.append((mask(candidate), problem))
                continue
            if candidate not in self.proxies:
                self.proxies.append(candidate)

        # Снимок адресов из переменной окружения — единственное, что remove()
        # не имеет права трогать. Всё, что появится в self.proxies позже
        # (через add()), пришло из /proxyadd и может быть убрано тем же путём.
        self._base = frozenset(self.proxies)

    def add(self, raw: str) -> AddResult:
        """
        Добавить адреса на ходу. Ограничения на количество нет.

        Нужно, чтобы пополнять пул командой из Telegram, а не передеплоем ради
        каждого нового адреса. Дубликаты по-прежнему отбрасываются (при
        копировании списками они неизбежны), но теперь СЧИТАЮТСЯ и попадают в
        ответ. Раньше они пропадали молча, и это прямо вводило в заблуждение:
        вставив двадцать адресов, из которых тринадцать уже были в пуле,
        пользователь видел «Добавлено: 7» и делал вывод, что бот дальше семи
        не пускает. Никакого потолка не было — просто отчёт умалчивал о
        половине разобранного.
        """
        added = 0
        duplicates = 0
        rejected: list[tuple[str, str]] = []
        for candidate in _SPLIT_RE.split(raw or ""):
            candidate = candidate.strip()
            if not candidate:
                continue
            problem = validate(candidate)
            if problem:
                rejected.append((mask(candidate), problem))
                continue
            if candidate in self.proxies:
                duplicates += 1
                continue
            self.proxies.append(candidate)
            added += 1
        return AddResult(added=added, duplicates=duplicates, rejected=rejected)

    def from_env(self) -> list[str]:
        """Адреса, пришедшие из переменной окружения (пережившие все add/remove)."""
        return [p for p in self.proxies if p in self._base]

    def extra(self) -> list[str]:
        """
        Адреса, добавленные на ходу через add(), без пришедших из переменной
        окружения. Именно их (и только их) имеет смысл класть в хранилище:
        env-адреса подхватятся сами при следующем старте, а их дубль в
        хранилище потом не даёт понять, что вообще добавлял пользователь.
        """
        return [p for p in self.proxies if p not in self._base]

    def remove(self, addresses: list[str], *, include_env: bool = False) -> int:
        """
        Убрать конкретные адреса из УЖЕ РАБОТАЮЩЕГО пула — без рестарта процесса.

        Нужен парой к add(): /proxyadd действует немедленно, а /proxyclear до
        этого метода — только наполовину. Он чистил хранилище (так что при
        следующем рестарте адреса не вернутся), но сам пул в памяти их не
        трогал: 8 адресов, добавленных через бота, продолжали числиться в
        proxies (пусть и мёртвыми после mark_dead) до тех пор, пока Render не
        передеплоит процесс сам по себе — то есть неопределённо долго.

        Адреса из self._base (переменная окружения) по умолчанию не трогает —
        их штатно снимают правкой самой переменной и рестартом. Но когда
        include_env=True, убирает и их: если провайдер отключил аккаунт целиком
        (403 на всех сессиях), ждать передеплоя ради того, чтобы перестать
        долбить мёртвые адреса, бессмысленно. При следующем старте они всё
        равно вернутся из переменной — это ожидаемо, и вызывающий код обязан
        сказать об этом пользователю.

        Возвращает, сколько реально убрано.
        """
        to_remove = set(addresses)
        if not include_env:
            to_remove -= self._base
        if not to_remove:
            return 0
        self.proxies = [p for p in self.proxies if p not in to_remove]
        for p in to_remove:
            self._cooldowns.pop(p, None)
            self.dead.pop(p, None)
        return len(to_remove)

    # Сколько отказов подряд терпим, прежде чем счесть адрес нерабочим.
    # Меньше трёх ставить нельзя: у 403 полно ВРЕМЕННЫХ причин (лимит
    # одновременных сессий, протухшая sticky-сессия, разовый сбой на стороне
    # провайдера), и хоронить адрес с первого раза — значит выключать рабочий
    # прокси навсегда из-за минутной заминки. Ровно это и случилось
    # 2026-08-23: единственный живой адрес пользователя получил один 403 и
    # был вычеркнут до перезапуска процесса.
    REFUSALS_BEFORE_DEAD = 3

    # Через сколько дать мёртвому адресу шанс снова.
    #
    # Раньше mark_dead была дверью в одну сторону: адрес исключался из
    # available() и next(), а вернуть его мог только mark_ok — который
    # вызывается после УСПЕШНОГО запроса через этот адрес. То есть воскреснуть
    # он не мог никогда, потому что запросов ему больше не давали. До конца
    # жизни процесса.
    #
    # На плавающем 403 это выедает пул на глазах: три отказа подряд — минус
    # логин, и 2026-09-13 за пятнадцать минут пул усох с 96 до 86, а сообщение
    # «0 свободных из 96» стало «0 свободных из 86». При такой скорости пул
    # кончается за пару часов, и лечится это только передеплоем.
    #
    # Пятнадцать минут выбраны так, чтобы заминка провайдера успела пройти, но
    # по-настоящему сломанный логин не мешал работать: он снова провалится,
    # снова уйдёт в мёртвые и попробует ещё через пятнадцать минут.
    DEAD_RETRY_SECONDS = envcfg.env_int("PROXY_DEAD_RETRY_SECONDS", 15 * 60)

    def _revive_expired(self) -> None:
        """Вернуть в строй адреса, отлежавшие свой срок в мёртвых."""
        if not self._dead_until:
            return
        now = time.time()
        for proxy, until in list(self._dead_until.items()):
            if until <= now:
                self._dead_until.pop(proxy, None)
                if self.dead.pop(proxy, None) is not None:
                    self._refusals.pop(proxy, None)
                    log.info(
                        "%s: адрес %s снова в строю — отлежал %d мин, даю ещё попытку",
                        self.name, mask(proxy), self.DEAD_RETRY_SECONDS // 60,
                    )

    def mark_refused(self, proxy: str, cooldown_seconds: float, reason: str = "") -> bool:
        """
        Прокси отказал в обслуживании (403/407 на CONNECT).

        Первые отказы трактуем как временные — адрес уходит на кулдаун и
        вернётся сам. Только когда отказы идут подряд REFUSALS_BEFORE_DEAD раз,
        признаём адрес нерабочим: тогда это уже похоже на настоящую проблему с
        доступом, а не на заминку.

        Возвращает True, если адрес признан мёртвым.
        """
        if not proxy:
            return False
        count = self._refusals.get(proxy, 0) + 1
        self._refusals[proxy] = count
        if count >= self.REFUSALS_BEFORE_DEAD:
            self.mark_dead(proxy, f"{count} отказ(ов) подряд: {reason}" if reason else "отказы подряд")
            log.warning(
                "%s: адрес %s признан нерабочим после %d отказов подряд%s",
                self.name, mask(proxy), count, f" ({reason})" if reason else "",
            )
            return True
        self.mark_exhausted(proxy, cooldown_seconds, reason or "отказ прокси")
        return False

    # Сколько РАЗНЫХ логинов должны отказать подряд, чтобы поверить, что
    # отказывает сам шлюз, а не отдельная сессия.
    #
    # Число обязано быть ЗАМЕТНО БОЛЬШЕ числа одновременных воркеров, иначе
    # порог не спасает: воркеров четыре, они уходят в сеть одновременно и при
    # общей заминке дают четыре отказа в одну секунду — с порогом «4» пул
    # сложился бы ровно так же, как складывался до правки, и прогон снова
    # встал бы на первом предмете. При SCAN_CONCURRENCY=4 восемь означает, что
    # заминка должна повториться дважды подряд по всем воркерам.
    #
    # Сверху ограничивать нечем: восемь попыток — это секунды, а перебор всех
    # сорока семи логинов на мёртвой двери занял бы минуты, ради чего порог и
    # существует.
    GATEWAY_GIVE_UP_AFTER = envcfg.env_int("PROXY_GATEWAY_GIVE_UP_AFTER", 8)

    def note_gateway_refusal(self, proxy: str, seconds: float, reason: str = "") -> bool:
        """
        Логин отказал (403 на CONNECT). Откладываем ЕГО и говорим, стоит ли
        пробовать следующий. True — пора сдаваться, отказал сам шлюз.

        Раньше первый же 403 при одном шлюзе откладывал ВЕСЬ пул сразу.
        Основание было такое: за всеми логинами одна дверь и один аккаунт,
        значит отказ общий, а перебор сорока семи логинов на одной отказавшей
        двери — потерянные минуты и мусор в логе.

        Наблюдения 2026-09-13 это опровергли. В 20:18 тот же шлюз спокойно
        отдал прогон на 142 предмета, а в 20:26 и 20:28 отказал — то есть 403
        тут не «аккаунт закрыт», а плавающий отказ, который проходит сам.
        Отдельно: в 18:07 Steam вернул 429 ОДНОМУ логину, а остальные в ту же
        секунду работали — значит логины не взаимозаменяемы даже с точки зрения
        Steam, и подавно не обязаны отказывать одновременно.

        Цена прежнего поведения была высокой: четыре воркера получали 403,
        весь пул уходил на кулдаун, прогон останавливался на первом же
        предмете и отчитывался «предметов 0». Так прошли оба прогона после
        20:25.

        Исходное опасение при этом сохранено: сдаёмся не с первого отказа, но
        и не перебираем весь пул — хватает GATEWAY_GIVE_UP_AFTER разных
        логинов подряд.
        """
        if not proxy:
            return False
        self.mark_refused(proxy, seconds, reason)
        self._gateway_strikes.add(proxy)
        if len(self._gateway_strikes) < self.GATEWAY_GIVE_UP_AFTER:
            log.info(
                "%s: логин %s отказал (%s) — беру следующий (отказов подряд %d из %d)",
                self.name, mask(proxy), reason or "403",
                len(self._gateway_strikes), self.GATEWAY_GIVE_UP_AFTER,
            )
            return False
        self.mark_gateway_refused(proxy, seconds, reason)
        # Счётчик обнуляем ЗДЕСЬ, а не только на успехе. Без этого получался
        # самоподдерживающийся замок, и он тут же выстрелил в проде: набрав
        # восемь отказов, множество больше не пустело (успешного запроса взять
        # неоткуда — пул отложен целиком), и КАЖДЫЙ следующий 403 мгновенно
        # проходил порог заново. В логе 20:45-20:46 это видно как «отложен на
        # 1 мин» и сразу «отложены все 96» — восемь раз подряд, то есть порог
        # не работал вообще, а вёл себя как прежний «сдаёмся с первого отказа».
        #
        # Тормозом в этой ситуации служит кулдаун самого пула, а не счётчик:
        # адреса лежат минуту, и следующая волна должна заново набрать восемь
        # отказов, чтобы уложить их снова.
        self._gateway_strikes.clear()
        return True

    def mark_ok(self, proxy: str) -> None:
        """
        Через этот адрес прошёл успешный запрос — забываем накопленные отказы.

        Без сброса счётчик копился бы месяцами и однажды похоронил бы
        совершенно рабочий адрес по трём случайным отказам за неделю.

        Серию отказов шлюза сбрасываем тоже, и по той же причине: удачный
        запрос доказывает, что дверь открыта, а копившиеся до него отказы
        относились к прошлой заминке.
        """
        if not proxy:
            return
        self._refusals.pop(proxy, None)
        self.dead.pop(proxy, None)
        self._dead_until.pop(proxy, None)
        self._gateway_strikes.clear()

    def mark_dead(self, proxy: str, reason: str = "") -> None:
        """
        Пометить адрес нерабочим (не ответил вовсе, а не получил отказ от
        площадки). Держим отдельно от кулдауна: кулдаун — это «занят сейчас»,
        а тут «похоже, не работает совсем», и в /proxycheck это разные строки.
        """
        self.dead[proxy] = reason or "не отвечает"
        self._dead_until[proxy] = time.time() + self.DEAD_RETRY_SECONDS

    def mark_alive(self, proxy: str) -> None:
        self.dead.pop(proxy, None)

    def hosts(self) -> set[str]:
        """Разные точки входа в пуле — «хост:порт» без логина и пароля."""
        out = set()
        for url in self.proxies:
            try:
                parsed = urlsplit(url)
                if parsed.hostname:
                    out.add(f"{parsed.hostname}:{parsed.port}")
            except Exception:
                continue
        return out

    def single_gateway(self) -> bool:
        """
        Весь пул — это один шлюз под разными логинами?

        Так устроены сессионные прокси: адрес один, а IP на выходе выбирается
        токеном внутри ЛОГИНА (…session-xxxx…). Для нас это выглядит как
        сорок семь независимых адресов, но дверь у них одна и учётная запись
        одна. Значит и отказ у них общий: если шлюз ответил 403, он ответит
        так же всем сорока семи, и перебирать их бессмысленно.

        Проверено на живом логе: 47 адресов, а хост во всех записях один —
        proxy.flameproxies.com:8989. Бот при этом честно докладывал «37
        свободных из 47», и это вводило в заблуждение: свободных в его
        бухгалтерии, а работающих — ноль.
        """
        return len(self.proxies) > 1 and len(self.hosts()) == 1

    def _same_host(self, proxy: str) -> list[str]:
        try:
            parsed = urlsplit(proxy)
            key = f"{parsed.hostname}:{parsed.port}"
        except Exception:
            return [proxy]
        out = []
        for url in self.proxies:
            try:
                other = urlsplit(url)
                if f"{other.hostname}:{other.port}" == key:
                    out.append(url)
            except Exception:
                continue
        return out or [proxy]

    def mark_gateway_refused(self, proxy: str, seconds: float, reason: str = "") -> int:
        """
        Отказал не адрес, а шлюз: откладываем ВСЕ логины на том же хосте.

        Возвращает, сколько адресов отложено. Нужно, потому что иначе код
        перебирает десяток логинов на одной и той же отказавшей двери, тратит
        на это время и место в логе, а результат заранее известен.
        """
        same = self._same_host(proxy)
        for url in same:
            self._cooldowns[url] = max(
                self._cooldowns.get(url, 0.0), time.time() + seconds
            )
        log.warning(
            "%s: шлюз %s отказал (%s) — отложены все %d логин(ов) на нём. "
            "Свободно ещё %d из %d",
            self.name, mask(proxy), reason or "отказ", len(same),
            len(self.available()), len(self.proxies),
        )
        return len(same)

    def enabled(self) -> bool:
        return bool(self.proxies)

    def __len__(self) -> int:
        return len(self.proxies)

    def cooldown_remaining(self, proxy: str) -> float:
        return max(0.0, self._cooldowns.get(proxy, 0.0) - time.time())

    def available(self) -> list[str]:
        # Мёртвые адреса (mark_dead) исключены наравне с адресами на кулдауне.
        # Раньше dead влиял только на текст /proxycheck, а next()/available()
        # его не читали вовсе — то есть "мёртвый" прокси всё равно продолжал
        # получать реальные запросы и заново проваливаться на каждом из них.
        self._revive_expired()
        return [p for p in self.proxies if p not in self.dead and self.cooldown_remaining(p) <= 0]

    def all_exhausted(self) -> bool:
        return self.enabled() and not self.available()

    def next(self) -> str | None:
        """
        Следующий свободный прокси по кругу. None — пул пуст или все на
        кулдауне (тогда вызывающий код решает, ждать или идти напрямую).

        Перебор именно по кругу, а не «всегда первый свободный»: иначе весь
        трафик утыкается в один адрес, его квота выгорает первой, и остальные
        простаивают до тех пор, пока он не свалится в 429.
        """
        if not self.proxies:
            return None
        self._revive_expired()
        for _ in range(len(self.proxies)):
            proxy = self.proxies[self._cursor % len(self.proxies)]
            self._cursor += 1
            if proxy not in self.dead and self.cooldown_remaining(proxy) <= 0:
                return proxy
        return None

    def mark_exhausted(self, proxy: str, seconds: float, reason: str = "") -> None:
        """Пометить адрес занятым на seconds секунд (обычно до сброса окна лимита)."""
        if not proxy:
            return
        self._cooldowns[proxy] = max(self._cooldowns.get(proxy, 0.0), time.time() + seconds)
        log.warning(
            "%s: адрес %s отложен на %.0f мин%s. Свободно ещё %d из %d",
            self.name, mask(proxy), seconds / 60,
            f" ({reason})" if reason else "",
            len(self.available()), len(self.proxies),
        )

    def all_dead(self) -> bool:
        """Все адреса помечены нерабочими — то есть отказал сам провайдер, а не площадка."""
        return self.enabled() and all(p in self.dead for p in self.proxies)

    def failure_hint(self) -> str:
        """
        Человеческое объяснение, почему через прокси ничего не выходит.

        Нужно потому, что один и тот же отказ провайдера всплывает в четырёх
        независимых местах (листинги, цены, CSFloat, инвентарь), и в каждом
        раньше сочинялась своя формулировка — вплоть до «сетевая ошибка» на
        честном HTTP 403. Пользователь при этом четыре раза подряд читает
        разные тексты про одну и ту же причину и ищет четыре разные поломки.
        """
        if not self.proxies:
            return "прокси не заданы"
        if self.all_dead():
            # Причину называем как список версий, а не как приговор. 403 на
            # CONNECT приходит по добром десятку поводов, и утверждать по нему
            # «аккаунт не оплачен» — значит отправить человека проверять не то.
            return (
                f"все {len(self.proxies)} адрес(ов) отвечают отказом (это сам прокси-сервис, "
                f"не Steam). Обычные причины: кончился трафик, превышен лимит одновременных "
                f"сессий, истекла sticky-сессия, целевой хост не разрешён тарифом или "
                f"сменились логин/пароль. Проверь личный кабинет и /proxycheck"
            )
        if self.single_gateway():
            # Без этой оговорки «37 свободных из 47» читается как «есть ещё
            # 37 рабочих запасных», хотя запасных нет: дверь одна.
            return (
                f"{self.describe()} — но все {len(self.proxies)} логинов ведут на ОДИН "
                f"шлюз {next(iter(self.hosts()))}, это не независимые адреса. "
                f"Отказ шлюза общий для всех, перебирать их нечего"
            )
        return self.describe()

    def describe(self) -> str:
        """Состояние пула для /status — без паролей."""
        if not self.proxies:
            return "не задан"
        free = self.available()
        parts = [f"{len(free)} свободных из {len(self.proxies)}"]
        if self.dead:
            parts.append(f"{len(self.dead)} не отвечают")
        busy = [p for p in self.proxies if self.cooldown_remaining(p) > 0]
        if busy:
            soonest = min(self.cooldown_remaining(p) for p in busy)
            parts.append(f"ближайший освободится через {soonest / 60:.0f} мин")
        if self.problems:
            parts.append(f"{len(self.problems)} адрес(ов) с ошибкой в настройке")
        return ", ".join(parts)
