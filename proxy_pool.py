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
        self.dead: dict[str, str] = {}
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

    def add(self, raw: str) -> tuple[int, list[tuple[str, str]]]:
        """
        Добавить адреса на ходу. Возвращает (сколько добавлено, что не приняли).

        Нужно, чтобы пополнять пул командой из Telegram, а не передеплоем ради
        каждого нового адреса. Дубликаты отбрасываются молча: при копировании
        списками они неизбежны, и падать из-за них незачем.
        """
        added = 0
        rejected: list[tuple[str, str]] = []
        for candidate in _SPLIT_RE.split(raw or ""):
            candidate = candidate.strip()
            if not candidate:
                continue
            problem = validate(candidate)
            if problem:
                rejected.append((mask(candidate), problem))
                continue
            if candidate not in self.proxies:
                self.proxies.append(candidate)
                added += 1
        return added, rejected

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

    def mark_ok(self, proxy: str) -> None:
        """
        Через этот адрес прошёл успешный запрос — забываем накопленные отказы.

        Без сброса счётчик копился бы месяцами и однажды похоронил бы
        совершенно рабочий адрес по трём случайным отказам за неделю.
        """
        if not proxy:
            return
        self._refusals.pop(proxy, None)
        self.dead.pop(proxy, None)

    def mark_dead(self, proxy: str, reason: str = "") -> None:
        """
        Пометить адрес нерабочим (не ответил вовсе, а не получил отказ от
        площадки). Держим отдельно от кулдауна: кулдаун — это «занят сейчас»,
        а тут «похоже, не работает совсем», и в /proxycheck это разные строки.
        """
        self.dead[proxy] = reason or "не отвечает"

    def mark_alive(self, proxy: str) -> None:
        self.dead.pop(proxy, None)

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
