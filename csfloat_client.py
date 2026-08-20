"""
Клиент CSFloat Market API — вторая площадка для сравнения цен со Steam.

Зачем: на CSFloat расплачиваются живыми деньгами, на Steam — запертым балансом
кошелька, поэтому цены систематически расходятся. Бот ищет лоты, которые на
CSFloat заметно дешевле стимовской цены.

ЦЕНА STEAM ЗДЕСЬ БОЛЬШЕ НЕ ПРИХОДИТ — не закладывайся на неё снова.
Модуль писался в расчёте на item.scm: там были price (цена Steam Community
Market в центах) и volume (объём продаж), плюс scm-цены у каждой наклейки, и
весь арбитраж считался из одного ответа. 2026-08-19 оказалось, что ключа scm
в ответе нет вообще — ни у одного лота из 50 (проверено логом реальных ключей
item). Когда именно он пропал, неизвестно; арбитраж к тому моменту не отдавал
находок ни разу.

Отбор при этом ломался молча и очень неприятно: лот с именем и ценой
разбирается успешно, warning'а «не разобрались» нет, а в analyzer он вылетает
на «нет цены Steam — сравнивать не с чем». Снаружи это неотличимо от слишком
строгого порога, и именно так мы это и читали.

Теперь цена Steam берётся из прайс-листа csgotrader.app (bot._fill_steam_prices):
статический файл на CDN со всем каталогом CS2, который бот и так качает для
цен стикеров. Это устойчивее — от необязательного чужого поля мы больше не
зависим.

ЧТО ИЗ scm ПОКА НЕ ВОССТАНОВЛЕНО, чтобы не выглядело работающим:
  * steam_volume (ликвидность) — фильтр min_volume теперь НЕ отсеивает лоты с
    неизвестным объёмом, иначе он выкашивал бы всё тем же молчаливым способом;
  * scm-цены наклеек (stickers_value) — значит отбор «по наклейкам»
    (sticker_max_markup_pct) сейчас не срабатывает, работает только ценовой.

Из ответа по-прежнему честно приходят: цена лота, market_hash_name,
float_value, wear_name, стикеры (имена), inspect-ссылка.

Документация: https://docs.csfloat.com (исходник — github.com/csfloat/docs).
Ключ берётся в профиле csfloat.com на вкладке developer и задаётся переменной
окружения CSFLOAT_API_KEY (в код не зашивается).

ВАЖНО про ключ и GET /listings: ДОКУМЕНТАЦИЯ УСТАРЕЛА, ключ обязателен.
В доке пример голый — `curl "https://csfloat.com/api/v1/listings"` без всякой
авторизации, и раздел Authentication говорит "Endpoints that require an API Key
will state so", а /listings этого не заявляет. По этому я и заключил, что
эндпоинт публичный. На практике запрос без ключа получает
403 {"code":1,"message":"You need to be logged in to search listings"}.
Квоты у авторизованных и анонимных тоже разные: с ключом приходит
x-ratelimit-limit: 200, без ключа — 50000 (но с отказом по существу).
Вывод: ключ здесь нужен, он проверяется, и лимит 200 — это лимит НА КЛЮЧ.

ВАЖНО про лимиты: заголовки остатка лимита CSFloat присылает не всегда. 429 без
них — это НЕ обязательно квота: как минимум один раз тело ответа прямым текстом
говорило "Please disable your VPN or try a different network" — то есть нас
отсекли по репутации адреса (Render сидит на датацентровых IP, которые
Cloudflare/CSFloat помечают как VPN), а не по исчерпанной квоте. Такое время не
лечит, поэтому для этого случая отдельный длинный и не растущий кулдаун.

ВАЖНО про заголовки — тут была ошибка, не повторять. Сначала мы слали обрезанный
"Mozilla/5.0" (явная подпись бота), потом — полный набор заголовков Chrome с
Origin/Referer/Sec-Fetch-Site: same-origin. Второе, судя по всему, только
ухудшило дело: заголовки заявляют вкладку браузера на csfloat.com, а TLS-отпечаток
у aiohttp питоновский, кук и cf_clearance нет — это противоречие антибот-защита
ловит надёжнее, чем честного клиента. В документации показан обычный curl, то
есть честный не-браузерный клиент для их API — норма. Поэтому здесь МИНИМАЛЬНЫЙ
честный набор заголовков и никакой имитации браузера.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field

from urllib.parse import urlsplit, urlunsplit

import aiohttp
import yarl

log = logging.getLogger("steam_bot.csfloat")

# .strip() не косметика: лишний пробел или перевод строки, случайно попавший в
# значение переменной окружения на Render, делает заголовок невалидным — aiohttp
# в этом случае падает с ValueError, и выглядело бы это как загадочная поломка.
CSFLOAT_API_KEY = os.environ.get("CSFLOAT_API_KEY", "").strip()
CSFLOAT_BASE_URL = "https://csfloat.com/api/v1"

# Пауза между запросами. Точного публичного числа у CSFloat нет (в доке лимиты
# описаны как "N запросов за 5 минут" без самого N), поэтому стартуем
# консервативно и смотрим на заголовки остатка — они скажут правду.
MIN_REQUEST_INTERVAL = 1.5
MAX_LIMIT = 50  # жёсткий потолок эндпоинта, больше он всё равно не отдаст

# Честный API-клиент: ровно то, что шлёт curl из примера в документации, плюс
# осмысленный User-Agent, по которому нас можно опознать. Никаких Origin,
# Referer и Sec-Fetch-* — см. предупреждение в докстринге модуля: имитация
# браузера с питоновским TLS-отпечатком выглядит подозрительнее честного бота.
# UA вынесен в переменную окружения, чтобы перебирать варианты без правки кода
# и передеплоя логики — это чисто диагностическая ручка.
CSFLOAT_USER_AGENT = os.environ.get(
    "CSFLOAT_USER_AGENT",
    "Dextrade/1.0 (+https://github.com/ngimrngimr23-ctrl/Dextrade)",
)

_API_HEADERS = {
    "User-Agent": CSFLOAT_USER_AGENT,
    "Accept": "application/json",
}

# Прокси через Cloudflare Worker — ПОПЫТКА сменить исходящий IP, не проверенное
# решение. Честно про историю: со Steam такой воркер проблему НЕ решил (там
# причина оказалась совсем другой — кука бета-маркета, см. докстринг
# steam_client), поэтому опираться на "у нас это уже сработало" нельзя, это
# было моё ошибочное утверждение.
#
# Здесь основания другие и они прямые: CSFloat сам пишет в теле ответа "disable
# your VPN or try a different network", то есть режет именно по адресу. Три
# разные конфигурации заголовков дали идентичный 429 — заголовки ни при чём.
# Так что смена IP бьёт в подтверждённую причину. Но сработает ли конкретно
# воркер, зависит от того, не в том же ли чёрном списке адреса Cloudflare —
# это выясняется только опытом.
#
# Интерфейс воркера общий: GET <прокси>/proxy?url=<полный целевой URL>, поэтому
# можно переиспользовать уже развёрнутый воркер, указав здесь его адрес (в его
# белый список хостов нужно добавить csfloat.com — иначе он отвечает 403
# "host not allowed"). Пусто (по умолчанию) — ходим напрямую, как раньше.
#
# ВАЖНО: заголовки (в т.ч. Authorization) при таком запросе уходят ВОРКЕРУ.
# Чтобы ключ дошёл до CSFloat, воркер должен пересылать заголовок дальше —
# ровно та же оговорка, что про куки Steam. Для GET /listings это, впрочем,
# скорее всего неважно: по документации этот эндпоинт ключа не требует.
#
# ВОРКЕР ТЕПЕРЬ ЗАПАСНОЙ ВАРИАНТ. Основной маршрут — CSFLOAT_HTTP_PROXY
# (резидентный прокси, см. ниже): у воркера исходящий адрес берётся из общего
# пула Cloudflare, и часть адресов приходит уже выжженной чужими запросами,
# из-за чего скан проходит через раз. Воркер оставлен рабочим и включается
# сам, если резидентный прокси не задан, — он всё равно лучше прямого
# запроса. Ниже история, из-за которой он вообще появился.
#
# ПРОВЕРЕНО ОПЫТОМ 2026-08-19 — без прокси вообще CSFloat недоступен.
# Переменную убирали и гоняли /arbnow напрямую: пришёл 429 с телом
# {"error": "Please disable your VPN or try a different network, too many
# requests"} и БЕЗ единого заголовка лимита. Это бан по репутации адреса, и он
# ровно тот, ради которого воркер и заводился. Через воркер в ту же минуту
# запрос доезжает, ключ виден (limit 200; аноним получил бы 50000).
#
# ТУТ ЖЕ ЗАКРЫТА ОШИБОЧНАЯ ВЕРСИЯ, чтобы её не воскрешали. Некоторое время
# держалось объяснение «x-ratelimit-remaining = 0 на первом же запросе свежего
# процесса, значит окно привязано к IP, а исходящий адрес Cloudflare Workers
# общий на всех арендаторов, и бюджет выжигают чужие». Это неверно. В успешном
# прогоне счётчик шёл 199 -> 198 -> 197 -> 196 на наши четыре запроса, то есть
# убывает РОВНО на нашу активность: бюджет привязан к ключу и он наш целиком.
#
# Настоящая причина нулевого остатка была своя: 200 запросов в час — это
# бюджет, который прежние 4 страницы каждые 5 минут (48/час) съедали вместе с
# передеплоями, каждый из которых восстанавливал джобы и заново сканировал.
# Лечится не сменой маршрута, а экономией запросов — см. ARB_PAGES_PER_SCAN.
CSFLOAT_PROXY_URL = os.environ.get("CSFLOAT_PROXY_URL", "").rstrip("/")

# Обычный HTTP-прокси — то, что продают под видом «резидентных прокси»
# (Bright Data, Oxylabs, IPRoyal, Webshare и прочие). Формат стандартный:
# http://логин:пароль@хост:порт
#
# Это ДРУГОЙ механизм, не путать с воркером выше. Воркер — это наш собственный
# сервис, которому мы отдаём целевой адрес параметром url. Здесь же прокси
# работает на транспортном уровне: запрос уходит по настоящему адресу
# csfloat.com, а прокси лишь подменяет исходящий IP (aiohttp делает CONNECT).
# Поэтому при заданном CSFLOAT_HTTP_PROXY воркер не используется вовсе —
# городить два прокси друг за другом незачем.
#
# Зачем это вообще: датацентровый адрес Render CSFloat режет по репутации
# («disable your VPN»), а у воркера исходящий адрес берётся из общего пула
# Cloudflare, и часть адресов приходит уже выжженной чужими запросами — оба
# случая подтверждены логами 2026-08-19. Резидентный адрес снимает обе
# причины сразу: он не помечен как VPN и квота на нём наша.
#
# ВАЖНО: в этой строке лежит пароль. В логи она попадает ТОЛЬКО через
# _mask_proxy() — не логировать её как есть.
CSFLOAT_HTTP_PROXY = os.environ.get("CSFLOAT_HTTP_PROXY", "").strip()


def http_proxy_problem() -> str | None:
    """
    Что не так с CSFLOAT_HTTP_PROXY, если не так. None — всё в порядке.

    Проверяем схему отдельно и заранее, потому что aiohttp умеет только
    http/https-прокси: SOCKS5 ему нужен через отдельный пакет aiohttp-socks,
    которого в requirements нет. Продавцы резидентных прокси обычно дают и то
    и другое, и молча купить SOCKS5-порт очень легко — а проявилось бы это
    невнятной ошибкой соединения уже после оплаты.
    """
    if not CSFLOAT_HTTP_PROXY:
        return None
    parsed = urlsplit(CSFLOAT_HTTP_PROXY)
    if parsed.scheme not in ("http", "https"):
        return (
            f"схема {parsed.scheme or 'не указана'!r} не поддерживается — aiohttp умеет "
            "только http/https-прокси. Возьми у провайдера HTTP-порт "
            "(обычно тот же хост, другой номер порта)"
        )
    if not parsed.hostname or not parsed.port:
        return "не разобрать хост и порт — нужен формат http://логин:пароль@хост:порт"
    return None


def _mask_proxy(url: str) -> str:
    """Адрес прокси без логина и пароля — для логов и /status."""
    try:
        parsed = urlsplit(url)
        if not parsed.hostname:
            # urlsplit на мусоре не падает, а возвращает пустые части — без этой
            # проверки в лог уехала бы пустая строка вместо внятного «неверный
            # формат», и настройка выглядела бы применённой.
            return "адрес не разобрался (ожидается http://логин:пароль@хост:порт)"
        if parsed.username or parsed.password:
            return urlunsplit((parsed.scheme, f"***@{parsed.hostname}:{parsed.port}", "", "", ""))
        return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
    except Exception:
        return "адрес не разобрался (ожидается http://логин:пароль@хост:порт)"

COOLDOWN_AFTER_429_SECONDS = 10 * 60
COOLDOWN_MAX_SECONDS = 2 * 60 * 60
# Пауза для 429 БЕЗ заголовков лимита и без признаков IP-блока — общий случай
# "непонятно почему, но не квота". Короткая и не растущая: ожидание такую
# блокировку не снимает, а длинный кулдаун только мешает проверить исправление.
SUSPECT_BLOCK_COOLDOWN_SECONDS = 2 * 60
# Пауза для ПОДТВЕРЖДЁННОГО IP-блока (тело ответа прямо говорит про VPN) —
# это не квота и не временный челлендж, а бинарная метка "этот IP не пускаем".
# Ждать 2 минуты и долбиться заново бессмысленно: метка сама не снимется.
# Кулдаун длинный и фиксированный (не растёт от повтора к повтору — это и так
# не квота, расти тут не от чего), но конечный — чтобы заметить, если исходящий
# IP всё же сменится (передеплой на Render иногда меняет адрес) или CSFloat
# снимет блокировку.
IP_BLOCK_COOLDOWN_SECONDS = 3 * 60 * 60
# Подстроки из реального ответа CSFloat при IP-блоке, по которым его отличаем
# от прочих 429 без заголовков лимита.
_IP_BLOCK_MARKERS = ("vpn", "different network")

_COOLDOWN_SCOPE = "csfloat"

# Повтор при 429 «квота исчерпана» — ДО ухода в кулдаун.
#
# Обоснование от 2026-08-19: остаток шёл ровно по нашим запросам (199..194) с
# якорем окна ~:32, а через шесть минут, без единого нашего запроса, пришёл
# остаток 0 с якорем :30:01. Разные якоря — разные счётчики, то есть запросы
# уходят не с одного исходящего адреса (Cloudflare берёт его из пула), и часть
# адресов приходит уже выжженной чужими.
#
# Если так, то уходить на 44 минуты в кулдаун после ОДНОГО отказа — ошибка:
# следующая попытка вполне может уехать с другого адреса, где бюджет целый.
# Цена проверки ограничена парой запросов, а выигрыш — рабочий скан вместо
# часа простоя. Если повторы стабильно упираются в один и тот же якорь, значит
# счётчик всё-таки общий и один — это будет видно в логе.
QUOTA_429_RETRIES = 2
QUOTA_429_RETRY_DELAY = 4.0


def _is_quota_429(headers: dict, body: str) -> bool:
    """429 про исчерпанную квоту, а не бан по репутации адреса и не антибот."""
    if any(marker in body.lower() for marker in _IP_BLOCK_MARKERS):
        return False
    return any("ratelimit" in k.lower() or k.lower() == "retry-after" for k in headers)


class CSFloatError(RuntimeError):
    """Что-то не так с запросом к CSFloat (кроме рейт-лимита)."""


class CSFloatRateLimited(RuntimeError):
    """CSFloat ответил 429 либо мы сами на кулдауне после недавнего 429."""

    def __init__(self, message: str, is_ip_block: bool = False):
        super().__init__(message)
        # True — подтверждённый бан по IP-репутации (см. IP_BLOCK_COOLDOWN_SECONDS),
        # а не обычная квота или разовый антибот-челлендж. Используется в bot.py,
        # чтобы один раз честно предупредить в чате, а не молчать вечно про то,
        # что арбитраж не работает.
        self.is_ip_block = is_ip_block


_request_lock = asyncio.Lock()
_last_request_at = 0.0
_cooldown_until = 0.0  # epoch-секунды: переживает рестарт через storage
_consecutive_429 = 0

# Последний увиденный остаток квоты (x-ratelimit-*). Раньше эти числа только
# уходили в лог одной строкой на успешный ответ — а успешных ответов как раз и
# не было, так что единственный момент, когда мы узнавали про бюджет, был уже
# постфактум, в 429. Из-за этого несколько дней держалась версия «фильтр
# слишком строгий, поэтому ничего не находится», хотя скан ни разу не дошёл до
# данных. Держим последнее замеренное значение и показываем его в /status.
_last_budget: dict | None = None


def csfloat_enabled() -> bool:
    """Без ключа модуль полностью выключен — бот работает как раньше."""
    return bool(CSFLOAT_API_KEY)


def cooldown_remaining() -> float:
    return max(0.0, _cooldown_until - time.time())


def key_fingerprint() -> str:
    """
    Безопасное описание ключа для логов и диагностики: длина и по два символа
    с краёв. Сам ключ в логи не попадает НИКОГДА — а понять, доехал ли он до
    Render целиком и тот ли он, что ожидался, этого достаточно.
    """
    if not CSFLOAT_API_KEY:
        return "не задан"
    k = CSFLOAT_API_KEY
    if len(k) <= 6:
        return f"длина {len(k)}, подозрительно короткий"
    return f"длина {len(k)}, {k[:2]}…{k[-2:]}"


def route_description() -> str:
    """Через что идём в CSFloat — для логов и /arbreset."""
    if CSFLOAT_HTTP_PROXY:
        return f"резидентный прокси {_mask_proxy(CSFLOAT_HTTP_PROXY)}"
    if CSFLOAT_PROXY_URL:
        return f"воркер {CSFLOAT_PROXY_URL}"
    return "напрямую (без прокси)"


async def reset_cooldown() -> None:
    """
    Снять кулдаун вручную. Нужно потому, что кулдаун при IP-блоке длинный (3 ч)
    и переживает передеплой: без этой ручки любая проверка изменений в запросе
    упиралась бы в ожидание, которое к самому изменению отношения не имеет.
    """
    global _cooldown_until, _consecutive_429
    _cooldown_until = 0.0
    _consecutive_429 = 0
    await _persist_cooldown()
    log.info("csfloat: кулдаун сброшен вручную")


async def _persist_cooldown() -> None:
    from storage import set_steam_cooldown  # хранилище общее, разделено по scope

    try:
        await set_steam_cooldown(_COOLDOWN_SCOPE, _cooldown_until, _consecutive_429)
    except Exception:
        log.exception("не смог сохранить кулдаун CSFloat")


async def load_persisted_cooldown() -> None:
    """Восстановить кулдаун после рестарта процесса (Render передеплоивает часто)."""
    global _cooldown_until, _consecutive_429
    from storage import get_steam_cooldown

    try:
        persisted = await get_steam_cooldown(_COOLDOWN_SCOPE)
    except Exception:
        log.exception("не смог загрузить сохранённый кулдаун CSFloat")
        return
    if not persisted:
        return
    _cooldown_until = persisted.get("cooldown_until", 0.0)
    _consecutive_429 = persisted.get("consecutive_429", 0)
    if cooldown_remaining() > 0:
        log.warning(
            "Восстановлен кулдаун CSFloat после рестарта: ещё %.0f мин",
            cooldown_remaining() / 60,
        )


def _header(headers: dict, name: str) -> str | None:
    """Заголовок без оглядки на регистр — CSFloat шлёт их в разном виде."""
    lowered = name.lower()
    for k, v in headers.items():
        if k.lower() == lowered:
            return v
    return None


def _seconds_until_reset(headers: dict) -> float | None:
    """
    Сколько ждать до сброса окна по x-ratelimit-reset (epoch-секунды).
    None — заголовка нет или он бессмысленный (в прошлом, слишком далеко).
    Потолок тот же COOLDOWN_MAX_SECONDS: доверять чужому числу без границы
    нельзя, опечатка на их стороне усыпила бы бота на сутки.
    """
    raw = _header(headers, "x-ratelimit-reset")
    if not raw:
        return None
    try:
        reset_at = float(raw)
    except (TypeError, ValueError):
        return None
    delta = reset_at - time.time()
    if delta <= 0 or delta > COOLDOWN_MAX_SECONDS:
        return None
    return delta + 5  # +5 сек, чтобы не проснуться ровно на границе окна


def _note_budget(headers: dict) -> None:
    """
    Запомнить остаток квоты из заголовков ЛЮБОГО ответа — и успешного, и 429.
    Смысл именно в «любого»: пока мы читали их только на 200, при постоянном
    429 бюджет оставался невидимым ровно тогда, когда он и был причиной.
    """
    global _last_budget

    remaining = _header(headers, "x-ratelimit-remaining")
    if remaining is None:
        return
    try:
        remaining_n = int(float(remaining))
    except (TypeError, ValueError):
        return

    limit = _header(headers, "x-ratelimit-limit")
    try:
        limit_n = int(float(limit)) if limit else None
    except (TypeError, ValueError):
        limit_n = None

    # Абсолютный якорь окна, а не только «через сколько». Без него нельзя
    # отличить одно окно от другого, а это ровно тот вопрос, который сейчас
    # открыт: 2026-08-19 остаток шёл ровно по нашим запросам (199..194) с
    # якорем ~:32, а через шесть минут БЕЗ единого нашего запроса пришёл 0 с
    # якорем :30:01. Два разных якоря — это два разных счётчика, то есть мы
    # ходим не через один исходящий адрес. Пока якорь не логировался, такие
    # переключения выглядели как «лимит ведёт себя необъяснимо».
    raw_reset = _header(headers, "x-ratelimit-reset")
    try:
        reset_at = float(raw_reset) if raw_reset else None
    except (TypeError, ValueError):
        reset_at = None

    _last_budget = {
        "remaining": remaining_n,
        "limit": limit_n,
        "reset_in": _seconds_until_reset(headers),
        "reset_at": reset_at,
        "seen_at": time.time(),
    }


def budget_description() -> str | None:
    """
    Человекочитаемый остаток квоты для /status. None — мы ещё ни одного ответа
    с заголовками лимита не видели.

    Зачем в /status: это единственное число, которое отличает «фильтр слишком
    строгий» от «мы вообще не доходим до данных». Без него обе ситуации
    выглядят одинаково — бот молчит.
    """
    if not _last_budget:
        return None
    b = _last_budget
    age_min = (time.time() - b["seen_at"]) / 60
    out = f"{b['remaining']} из {b['limit'] or '?'}"
    if b["reset_in"]:
        out += f", окно сбросится через {b['reset_in'] / 60:.0f} мин"
    if b.get("reset_at"):
        # Якорь окна в UTC — по нему видно, тот же это счётчик или уже другой.
        out += f" (окно до {time.strftime('%H:%M:%S', time.gmtime(b['reset_at']))} UTC)"
    out += f", замер {age_min:.0f} мин назад"
    return out


async def _note_429(retry_after: str | None, headers: dict, body: str = "") -> tuple[float, bool]:
    """Возвращает (пауза_в_секундах, is_ip_block)."""
    global _cooldown_until, _consecutive_429

    _note_budget(headers)

    # Настоящий рейт-лимит всегда сообщает Retry-After или X-RateLimit-*.
    has_limit_headers = any(
        "ratelimit" in k.lower() or k.lower() == "retry-after" for k in headers
    )
    # Подтверждённый бан по IP-репутации — тело прямым текстом просит
    # отключить VPN/сменить сеть. Проверяем ДО ветки has_limit_headers на
    # случай, если CSFloat когда-нибудь начнёт слать лимит-заголовки и на
    # такие ответы тоже — это всё равно не квота, ждать бесполезно.
    is_ip_block = any(marker in body.lower() for marker in _IP_BLOCK_MARKERS)

    if is_ip_block:
        seconds = IP_BLOCK_COOLDOWN_SECONDS
        verdict = "тело ответа говорит про VPN — это бан по IP, не квота (короткие ретраи бессмысленны)"
    elif has_limit_headers:
        # Реальная квота: имеет смысл ждать, и ждать всё дольше при повторах.
        _consecutive_429 += 1
        seconds = min(
            COOLDOWN_AFTER_429_SECONDS * (2 ** (_consecutive_429 - 1)), COOLDOWN_MAX_SECONDS
        )
        verdict = "похоже на реальную квоту"

        # x-ratelimit-reset — точный момент сброса окна, epoch-секунды. Это
        # лучший источник, чем наша формула: в первом же живом случае формула
        # дала 10 минут, а окно сбрасывалось через 46, то есть бот пошёл бы
        # долбиться в заведомо пустую квоту ещё четыре раза подряд.
        reset_in = _seconds_until_reset(headers)
        if reset_in is not None:
            seconds = reset_in
            verdict = f"квота исчерпана, окно сбросится через {reset_in / 60:.0f} мин (x-ratelimit-reset)"
        elif retry_after:  # сервис прямо сказал, сколько ждать — верим ему, а не формуле
            try:
                seconds = max(seconds, float(retry_after))
            except ValueError:
                pass
    else:
        # Антибот-защита: ожидание НЕ помогает, лечится только изменением
        # запроса (заголовки, IP). Поэтому короткая фиксированная пауза без
        # нарастания — иначе бот сам себя запирает на часы из-за проблемы,
        # которую время не решает, и проверить исправление невозможно.
        seconds = SUSPECT_BLOCK_COOLDOWN_SECONDS
        verdict = "заголовков лимита НЕТ — вероятно, антибот-защита, а не квота (ждать бесполезно)"

    _cooldown_until = max(_cooldown_until, time.time() + seconds)
    log.warning(
        "CSFloat вернул 429 — пауза %.0f мин. Retry-After=%s. %s",
        seconds / 60, retry_after or "нет", verdict,
    )
    # Логируем ВСЕ заголовки и начало тела: в прошлый раз фильтр по словам
    # limit/retry оставил нас с пустым {} ровно тогда, когда данные были нужнее
    # всего. cf-ray/cf-mitigated/server сразу покажут, Cloudflare это или нет.
    log.warning("CSFloat 429: все заголовки ответа: %s", dict(headers))
    if body:
        log.warning("CSFloat 429: начало тела ответа: %r", body[:400])
    # Что мы сами отправили — чтобы не гадать, доехал ли ключ и с каким UA
    # стучались. По документации GET /listings ключ не требует, так что его
    # наличие тут скорее всего ни на что не влияет, но видеть это надо.
    log.warning(
        "CSFloat 429: наш запрос — User-Agent=%r, ключ: %s, маршрут: %s",
        CSFLOAT_USER_AGENT, key_fingerprint(), route_description(),
    )

    await _persist_cooldown()
    return seconds, is_ip_block


async def _note_ok() -> None:
    global _consecutive_429
    if _consecutive_429 != 0:
        _consecutive_429 = 0
        await _persist_cooldown()


async def _throttle() -> None:
    global _last_request_at
    async with _request_lock:
        wait = MIN_REQUEST_INTERVAL - (time.monotonic() - _last_request_at)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_request_at = time.monotonic()


@dataclass
class CSFloatListing:
    """Один лот с CSFloat вместе с ценой Steam для сравнения (всё в долларах)."""

    listing_id: str
    price: float                      # цена на CSFloat
    market_hash_name: str
    steam_price: float | None         # item.scm.price — цена Steam Community Market
    steam_volume: int | None          # item.scm.volume — сколько продаётся, грубая ликвидность
    float_value: float | None = None
    wear_name: str | None = None
    is_stattrak: bool = False
    is_souvenir: bool = False
    stickers: list[str] = field(default_factory=list)
    stickers_value: float = 0.0       # сумма scm-цен наклеек
    stickers_priced: int = 0          # у скольких наклеек цена вообще известна
    inspect_link: str | None = None
    watchers: int = 0

    @property
    def url(self) -> str:
        return f"https://csfloat.com/item/{self.listing_id}"


def _cents(value) -> float | None:
    """CSFloat отдаёт все цены в центах."""
    if value is None:
        return None
    try:
        return float(value) / 100.0
    except (TypeError, ValueError):
        return None


def _parse_listing(raw: dict) -> CSFloatListing | None:
    """
    Разбирает один элемент ответа. Возвращает None, если структура не та —
    но НЕ молча: вызывающий код считает такие случаи и логирует, иначе
    поломка формата на их стороне выглядела бы как "просто ничего не нашлось"
    (ровно так мы недавно неделю искали причину пустого результата по флоату).
    """
    try:
        item = raw.get("item") or {}
        name = item.get("market_hash_name")
        price = _cents(raw.get("price"))
        if not name or price is None:
            return None

        scm = item.get("scm") or {}
        stickers_raw = item.get("stickers") or []
        sticker_names: list[str] = []
        stickers_value = 0.0
        stickers_priced = 0
        for st in stickers_raw:
            st_name = st.get("name")
            if st_name:
                sticker_names.append(st_name)
            st_price = _cents((st.get("scm") or {}).get("price"))
            if st_price:
                stickers_value += st_price
                stickers_priced += 1

        return CSFloatListing(
            listing_id=str(raw.get("id", "")),
            price=price,
            market_hash_name=name,
            steam_price=_cents(scm.get("price")),
            steam_volume=scm.get("volume"),
            float_value=item.get("float_value"),
            wear_name=item.get("wear_name"),
            is_stattrak=bool(item.get("is_stattrak")),
            is_souvenir=bool(item.get("is_souvenir")),
            stickers=sticker_names,
            stickers_value=stickers_value,
            stickers_priced=stickers_priced,
            inspect_link=item.get("inspect_link"),
            watchers=raw.get("watchers") or 0,
        )
    except Exception:
        log.exception("csfloat: не смог разобрать лот")
        return None


# Ответы, которые генерирует сам воркер-прокси, а не CSFloat. Список короткий и
# по делу: воркер отдаёт короткий текст, CSFloat — JSON с полем error.
_PROXY_ERROR_MARKERS = ("host not allowed", "missing url", "invalid url", "bad url")


def _looks_like_proxy_error(body: str) -> bool:
    """
    Похоже ли, что этот ответ сочинил наш воркер, а не CSFloat. Нужно, чтобы не
    выдавать ошибку прокси за ошибку площадки: один раз бот уже доложил
    "CSFloat отклонил ключ" на воркерское "host not allowed: csfloat.com",
    и это увело диагностику совсем не туда.
    """
    # Резидентный прокси таких ответов не сочиняет — он вообще не читает тело,
    # а только пробрасывает соединение. Сочинять их может только наш воркер.
    if CSFLOAT_HTTP_PROXY or not CSFLOAT_PROXY_URL:
        return False
    return any(marker in body.lower() for marker in _PROXY_ERROR_MARKERS)


def _build_request(path: str, params: dict[str, str]) -> tuple[str, dict[str, str]]:
    """
    Куда реально слать запрос. Без CSFLOAT_PROXY_URL — напрямую в CSFloat.
    С ним — в воркер, а настоящий адрес уезжает параметром url.

    Целевой URL собираем через yarl.URL.with_query, а наружу отдаём его одной
    строкой в params — так aiohttp закодирует его РОВНО ОДИН раз. Ровно на этом
    в steam_client уже обжигались: если склеить закодированную строку руками,
    yarl кодирует её повторно (%20 -> %2520) и прокси получает мусор.
    """
    # С резидентным прокси идём по настоящему адресу: подмена IP там на
    # транспортном уровне (см. CSFLOAT_HTTP_PROXY), переписывать URL не нужно.
    if CSFLOAT_HTTP_PROXY or not CSFLOAT_PROXY_URL:
        return f"{CSFLOAT_BASE_URL}{path}", params
    target = yarl.URL(f"{CSFLOAT_BASE_URL}{path}").with_query(params)
    return f"{CSFLOAT_PROXY_URL}/proxy", {"url": str(target)}


class _QuotaRetry(Exception):
    """
    Внутренний сигнал «429 по квоте, но кулдаун ещё не ставили».

    Нужен, чтобы решение о повторе принимал вызывающий код, а сам запрос
    оставался одной прямой функцией без ветки «а это уже последняя попытка?».
    Наружу не выходит: либо повторяем, либо превращаем в CSFloatRateLimited.
    """

    def __init__(self, headers: dict, body: str):
        super().__init__("429 quota")
        self.headers = headers
        self.body = body


async def _request_listings(
    session: aiohttp.ClientSession, url: str, request_params: dict[str, str]
):
    """Один запрос за страницей лотов. Возвращает разобранный JSON."""
    await _throttle()
    try:
        return await _do_request(session, url, request_params)
    except aiohttp.ClientHttpProxyError as e:
        # 407 и подобное от самого прокси: логин/пароль или тариф, а не CSFloat.
        # Без этой ветки наружу летел бы голый трейсбек, и было бы неочевидно,
        # что площадка тут вообще ни при чём.
        raise CSFloatError(
            f"Резидентный прокси отклонил запрос (HTTP {e.status}): проверь логин, пароль "
            f"и остаток трафика в личном кабинете. Маршрут: {route_description()}"
        ) from None
    except aiohttp.ClientProxyConnectionError as e:
        raise CSFloatError(
            f"Не удалось подключиться к резидентному прокси ({e}). Проверь хост и порт "
            f"в CSFLOAT_HTTP_PROXY. Маршрут: {route_description()}"
        ) from None


async def _do_request(
    session: aiohttp.ClientSession, url: str, request_params: dict[str, str]
):
    async with session.get(
        url,
        params=request_params,
        headers={**_API_HEADERS, "Authorization": CSFLOAT_API_KEY},
        proxy=CSFLOAT_HTTP_PROXY or None,
    ) as resp:
        if resp.status == 429:
            body = ""
            try:
                body = await resp.text()
            except Exception:
                pass
            headers = dict(resp.headers)
            if _is_quota_429(headers, body):
                raise _QuotaRetry(headers, body)
            # Бан по репутации адреса или антибот — повторять бессмысленно,
            # кулдаун ставим сразу.
            seconds, is_ip_block = await _note_429(
                _header(headers, "Retry-After"), headers, body
            )
            raise CSFloatRateLimited(
                f"CSFloat ответил 429 — запросы приостановлены на {seconds / 60:.0f} мин.",
                is_ip_block=is_ip_block,
            )
        if resp.status in (401, 403):
            body = (await resp.text())[:200]
            # Через прокси 4xx может прийти ОТ ВОРКЕРА, а не от CSFloat, и тогда
            # совет "проверь ключ" уводит в сторону — так и вышло: воркер отдал
            # "host not allowed: csfloat.com" (в его белом списке был только
            # steamcommunity.com), а бот доложил про отклонённый ключ.
            if _looks_like_proxy_error(body):
                raise CSFloatError(
                    f"Воркер-прокси не пропустил запрос (HTTP {resp.status}): {body!r}. "
                    f"Это ответ прокси, а не CSFloat — ключ ни при чём. "
                    f"Добавь csfloat.com в белый список хостов воркера. Маршрут: {route_description()}"
                )
            # "You need to be logged in" (code 1) значит не "ключ плохой", а
            # "ключа не было вовсе": до CSFloat он не доехал. Через прокси это
            # чаще всего воркер, не пересылающий Authorization.
            if "logged in" in body.lower():
                raise CSFloatError(
                    f"CSFloat не увидел ключ (HTTP {resp.status}): {body!r}. "
                    f"Ключ у нас {key_fingerprint()}, маршрут: {route_description()}. "
                    "Если идём через прокси — проверь, что воркер пересылает заголовок "
                    "Authorization (см. cloudflare-worker/worker.js)."
                )
            raise CSFloatError(
                f"CSFloat отклонил ключ (HTTP {resp.status}). Проверь CSFLOAT_API_KEY "
                f"на Render — он берётся в профиле csfloat.com, вкладка developer. "
                f"Ответ: {body!r}. Маршрут: {route_description()}"
            )
        if resp.status != 200:
            body = (await resp.text())[:200]
            if _looks_like_proxy_error(body):
                raise CSFloatError(
                    f"Воркер-прокси вернул HTTP {resp.status}: {body!r} "
                    f"(это ответ прокси, а не CSFloat). Маршрут: {route_description()}"
                )
            raise CSFloatError(
                f"CSFloat вернул HTTP {resp.status}: {body!r}. Маршрут: {route_description()}"
            )

        await _note_ok()
        # Остаток лимита логируем — это то, чего так не хватало со Steam:
        # там мы про лимит узнавали только по факту бана.
        _note_budget(dict(resp.headers))
        budget = budget_description()
        if budget:
            log.info("csfloat: остаток лимита %s", budget)

        return await resp.json()


async def fetch_listings_page(
    session: aiohttp.ClientSession,
    *,
    cursor: str | None = None,
    limit: int = MAX_LIMIT,
    sort_by: str = "most_recent",
    min_price: float | None = None,
    max_price: float | None = None,
) -> tuple[list[CSFloatListing], str | None]:
    """
    Одна страница лотов CSFloat. Возвращает (лоты, курсор_следующей_страницы).
    Цены на вход — в долларах, наружу в API уходят центами.
    """
    if not csfloat_enabled():
        raise CSFloatError("CSFLOAT_API_KEY не задан")
    if cooldown_remaining() > 0:
        raise CSFloatRateLimited(
            f"CSFloat на кулдауне после 429 — ещё {cooldown_remaining() / 60:.0f} мин."
        )

    params: dict[str, str] = {
        "limit": str(min(limit, MAX_LIMIT)),
        "sort_by": sort_by,
        "type": "buy_now",  # аукционы для мгновенного арбитража не годятся
    }
    if cursor:
        params["cursor"] = cursor
    if min_price is not None:
        params["min_price"] = str(int(min_price * 100))
    if max_price is not None:
        params["max_price"] = str(int(max_price * 100))

    url, request_params = _build_request("/listings", params)

    # Повторяем только квотный 429 — см. QUOTA_429_RETRIES. Кулдаун ставится
    # ОДИН раз, после последней неудачной попытки: иначе первый же отказ
    # запирает бота на час, даже если следующий запрос уехал бы с другого,
    # не выжженного адреса.
    attempt = 0
    while True:
        try:
            data = await _request_listings(session, url, request_params)
            break
        except _QuotaRetry as retry:
            attempt += 1
            _note_budget(retry.headers)
            if attempt > QUOTA_429_RETRIES:
                seconds, is_ip_block = await _note_429(
                    _header(retry.headers, "Retry-After"), retry.headers, retry.body
                )
                raise CSFloatRateLimited(
                    f"CSFloat ответил 429 — запросы приостановлены на {seconds / 60:.0f} мин.",
                    is_ip_block=is_ip_block,
                ) from None
            log.warning(
                "csfloat: 429 по квоте (%s). Попытка %d из %d — повторяю через %.0f с, "
                "вдруг следующий запрос уедет с другого исходящего адреса",
                budget_description() or "остаток неизвестен",
                attempt, QUOTA_429_RETRIES, QUOTA_429_RETRY_DELAY,
            )
            await asyncio.sleep(QUOTA_429_RETRY_DELAY)

    # Формат ответа документирован как массив, но встречались обёртки вида
    # {"data": [...]} — поддерживаем оба, чтобы не падать на ровном месте.
    if isinstance(data, dict):
        rows = data.get("data") or data.get("listings") or []
        next_cursor = data.get("cursor") or data.get("next_cursor")
    else:
        rows = data or []
        next_cursor = None

    listings = []
    bad = 0
    for raw in rows:
        parsed = _parse_listing(raw)
        if parsed is None:
            bad += 1
        else:
            listings.append(parsed)

    if bad:
        log.warning("csfloat: %s из %s лотов не разобрались (формат ответа изменился?)", bad, len(rows))
    log.info("csfloat: получено %s лотов (курсор дальше: %s)", len(listings), "есть" if next_cursor else "нет")

    # Лот без item.scm.price разбирается УСПЕШНО (имя и цена на месте), просто
    # приезжает без цены Steam — и потом молча вылетает в отборе, которому не с
    # чем сравнивать. Снаружи это выглядело как «порог слишком строгий», хотя
    # дело в форме ответа. Поэтому считаем такие лоты отдельно и показываем
    # настоящие ключи ответа, а не гадаем, куда переехало поле.
    without_scm = [l for l in listings if l.steam_price is None]
    if without_scm:
        log.warning(
            "csfloat: у %s из %s лотов нет цены Steam (item.scm.price) — "
            "сравнивать их не с чем, отбор их выбросит",
            len(without_scm), len(listings),
        )
        for raw in rows:
            item = raw.get("item") or {}
            if not (item.get("scm") or {}).get("price"):
                # Ключи ВЕРХНЕГО уровня тоже: цена рынка могла не пропасть, а
                # переехать из item наружу (у CSFloat есть блок reference с
                # base_price — по нему они, судя по всему, и считают свою
                # сортировку по скидке). Если он тут есть, это более точный
                # источник, чем медиана за сутки из прайс-листа.
                log.warning(
                    "csfloat: пример такого лота — ключи лота: %s; ключи item: %s; ключи item.scm: %s",
                    sorted(raw.keys()), sorted(item.keys()),
                    sorted((item.get("scm") or {}).keys()),
                )
                reference = raw.get("reference")
                if isinstance(reference, dict):
                    log.warning("csfloat: в лоте есть reference, его ключи: %s", sorted(reference.keys()))
                break

    # Проверка, что sort_by вообще уважается. От этого зависит, имеет ли смысл
    # качать вторую страницу: при работающей сортировке по скидке страница 1 —
    # это лучшее, что есть на рынке, и остальные страницы заведомо хуже.
    with_scm = [l for l in listings if l.steam_price]
    if sort_by == "highest_discount" and len(with_scm) >= 2:
        def _disc(l):
            return (l.steam_price - l.price) / l.steam_price * 100
        first, last = _disc(with_scm[0]), _disc(with_scm[-1])
        log.info(
            "csfloat: скидка к Steam по странице — первый лот %.1f%%, последний %.1f%% (%s)",
            first, last,
            "сортировка по скидке работает" if first >= last
            else "СОРТИРОВКА НЕ РАБОТАЕТ, sort_by игнорируется",
        )

    return listings, next_cursor


async def fetch_market(
    *,
    pages: int = 4,
    sort_by: str = "most_recent",
    min_price: float | None = None,
    max_price: float | None = None,
) -> list[CSFloatListing]:
    """
    Несколько страниц рынка подряд, с постраничным курсором.
    pages ограничивает объём: 4 страницы по 50 = 200 лотов за прогон.

    sort_by по умолчанию most_recent — так исторически сложилось для этой
    функции как общей "выгрузки рынка". Для арбитражного скана в bot.py это
    было ошибкой: most_recent значит "что появилось только что", а не "что
    дешевле Steam" — свежевыставленный лот почти всегда честно оценён, скидка
    в несколько процентов среди случайных 200 новых лотов редкость, а не
    норма. Там, где важна именно недооценка, звать с sort_by="highest_discount"
    — CSFloat сам сортирует по скидке к scm.price, это ровно то, что мы ищем.
    """
    out: list[CSFloatListing] = []
    cursor = None
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        headers=_API_HEADERS,
    ) as session:
        for page in range(pages):
            listings, cursor = await fetch_listings_page(
                session, cursor=cursor, sort_by=sort_by,
                min_price=min_price, max_price=max_price,
            )
            out.extend(listings)
            if not cursor or not listings:
                break
    return out
