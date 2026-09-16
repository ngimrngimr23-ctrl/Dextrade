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
import envcfg
from scan_errors import scrub
import os
import time
from dataclasses import dataclass, field

import aiohttp
import yarl

from proxy_pool import ProxyPool, mask as mask_proxy

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

# Сколько разных адресов показывать CSFloat за прогон (см. fetch_market_wide).
# Один — потому что квота у CSFloat считается по КЛЮЧУ, а не по адресу, а на
# множество адресов с одного ключа он ругается напрямую: 2026-08-27 в теле 429
# пришло "You've been making too many requests from too many IPs".
CSFLOAT_MAX_ADDRESSES = envcfg.env_int("CSFLOAT_MAX_ADDRESSES", 1)

# Пауза между ЛЮБЫМИ двумя запросами, независимо от адреса.
#
# Появилась после прода 2026-08-20. Поадресной паузы оказалось мало: семь
# полос по 1.5 секунды дают почти пять запросов в секунду суммарно, и CSFloat
# ответил не квотным 429, а антифродом — {"code": 4874, "message": "You've been
# making too many requests lately"}, без заголовков лимита.
#
# Причина в том, что лимит у CSFloat привязан к КЛЮЧУ, а не к адресу. Это
# видно прямо в логе: при семи разных прокси счётчик шёл сплошной цепочкой
# 158 -> 157 -> 156 -> 155 -> 154 и у всех ответов был один и тот же момент
# сброса окна. Значит разгон по адресам не покупает квоту — он покупает только
# скорость, а скорость упирается в антифрод.
GLOBAL_MIN_INTERVAL = 1.0
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
# Можно задать НЕСКОЛЬКО адресов через запятую/пробел — это не косметика, а
# основной способ поднять потолок: лимит CSFloat (200 запросов в час) считается
# по IP, поэтому шесть адресов дают шесть независимых бюджетов. При отказе по
# квоте откладывается только выдохшийся адрес, работа продолжается с
# остальных — см. proxy_pool.ProxyPool.
#
# ВАЖНО: в этой строке лежат пароли. В логи они попадают ТОЛЬКО через
# proxy_pool.mask() — не логировать значение как есть.
CSFLOAT_POOL = ProxyPool(os.environ.get("CSFLOAT_HTTP_PROXY", ""), name="csfloat")


def http_proxy_problem() -> str | None:
    """Что не так с настройкой прокси, если не так. None — всё в порядке."""
    if not CSFLOAT_POOL.problems:
        return None
    return "; ".join(f"{addr}: {problem}" for addr, problem in CSFLOAT_POOL.problems)


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
    if CSFLOAT_POOL.enabled():
        return f"резидентные прокси ({CSFLOAT_POOL.describe()})"
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


def budget_remaining() -> int | None:
    """
    Сколько запросов осталось в текущем окне по последнему ответу CSFloat.
    None — заголовков лимита мы ещё не видели.

    Число, а не строка: по нему принимаются решения, а не только пишутся логи.
    """
    return None if not _last_budget else _last_budget.get("remaining")


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


# Счётчик скачанного за прогон — чтобы трафик резидентных прокси был измеренным
# числом, а не оценкой. Оценки тут уже подводили: объём Steam был прикинут в
# десять раз выше реального, потому что считался несжатым.
_bytes_downloaded = 0
_bytes_exact = True


def take_downloaded_bytes() -> tuple[int, bool]:
    """Сколько скачано с прошлого вызова и точное ли это число. Сбрасывает счётчик."""
    global _bytes_downloaded, _bytes_exact
    total, exact = _bytes_downloaded, _bytes_exact
    _bytes_downloaded, _bytes_exact = 0, True
    return total, exact


_throttle_locks: dict[str, asyncio.Lock] = {}
_throttle_last: dict[str, float] = {}


_GLOBAL_THROTTLE_KEY = "\x00global"


async def _throttle_all(proxy: str | None) -> None:
    """
    Две паузы подряд: общая на весь ключ и отдельная на этот адрес.

    Общая — потому что квота и антифрод у CSFloat считаются по ключу, и никакое
    число адресов их не обходит. Поадресная — чтобы один адрес не молотил
    подряд, даже когда общий темп это позволяет.
    """
    await _throttle(_GLOBAL_THROTTLE_KEY, GLOBAL_MIN_INTERVAL)
    if proxy:
        await _throttle(proxy, MIN_REQUEST_INTERVAL)


async def _throttle(key: str = "", interval: float = MIN_REQUEST_INTERVAL) -> None:
    """
    Пауза между запросами, ОТДЕЛЬНАЯ для каждого исходящего адреса.

    Раньше замок был один на весь процесс, и это делало пул прокси бесполезным
    для скорости: сколько бы адресов ни было, запросы всё равно выстраивались в
    одну очередь по MIN_REQUEST_INTERVAL. Между тем лимит считается по адресу,
    значит и темп надо держать по адресу — тогда N адресов дают N параллельных
    полос, а не N раз по одной.
    """
    lock = _throttle_locks.get(key)
    if lock is None:
        lock = _throttle_locks[key] = asyncio.Lock()
    async with lock:
        wait = interval - (time.monotonic() - _throttle_last.get(key, 0.0))
        if wait > 0:
            await asyncio.sleep(wait)
        _throttle_last[key] = time.monotonic()


@dataclass
class CSFloatListing:
    """Один лот с CSFloat вместе с ценой Steam для сравнения (всё в долларах)."""

    listing_id: str
    price: float                      # цена на CSFloat
    market_hash_name: str
    steam_price: float | None         # цена Steam; заполняется извне, см. bot._fill_steam_prices
    steam_volume: int | None          # сколько продаётся, грубая ликвидность (сейчас всегда None)
    # Из какого окна прайс-листа взята steam_price: "last_24h", "last_7d",
    # "last_30d", "last_90d". См. pricing._steam_price_from_windows.
    steam_price_window: str | None = None
    # Насколько разъезжаются окна цены, в процентах. Это мера доверия к цене:
    # у устойчивого предмета единицы процентов, разброс в разы означает, что
    # считать от такой цены скидку нельзя. None — окно всего одно.
    steam_price_spread_pct: float | None = None
    # Все окна одной строкой — только для логов и разбора спорных случаев.
    steam_price_windows: str | None = None
    float_value: float | None = None
    wear_name: str | None = None
    is_stattrak: bool = False
    is_souvenir: bool = False
    stickers: list[str] = field(default_factory=list)
    stickers_value: float = 0.0       # сумма scm-цен наклеек
    stickers_priced: int = 0          # у скольких наклеек цена вообще известна
    inspect_link: str | None = None
    watchers: int = 0
    # Блок reference из ответа — то, что пришло на замену пропавшему item.scm.
    # base_price это собственная справочная цена CSFloat: именно по ней они
    # считают свою сортировку highest_discount. Ценна тем, что приходит с
    # каждым лотом бесплатно и получена НЕЗАВИСИМО от прайс-листа csgotrader —
    # значит их можно сверять друг с другом (см. bot._fill_steam_prices).
    reference_price: float | None = None
    predicted_price: float | None = None
    reference_quantity: int | None = None
    # Сколько таких предметов лежит на витрине CSFloat. ЗАПАС, не продажи.
    # Отдельно от steam_volume намеренно: путать их — значит сравнивать
    # единицы с единицами другого рода (см. bot._fill_steam_prices).
    csfloat_quantity: int | None = None
    # Были ли у предмета продажи в Steam за последнюю неделю — по наличию окон
    # last_24h/last_7d в прайс-листе. Это НЕЗАВИСИМЫЙ от CSFloat признак
    # ликвидности и единственный честный ответ на вопрос «смогу ли я это
    # перепродать». Заполняется в bot._fill_steam_prices.
    steam_sales_recent: bool | None = None
    # Сошлись ли два независимых источника цены — прайс-лист csgotrader и
    # справка CSFloat. True: расходятся не больше ARB_SOURCE_GAP_PCT, цене
    # можно верить. False: разошлись, один из них врёт, и какой — неизвестно.
    # None: второго мнения нет вовсе. Заполняется в bot._fill_steam_prices.
    #
    # Раньше этот вывод существовал только как строчка для человека внутри
    # steam_price_windows, и код им пользоваться не мог. А это самый сильный
    # признак того, что кандидата стоит проверять живым запросом: живых
    # запросов восемь на прогон, а кандидатов полторы сотни.
    steam_price_confirmed: bool | None = None
    # Оценка источника ДО приведения к линейке низа стакана. Нужна ровно для
    # одного: по ней замеряется сам перекос (price_ruler.note). Замерять по
    # исправленному значению нельзя — коэффициент отменил бы сам себя.
    steam_price_raw: float | None = None
    # Когда лот выставили (created_at из ответа, ISO-строка как есть).
    # Нужно для скана «по свежести»: по ней видно, докуда мы досмотрели в
    # прошлый раз, и можно не перекачивать уже просмотренное.
    created_at: str | None = None

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
        reference = raw.get("reference") or {}
        # quantity — сколько таких предметов на рынке; это ближайшая замена
        # пропавшему scm.volume как грубой мере ликвидности.
        try:
            reference_quantity = int(reference["quantity"]) if reference.get("quantity") is not None else None
        except (TypeError, ValueError):
            reference_quantity = None
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
            reference_price=_cents(reference.get("base_price")),
            predicted_price=_cents(reference.get("predicted_price")),
            reference_quantity=reference_quantity,
            created_at=raw.get("created_at"),
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
    if CSFLOAT_POOL.enabled() or not CSFLOAT_PROXY_URL:
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
    # транспортном уровне (см. CSFLOAT_POOL), переписывать URL не нужно.
    if CSFLOAT_POOL.enabled() or not CSFLOAT_PROXY_URL:
        return f"{CSFLOAT_BASE_URL}{path}", params
    target = yarl.URL(f"{CSFLOAT_BASE_URL}{path}").with_query(params)
    return f"{CSFLOAT_PROXY_URL}/proxy", {"url": str(target)}


# Ручки, по которым проверяем, что доступно нашему ключу.
#
# Документация CSFloat (официальный репозиторий csfloat/docs — там ровно два
# файла, source/index.html.md и includes/_errors.md) описывает три эндпоинта:
# GET /listings, GET /listings/<id> и POST /listings. Ордеров на покупку в ней
# нет вовсе.
#
# Но на /me/buy-orders сервер отвечает не 404, а осмысленным
# {"code":27,"message":"authorization not set"} — значит ручка существует и
# просто не увидела заголовка. Документация на вопрос «пустит ли туда наш
# ключ» не отвечает; отвечает только живой запрос. А сделать его больше некому:
# у CSFloat закрыт доступ из среды разработки, а из браузера телефона заголовок
# Authorization выставить нельзя в принципе — ровно поэтому и пришёл тот
# «authorization not set».
# Сколько логинов перебрать на одну ручку, прежде чем признать её недоступной.
# Шесть при половине отказывающих логинов дают шанс промаха около 1.5%.
PROBE_ATTEMPTS = envcfg.env_int("CSFLOAT_PROBE_ATTEMPTS", 6)

PROBE_PATHS = (
    ("/listings?limit=1", "рынок (документирована)"),
    ("/me", "профиль — принимается ли ключ вообще"),
    ("/me/buy-orders", "ордера на покупку — НЕ документирована"),
    ("/me/offers-timeline", "история сделок — НЕ документирована"),
)


async def probe() -> list[tuple[str, str, int | None, str]]:
    """
    Дёрнуть ручки из PROBE_PATHS ключом и вернуть, что ответил сервер.

    ТОЛЬКО GET и только чтение. Ничего не создаёт, не покупает и не тратит: на
    счету деньги, и пробник — не то место, где стоит рисковать.

    Возвращает [(путь, зачем, HTTP-код или None, начало ответа, каким
    маршрутом, остаток лимита)].

    ОСТАТОК ЛИМИТА — не украшение, а единственный способ прочитать 429
    правильно. CSFloat отвечает на /listings с прямого адреса фразой
    «Please disable your VPN or try a different network, too many requests»,
    и в ней ДВА разных обвинения сразу: «ты через VPN» и «ты частишь». Первое
    лечится только сменой адреса, второе проходит само за час, и это
    противоположные выводы о том, что делать.

    Различает их x-ratelimit-remaining. Ноль — мы честно выбрали окно, надо
    ждать или освободить квоту. Двести при том же 429 — окно целое, значит
    дело в репутации адреса. Пробник эти заголовки читал и выбрасывал.
    """
    if not csfloat_enabled():
        raise CSFloatError("CSFLOAT_API_KEY не задан")

    out: list[tuple[str, str, int | None, str, str, str]] = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        for path, why in PROBE_PATHS:
            url, params = _build_request(path, {})

            # Маршруты по порядку: СНАЧАЛА прямой адрес, потом логины из пула
            # (каждая попытка — свой).
            #
            # Порядок именно такой по результату первого же замера: с прямого
            # адреса /me и /me/buy-orders ответили 200, а 429 «disable your
            # VPN» словила только публичная /listings. То есть ручки с
            # авторизацией CSFloat с датацентрового адреса пускает, и гнать их
            # через прокси незачем — он платный, он лежит третий день, и без
            # него они прекрасно работают.
            #
            # Прокси остаётся запасным: он нужен публичной /listings, которая
            # напрямую не идёт.
            routes: list[tuple[str | None, str]] = [(None, "прямой адрес")]
            if CSFLOAT_POOL.enabled():
                routes += [(CSFLOAT_POOL.next(), "прокси") for _ in range(PROBE_ATTEMPTS)]

            last_error = "не пробовали"
            status = None
            body = ""
            route_used = "—"
            limits = "заголовков лимита нет"
            for proxy, label in routes:
                try:
                    async with session.get(
                        url, params=params, proxy=proxy,
                        headers={**_API_HEADERS, "Authorization": CSFLOAT_API_KEY},
                    ) as resp:
                        status = resp.status
                        body = " ".join((await resp.text())[:200].split())
                        route_used = label
                        headers = dict(resp.headers)
                        # Заодно учим бот остатку квоты: он решает, начинать ли
                        # широкий скан (см. _orders_scan_target).
                        _note_budget(headers)
                        remaining = _header(headers, "x-ratelimit-remaining")
                        limit = _header(headers, "x-ratelimit-limit")
                        if remaining is not None:
                            limits = f"остаток {remaining} из {limit or '?'}"
                    break
                except Exception as e:  # noqa: BLE001 — пробник не падает целиком
                    last_error = scrub(str(e))
                    await asyncio.sleep(MIN_REQUEST_INTERVAL)

            if status is None:
                body = (
                    f"не дошло ни напрямую, ни через прокси "
                    f"({PROBE_ATTEMPTS} попыт.): {last_error}"
                )
            out.append((path, why, status, body, route_used, limits))
            await asyncio.sleep(MIN_REQUEST_INTERVAL)
    return out


class _ProxyTransient(Exception):
    """
    Сетевой сбой на конкретном адресе: оборвалось соединение, таймаут, отказ
    туннеля. Не ошибка CSFloat и не наша — просто этот адрес сейчас плох.

    Отдельный класс нужен, потому что резидентные прокси рвут соединения
    регулярно, это их нормальное поведение, а не исключительная ситуация. При
    восьми полосах по 25 страниц один обрыв убивал всю полосу целиком — 25
    страниц коту под хвост. Ловим, откладываем адрес ненадолго и повторяем
    запрос с другого.
    """


# Сетевые сбои, после которых имеет смысл просто взять другой адрес.
# ServerDisconnectedError сюда попал по факту с прода: прокси закрыл туннель на
# этапе CONNECT, а прежние обработчики ловили только ClientHttpProxyError и
# ClientProxyConnectionError, так что этот класс пролетал мимо и валил прогон.
_TRANSIENT_PROXY_ERRORS = (
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientProxyConnectionError,
    aiohttp.ClientOSError,
    aiohttp.ServerTimeoutError,
    asyncio.TimeoutError,
)

# На сколько откладывать адрес после сетевого сбоя. Коротко: это не квота и не
# бан, адрес почти наверняка живой — надо лишь не долбиться в него подряд.
PROXY_TRANSIENT_COOLDOWN_SECONDS = 60


# Прямой адрес Render как ЗАПАСНОЙ маршрут широкого скана.
#
# У /listings он второсортный, и это измерено: с прямого адреса CSFloat
# отвечает 429 «Please disable your VPN or try a different network» — адреса
# дата-центров он считает VPN. Но второсортный маршрут лучше, чем никакого:
# 2026-09-15 шлюз прокси отказывал на КАЖДОМ из 96 логинов подряд, и скан не
# уходил вообще никуда — команда просто молчала.
#
# Два правила, без которых этот маршрут вредит больше, чем помогает:
#   1. он ПОСЛЕДНИЙ в очереди. Сначала весь пул, и только когда свободных
#      адресов не осталось — прямой. Обратный порядок сжёг бы репутацию
#      прямого адреса на ровном месте (у авторизованных ручек /me и
#      /me/buy-orders порядок как раз обратный — там прямой работает, а
#      прокси нет, см. commit 6fbb7a0);
#   2. его 429 НЕ ставит общий кулдаун на ключ. Кулдаун у нас один на ключ,
#      и отказ прямому адресу глушил бы заодно авторизованные ручки, которые
#      именно прямым адресом и живут. Вместо этого запоминаем «прямой сейчас
#      закрыт» отдельно и не трогаем его DIRECT_BLOCK_SECONDS.
# Сколько РАЗНЫХ прокси-логинов пробовать на одну страницу, прежде чем
# признать, что дверь закрыта, и пойти прямым адресом. Не размер пула:
# пул может быть каким угодно большим, а ждать человек готов секунды.
PROXY_ATTEMPTS_PER_PAGE = envcfg.env_int("CSFLOAT_PROXY_ATTEMPTS", 8)

# Сколько полос должно сорваться на маршруте, чтобы остальные не повторяли их
# путь. Маршрут у полос общий: если его нет, он одинаково отсутствует у всех,
# и выяснять это восемь раз по восемь попыток — чистая трата времени (замер
# 2026-09-16: 53 секунды ради одного и того же ответа).
#
# ОДНА, а не две, и вот почему это не «сдаться с первого сбоя». Первый круг
# скана идёт разведкой, одной полосой (см. ниже), и эта полоса внутри себя
# успевает перебрать восемь РАЗНЫХ логинов и сходить прямым адресом. Её
# провал — не единичная неудача, а подробный ответ «хода нет».
#
# Вдобавок замыкание срабатывает только пока на руках НИ ОДНОГО лота. Стоит
# хоть одной полосе что-то принести — сбой соседней уже не обрывает прогон,
# потому что маршрут очевидно есть. Знание живёт до конца прогона и ничего не
# откладывает, так что ошибиться тут дёшево.
ROUTE_GIVE_UP_BANDS = envcfg.env_int("CSFLOAT_ROUTE_GIVE_UP_BANDS", 1)


# Адрес, через который в последний раз реально прошёл запрос.
#
# CSFLOAT_MAX_ADDRESSES=1 — правило верное: квота у CSFloat считается по ключу,
# а на множество адресов с одного ключа он ругается прямым текстом («too many
# requests from too many IPs»). Но «один адрес» и «ПЕРВЫЙ попавшийся адрес» —
# разные вещи, и разницу эту измерили:
#
#     /proxycheck: работают 38 из 134
#
# То есть случайно взятый логин мёртв с вероятностью примерно 0.72. Полосы
# широкого скана брали lane_addresses = proxies[:1], все шесть садились на
# ОДИН такой логин, и когда он оказывался мёртвым — а так бывало в трёх
# случаях из четырёх — каждая полоса самостоятельно перебирала по восемь
# замен. В логе это ровно «48 из 48 отказал шлюз» при 86 свободных адресах.
#
# Липкий адрес чинит это, не нарушая правила: адресов по-прежнему один, просто
# это тот, про который известно, что он работает. Первый же удачный запрос
# запоминает его, и остальные полосы садятся сразу на рабочий.
_sticky_lane: str | None = None


def lane_address() -> str | None:
    """Рабочий адрес для полосы: липкий, если он ещё свободен, иначе новый."""
    if not CSFLOAT_POOL.enabled():
        return None
    if _sticky_lane and _sticky_lane in CSFLOAT_POOL.available():
        return _sticky_lane
    return CSFLOAT_POOL.next()

DIRECT_BLOCK_SECONDS = envcfg.env_int("CSFLOAT_DIRECT_BLOCK_SECONDS", 900)
_direct_blocked_until = 0.0


def direct_available() -> bool:
    """Можно ли прямо сейчас пробовать прямой адрес для /listings."""
    return time.time() >= _direct_blocked_until


def _block_direct(why: str) -> None:
    global _direct_blocked_until
    _direct_blocked_until = time.time() + DIRECT_BLOCK_SECONDS
    log.warning(
        "csfloat: прямой адрес закрыт на %d мин (%s)",
        DIRECT_BLOCK_SECONDS // 60, scrub(why),
    )


class _DirectRefused(Exception):
    """
    CSFloat отказал ИМЕННО прямому адресу (429 не по квоте).

    Отдельный класс, чтобы этот отказ не превращался в общий кулдаун ключа:
    прокси-маршруты и авторизованные ручки после него по-прежнему рабочие.
    """


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
    session: aiohttp.ClientSession, url: str, request_params: dict[str, str],
    proxy: str | None = None, *, shared_cooldown: bool = True,
):
    """
    Один запрос за страницей лотов. Возвращает разобранный JSON.

    shared_cooldown=False — «это запасной прямой маршрут»: его 429 остаётся
    его личной бедой и не останавливает остальные (см. _DirectRefused).
    """
    await _throttle_all(proxy)
    try:
        return await _do_request(
            session, url, request_params, proxy, shared_cooldown=shared_cooldown
        )
    except aiohttp.ClientHttpProxyError as e:
        # 407 и подобное от самого прокси: логин/пароль или тариф, а не CSFloat.
        #
        # Раньше это сразу превращалось в жёсткую ошибку на ПЕРВОМ же адресе,
        # минуя ротацию по пулу — даже если остальные 6 из 7 ещё живы, весь
        # скан падал. По логам 2026-08-22 у Steam-фолбэка обнаружилось, что
        # flameproxies может отказать разом ВСЕМ сессиям аккаунта (403 на
        # разных session-id подряд) — то есть это ровно тот же класс сбоя,
        # что и обрыв соединения, просто с другим кодом ответа. Раз пул общий
        # (CSFLOAT_HTTP_PROXY/STEAM_HTTP_PROXY по умолчанию — один и тот же
        # список), нет смысла требовать от CSFloat-пути иного поведения, чем
        # у Steam-пути: пробуем следующий адрес, и только когда пул кончится,
        # сообщаем что не так — а не сдаёмся после первого же адреса.
        if not proxy:
            raise CSFloatError(
                f"Прокси отклонил запрос (HTTP {e.status}): проверь логин, пароль и "
                f"остаток трафика в личном кабинете."
            ) from None
        if e.status == 403:
            # Отказ авторизации самого прокси-сервиса — разовым ретраем на тот
            # же адрес не лечится, помечаем насовсем (см. ProxyPool.mark_dead).
            #
            # И заодно считаем серию: восемь РАЗНЫХ логинов подряд с 403 на
            # CONNECT — это отказ шлюза, а не совпадение, и перебирать
            # оставшиеся сто двадцать шесть бессмысленно. Пул откладывается
            # целиком, next() отдаёт None, и запрос тем же заходом уходит
            # прямым адресом.
            #
            # Этой строчки тут не было, и из-за неё вся защита от мёртвого
            # шлюза работала только у Steam: CSFloat звал mark_refused, то
            # есть считал каждый отказ отдельным невезением. Отсюда и брались
            # минутные простои на ровном месте.
            CSFLOAT_POOL.note_gateway_refusal(
                proxy, PROXY_TRANSIENT_COOLDOWN_SECONDS, f"HTTP 403: {e}"
            )
        # ТЕКСТ ИСКЛЮЧЕНИЯ ЧИСТИМ ЗДЕСЬ, А НЕ НА ВЫХОДЕ.
        #
        # aiohttp кладёт в ClientHttpProxyError полный url прокси — вместе с
        # логином и ПАРОЛЕМ. Отсюда он расходился по цепочке: в _ProxyTransient,
        # оттуда в «Последний ответ: ...» у CSFloatError, оттуда в сообщение
        # арбитража — и 2026-09-16 пароль ушёл в чат открытым текстом.
        #
        # Чистка на выходе (scan_errors.scrub) существует, но полагаться
        # только на неё нельзя: путей наружу много, и достаточно одного, где
        # её забыли — что и случилось. Секрет не должен попадать в строку
        # вообще, тогда забывать будет нечего.
        raise _ProxyTransient(
            f"HTTP {e.status} от прокси {mask_proxy(proxy)}"
        ) from None
    except _TRANSIENT_PROXY_ERRORS as e:
        # Без прокси менять нечего — тогда это честная сетевая ошибка наружу.
        if not proxy:
            raise CSFloatError(
            f"Сетевая ошибка при запросе к CSFloat: {scrub(str(e))}"
        ) from None
        raise _ProxyTransient(
            f"{type(e).__name__} на {mask_proxy(proxy)}: {scrub(str(e))}"
        ) from None


async def _do_request(
    session: aiohttp.ClientSession, url: str, request_params: dict[str, str],
    proxy: str | None = None, *, shared_cooldown: bool = True,
):
    async with session.get(
        url,
        params=request_params,
        headers={**_API_HEADERS, "Authorization": CSFLOAT_API_KEY},
        proxy=proxy,
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
            if not shared_cooldown:
                # Запасной прямой маршрут. Его отказ закрывает только его —
                # общий кулдаун ключа тут поставить нельзя, иначе заодно
                # встанут авторизованные ручки, которые прямым и работают.
                raise _DirectRefused(body[:200] or f"HTTP {resp.status}")
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

        # Считаем СЖАТЫЕ байты — именно их тарифицирует резидентный прокси.
        # Content-Length у сжатого ответа и есть размер на проводе; когда его
        # нет (chunked), считаем распакованное тело и помечаем как верхнюю
        # оценку, чтобы не выдавать её за точное число.
        global _bytes_downloaded, _bytes_exact
        raw_len = resp.headers.get("Content-Length")
        if raw_len:
            try:
                _bytes_downloaded += int(raw_len)
            except ValueError:
                pass
        else:
            _bytes_exact = False
            _bytes_downloaded += resp.content.total_bytes

        await _note_ok()
        CSFLOAT_POOL.mark_ok(proxy)  # прошло — забываем прошлые отказы адреса
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
    market_hash_name: str | None = None,
    proxy: str | None = None,
) -> tuple[list[CSFloatListing], str | None]:
    """
    Одна страница лотов CSFloat. Возвращает (лоты, курсор_следующей_страницы).
    Цены на вход — в долларах, наружу в API уходят центами.

    market_hash_name — сузить выборку до одного предмета. Параметр
    документированный, широкому скану не нужен (он смотрит рынок целиком), а
    вот для разбора одного предмета — единственный способ не выкачивать всё
    подряд.
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
    if market_hash_name:
        params["market_hash_name"] = market_hash_name

    url, request_params = _build_request("/listings", params)

    # Квотный 429 — это «на ЭТОМ адресе бюджет кончился», а не «CSFloat
    # недоступен». Поэтому выдохшийся адрес откладывается до сброса его окна, а
    # запрос уходит со следующего. Общий кулдаун ставится только когда свободных
    # адресов не осталось совсем.
    #
    # Попыток даём PROXY_ATTEMPTS_PER_PAGE, а НЕ «сколько адресов в пуле».
    #
    # Раньше было по размеру пула, и это ломало запасной прямой маршрут ровно
    # тогда, когда он нужен. Замер 2026-09-15, пул из 134 логинов, шлюз
    # отказывает всем: перебор шёл со скоростью примерно один адрес в секунду,
    # то есть до конца пула — больше двух минут, и только потом очередь дошла
    # бы до прямого адреса. /orders со своим таймаутом 45 с не дожидался и
    # докладывал «прокси не отвечает», ни разу не сходив напрямую.
    #
    # Восьми хватает и в здоровом случае: по /proxycheck отказы примерно
    # случайны, около половины на попытку, так что восемь разных логинов
    # промахнутся мимо рабочего с вероятностью 0.5^8 ≈ 0.4%. А в больном
    # случае восемь отказов — это восемь секунд, а не две минуты.
    max_attempts = max(
        min(len(CSFLOAT_POOL), PROXY_ATTEMPTS_PER_PAGE), QUOTA_429_RETRIES + 1
    )
    attempt = 0

    # Прямой адрес — последний патрон. Тратим его один раз за страницу и
    # только когда пул кончился или весь перебор провалился; см. комментарий
    # у DIRECT_BLOCK_SECONDS про то, почему он именно последний.
    direct_left = CSFLOAT_POOL.enabled() and direct_available()
    going_direct = False
    force_direct = False

    while True:
        # Полоса широкого скана закрепляет за собой адрес (см. fetch_market_wide):
        # тогда пауза между запросами держится по этому адресу, и полосы идут
        # параллельно. На повторах после 429 берём уже любой свободный.
        if force_direct:
            proxy, force_direct = None, False
        elif proxy and attempt == 0:
            pass
        elif attempt == 0:
            # Первая попытка — на ЗАВЕДОМО рабочий логин, а не на случайный.
            #
            # Липкая полоса была только у широкого скана, а поштучные запросы
            # (поиск лота по имени для /orders) брали next() вслепую. При
            # доле рабочих логинов 37 из 134 это восемь промахов подряд с
            # вероятностью 0.72^8 ≈ 7%, и в отчёте регулярно вылезало
            # «8 из 8 отказал шлюз» при 126 неотложенных адресах. Знание,
            # какой логин живой, общее — незачем добывать его заново на
            # каждом запросе.
            proxy = lane_address()
        else:
            proxy = CSFLOAT_POOL.next() if CSFLOAT_POOL.enabled() else None

        going_direct = CSFLOAT_POOL.enabled() and proxy is None
        if going_direct:
            if not direct_left:
                # Пул пуст, прямой уже потрачен или закрыт — всё, что можно
                # было попробовать, попробовано. Дальше решает вызывающий.
                raise CSFloatRateLimited(
                    f"Все прокси на кулдауне по квоте CSFloat "
                    f"({CSFLOAT_POOL.describe()}), прямой адрес тоже не прошёл."
                )
            direct_left = False
            log.info(
                "csfloat: прокси не дали пройти (%s) — пробую прямым адресом",
                CSFLOAT_POOL.describe(),
            )

        try:
            data = await _request_listings(
                session, url, request_params, proxy,
                shared_cooldown=not going_direct,
            )
            # Запрос прошёл — запоминаем адрес как рабочую полосу. Следующим
            # полосам незачем заново выяснять то, что только что выяснилось
            # (см. _sticky_lane).
            if proxy:
                global _sticky_lane
                _sticky_lane = proxy
            break
        except _DirectRefused as refused:
            # Прямой адрес отказал. Ключ при этом цел, пул тоже — просто этот
            # маршрут закрыт, и повторять его ближайшие минуты бессмысленно.
            _block_direct(str(refused))
            raise CSFloatRateLimited(
                f"Прокси не пропускают запрос ({CSFLOAT_POOL.describe()}), "
                f"а прямому адресу CSFloat ответил отказом: {scrub(str(refused))}"
            ) from None
        except _ProxyTransient as broke:
            attempt += 1
            log.info("csfloat: попытка %d — %s", attempt + 1, broke)
            CSFLOAT_POOL.mark_exhausted(
                proxy, PROXY_TRANSIENT_COOLDOWN_SECONDS, f"сетевой сбой ({broke})"
            )
            if attempt >= max_attempts:
                if direct_left:
                    # Перебор кончился, но прямой ещё не пробовали. Именно в
                    # эту дыру всё и проваливалось: пул из 96 логинов сгорал
                    # за 96 попыток, и до прямого очередь не доходила никогда.
                    force_direct = True
                    continue
                # Коротко и по делу. Прежний текст вываливал в чат весь
                # разбор пула на пол-экрана плюс сырой ответ aiohttp — вместе
                # с логином и паролем прокси. Подробности место в логе;
                # человеку нужно знать, ЧТО не работает и ЧТО с этим делать.
                raise CSFloatError(
                    f"CSFloat недоступен: прокси отказали "
                    f"({PROXY_ATTEMPTS_PER_PAGE} логин(ов) подряд, 403 от шлюза), "
                    f"прямой адрес тоже не прошёл. Это сторона провайдера "
                    f"прокси. Проверить: /proxycheck, /csfloatapi"
                ) from None
            # Адрес уже помечен, следующий заход возьмёт другой — паузы не надо.
            continue
        except _QuotaRetry as retry:
            attempt += 1
            _note_budget(retry.headers)

            # Откладываем ровно тот адрес, на котором кончилась квота — до
            # сброса его окна, если CSFloat назвал момент.
            if proxy:
                reset_in = _seconds_until_reset(retry.headers) or COOLDOWN_AFTER_429_SECONDS
                CSFLOAT_POOL.mark_exhausted(proxy, reset_in, "квота CSFloat исчерпана")

            # Остаток НОЛЬ — перебирать адреса бессмысленно, и это не теория, а
            # число от самого CSFloat в заголовке x-ratelimit-remaining.
            #
            # Здесь в модуле жило прямое противоречие. Комментарий у цикла
            # утверждал «квотный 429 — это на ЭТОМ адресе бюджет кончился», а
            # докстринг модуля и проверка бюджета при старте исходят из
            # обратного: «лимит 200 — это лимит НА КЛЮЧ» и «6 прогонов в час =
            # 180 из 200 доступных». Верно второе, и цена ошибки измерена:
            # шесть полос по восемь повторов — это 48 запросов, каждый под
            # общим замком в секунду, то есть 48 секунд ожидания. /orders со
            # своим таймаутом 45 с не доживал. Хуже того, каждый повтор ещё и
            # ТРАТИЛ запрос из того же исчерпанного окна.
            #
            # Когда остаток больше нуля, поведение прежнее: возможно, дело
            # всё-таки в адресе, и следующий пройдёт.
            left = budget_remaining()
            if left == 0:
                seconds, is_ip_block = await _note_429(
                    _header(retry.headers, "Retry-After"), retry.headers, retry.body
                )
                raise CSFloatRateLimited(
                    f"Квота ключа CSFloat исчерпана ({budget_description() or 'остаток 0'}). "
                    f"Смена адреса не поможет — лимит считается по ключу. "
                    f"Пауза {seconds / 60:.0f} мин.",
                    is_ip_block=is_ip_block,
                ) from None

            if attempt >= max_attempts:
                if direct_left:
                    force_direct = True
                    continue
                seconds, is_ip_block = await _note_429(
                    _header(retry.headers, "Retry-After"), retry.headers, retry.body
                )
                raise CSFloatRateLimited(
                    f"CSFloat ответил 429 — запросы приостановлены на {seconds / 60:.0f} мин.",
                    is_ip_block=is_ip_block,
                ) from None

            log.warning(
                "csfloat: 429 по квоте (%s). Попытка %d из %d — пробую следующий адрес",
                budget_description() or "остаток неизвестен", attempt, max_attempts,
            )
            # Без пула менять нечего, поэтому просто ждём; с пулом следующий
            # запрос уедет уже с другого адреса, и пауза не нужна.
            if not CSFLOAT_POOL.enabled():
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
    # Справочная цена нужна и как замена пропавшему scm, и как независимая
    # сверка для прайс-листа. Если её вдруг не станет тоже — это должно быть
    # видно сразу, а не всплыть через неделю пустыми подборками.
    without_reference = [l for l in listings if l.reference_price is None]
    if without_reference:
        log.warning(
            "csfloat: у %s из %s лотов нет reference.base_price — сверить цену не с чем",
            len(without_reference), len(listings),
        )
        for raw in rows:
            if not (raw.get("reference") or {}).get("base_price"):
                log.warning(
                    "csfloat: пример такого лота — ключи лота: %s; ключи reference: %s",
                    sorted(raw.keys()), sorted((raw.get("reference") or {}).keys()),
                )
                break

    # Проверка, что sort_by вообще уважается. Считаем скидку по той же цене, по
    # которой её считает сам CSFloat (reference.base_price) — иначе проверяли бы
    # не сортировку, а расхождение источников. От результата зависит, есть ли
    # смысл в страницах после первой: при работающей сортировке страница 1 —
    # лучшее, что есть на рынке, и остальные заведомо хуже.
    with_ref = [l for l in listings if l.reference_price]
    if sort_by == "highest_discount" and len(with_ref) >= 2:
        def _disc(l):
            return (l.reference_price - l.price) / l.reference_price * 100
        first, last = _disc(with_ref[0]), _disc(with_ref[-1])
        log.info(
            "csfloat: скидка к справочной цене по странице — первый лот %.1f%%, "
            "последний %.1f%% (%s)",
            first, last,
            "сортировка по скидке работает" if first >= last
            else "СОРТИРОВКА НЕ РАБОТАЕТ, sort_by игнорируется",
        )

    return listings, next_cursor


# Ценовые полосы для широкого скана. Границы в долларах, None — без края.
#
# Зачем резать по цене, а не просто качать больше страниц: пагинация у CSFloat
# курсорная, страницу N не получить без курсора со страницы N-1. Одна цепочка
# принципиально последовательна, и 200 запросов по 1.5 секунды — это 5 минут на
# прогон. А вот РАЗНЫЕ ценовые диапазоны — это независимые цепочки, их можно
# качать одновременно, по одному адресу на полосу.
#
# Границы неравномерные намеренно: дешёвых лотов на рынке несопоставимо больше,
# поэтому внизу полосы узкие, вверху широкие — иначе верхние полосы кончались бы
# на первой же странице, а нижняя не успевала прокачаться.
DEFAULT_PRICE_BANDS: tuple[tuple[float | None, float | None], ...] = (
    (None, 2.0),
    (2.0, 5.0),
    (5.0, 10.0),
    (10.0, 20.0),
    (20.0, 50.0),
    (50.0, 100.0),
    (100.0, 300.0),
    (300.0, None),
)


def _clip_bands(bands, min_price, max_price):
    """Оставить только полосы, попадающие в заданный пользователем диапазон цен."""
    out = []
    for lo, hi in bands:
        if min_price is not None:
            if hi is not None and hi <= min_price:
                continue
            lo = max(lo, min_price) if lo is not None else min_price
        if max_price is not None:
            if lo is not None and lo >= max_price:
                continue
            hi = min(hi, max_price) if hi is not None else max_price
        out.append((lo, hi))
    return out or [(min_price, max_price)]


# Докуда каждая полоса досмотрела в прошлый проход: created_at самого свежего
# лота, который она видела. Ключ — границы полосы: при смене min_price/max_price
# полосы перекраиваются, и чужая метка была бы прямым враньём.
#
# Живёт в памяти процесса, а не в Redis, и это осознанно. Потеря метки стоит
# ровно один полный проход после рестарта, а поход в Redis обошёлся бы в
# лишний round-trip на каждую полосу каждые десять минут. Редеплой и так
# обнуляет кэши подороже этого.
_BAND_WATERMARKS: dict[tuple, str] = {}


def band_watermarks() -> dict:
    """Докуда досмотрела каждая полоса. Только для /status и разбора логов."""
    return dict(_BAND_WATERMARKS)


class _BandRun:
    """Состояние одной полосы в течение одного прогона."""

    __slots__ = ("lo", "hi", "proxy", "cursor", "lots", "fresh", "fresh_last",
                 "requests", "done", "reason", "broke")

    def __init__(self, lo, hi, proxy):
        self.lo, self.hi, self.proxy = lo, hi, proxy
        self.broke = False
        self.cursor: str | None = None
        self.lots: list[CSFloatListing] = []
        self.fresh = 0            # сколько лотов новее прошлой метки
        # Новизна последней страницы. По ней раздаётся бюджет: добирать надо
        # там, где новое ещё не кончилось. На старте ставим потолок, чтобы
        # первый круг получили все полосы без исключения.
        self.fresh_last = MAX_LIMIT
        self.requests = 0
        self.done = False
        self.reason = ""

    @property
    def key(self) -> tuple:
        return (self.lo, self.hi)

    @property
    def label(self) -> str:
        lo = f"{self.lo:g}" if self.lo is not None else "0"
        hi = f"{self.hi:g}" if self.hi is not None else "∞"
        return f"${lo}-{hi}"


async def buy_orders_for(
    session: aiohttp.ClientSession, listing_id: str, *, limit: int = 20
) -> list[int]:
    """
    Стакан чужих ордеров на покупку по этому лоту. Цены в ЦЕНТАХ, как отдаёт API.

    Ручка недокументирована: в официальном репозитории csfloat/docs описаны
    только GET /listings, GET /listings/<id> и POST /listings. Но она отвечает —
    проверено /csfloatapi на живом ключе, — и без неё нельзя узнать главное:
    какую цену надо перебить, чтобы наш ордер стоял первым.

    Отсутствие стакана НЕ ошибка: у предмета может не быть ни одного ордера, и
    это как раз лучший случай. Поэтому на любой сбой возвращаем пустой список —
    планировщик поймёт его как «соперников нет» и предложит свой потолок.

    ПРЯМОЙ АДРЕС ПЕРВЫМ, прокси только запасным. Это не общее правило для
    CSFloat, а вывод из замера /csfloatapi 2026-09-15 на живом ключе:

        /me, /me/buy-orders   -> HTTP 200 с прямого адреса Render
        /listings (публичная) -> HTTP 429 «disable your VPN»

    Ручки с авторизацией CSFloat с датацентрового адреса пускает, а публичный
    список лотов — нет. Значит стакан незачем гнать через прокси: прямой путь
    бесплатен, не зависит от провайдера и не тратит трафик. А прокси у нас
    лежит третий день подряд, и без этой перестановки /orders был бы мёртв
    вместе с ним.
    """
    url, params = _build_request(
        f"/listings/{listing_id}/buy-orders", {"limit": str(limit)}
    )
    routes: list[str | None] = [None]
    if CSFLOAT_POOL.enabled():
        # Запасной маршрут — липкий рабочий логин, а не случайный из пула.
        routes.append(lane_address())

    data = None
    for proxy in routes:
        try:
            async with session.get(
                url, params=params, proxy=proxy,
                headers={**_API_HEADERS, "Authorization": CSFLOAT_API_KEY},
            ) as resp:
                if resp.status != 200:
                    log.info(
                        "csfloat: стакан по лоту %s — HTTP %s через %s",
                        listing_id, resp.status,
                        "прямой адрес" if proxy is None else "прокси",
                    )
                    continue
                data = await resp.json(content_type=None)
            break
        except Exception as e:  # noqa: BLE001 — пустой стакан безопаснее отказа
            log.info(
                "csfloat: стакан по лоту %s не получен через %s (%s)",
                listing_id, "прямой адрес" if proxy is None else "прокси",
                scrub(str(e)),
            )
    if data is None:
        return []

    rows = data.get("orders", data) if isinstance(data, dict) else data
    out: list[int] = []
    for row in rows or []:
        price = row.get("price") if isinstance(row, dict) else None
        if isinstance(price, (int, float)) and price > 0:
            out.append(int(price))
    return out


async def fetch_market_wide(
    *,
    target: int,
    sort_by: str = "most_recent",
    min_price: float | None = None,
    max_price: float | None = None,
    bands=DEFAULT_PRICE_BANDS,
) -> list[CSFloatListing]:
    """
    Широкий скан рынка: примерно target лотов за прогон, полосами параллельно.

    Каждая полоса — свой ценовой диапазон, своя цепочка курсоров и свой
    закреплённый адрес из пула. Полосы идут одновременно, поэтому время прогона
    определяется самой длинной полосой, а не суммой всех запросов.

    Глубина полос НЕ фиксированная, и это главное отличие от прежнего
    поведения. Раньше каждой полосе выдавалось target/полос/50 страниц поровну,
    независимо от того, есть ли там что смотреть. На практике это давало худшее
    из обоих концов: в полосе $300+ за десять минут появляются единицы новых
    лотов, а мы честно качали ей четыре страницы, то есть перекачивали одно и
    то же по третьему разу; в полосе до $2 новое за те же десять минут не
    влезало и в четыре страницы, и свежие лоты просто не доезжали. Примерно
    половина запросов прогона уходила на повторную закачку.

    Теперь запросы — общий бюджет на весь прогон. Полосы разбирают его по
    кругу, и полоса выбывает из очереди, как только дошла до лотов, которые
    видела в прошлый раз (см. _BAND_WATERMARKS). Освободившийся бюджет
    достаётся тем, где новизна ещё не кончилась. Глубина настраивается сама
    под то, как рынок шевелится на самом деле.

    Бюджет раздаётся по кругу, а не «кто первый». Это сознательно: иначе весь
    прогон утекал бы в полосу до $2, где стока всегда больше всех, а выгода с
    лота — центы. Приоритет по новизне включается только когда бюджета не
    хватает даже на один круг.

    Отсечка по свежести работает только при sort_by="most_recent": при любой
    другой сортировке порядок выдачи с временем не связан, и метка «докуда
    досмотрели» смысла не имеет.

    Отказ одной полосы (кончилась квота на её адресе, ошибка сети) не отменяет
    остальные: собираем что получилось и честно пишем в лог, какая полоса на
    чём встала. Пустой результат лучше половинчатого молчания.
    """
    bands = _clip_bands(bands, min_price, max_price)

    proxies = CSFLOAT_POOL.proxies or [None]
    # Сколько РАЗНЫХ адресов показывать CSFloat за прогон.
    #
    # Раньше полосы разбирали адреса по кругу, и шесть полос означали шесть
    # разных IP на один API-ключ. Плюс это ротируемые сессии, меняющие адрес
    # каждые 10 минут, — за час набегали десятки. 2026-08-27 CSFloat сказал об
    # этом прямым текстом в теле 429:
    #     "You've been making too many requests from too many IPs"
    #
    # Множество адресов здесь не даёт НИЧЕГО: квота считается по ключу (200 в
    # час, проверено — числом прокси не умножается), а темп и так держит общий
    # throttle по ключу. Прокси нужен ровно для одного — обойти блокировку
    # датацентрового адреса Render. Для этого хватает одного.
    # Берём липкий рабочий адрес, а не просто первый из списка — см.
    # _sticky_lane. Число адресов от этого не меняется, меняется только их
    # качество.
    lane_addresses = [a for a in (lane_address(),) if a] or proxies[:1]
    lane_addresses = lane_addresses[:max(1, CSFLOAT_MAX_ADDRESSES)]

    # Бюджет запросов на прогон. Нижняя граница — по странице на полосу: без
    # неё при маленьком target часть полос не получила бы ни одного запроса и
    # молча выпала бы из скана целиком.
    budget = max(len(bands), -(-target // MAX_LIMIT))
    by_recency = sort_by == "most_recent"

    runs = [
        _BandRun(lo, hi, lane_addresses[i % len(lane_addresses)])
        for i, (lo, hi) in enumerate(bands)
    ]

    log.info(
        "csfloat: широкий скан — цель %d лотов, бюджет %d запрос(ов) на %d полос, "
        "адресов в пуле %d, использую %d, прямой адрес %s. Отсечка по свежести: %s",
        target, budget, len(bands), len(proxies), len(lane_addresses),
        "в запасе" if direct_available() else "закрыт",
        "да" if by_recency else f"нет (сортировка {sort_by})",
    )

    # Ошибки полос этого прогона. Нужны для двух вещей сразу: короткого
    # замыкания (см. ROUTE_GIVE_UP_BANDS) и честного отказа вместо «пусто».
    broken: list[Exception] = []

    async def one_page(band: _BandRun, session) -> None:
        watermark = _BAND_WATERMARKS.get(band.key) if by_recency else None
        try:
            listings, cursor = await fetch_listings_page(
                session, cursor=band.cursor, sort_by=sort_by,
                # Адрес берём В МОМЕНТ запроса, а не тот, что был при
                # постройке полосы. Полосы идут не одновременно — их
                # выстраивает общий throttle по ключу, — поэтому к запросу
                # второй полосы первая уже выяснила, какой логин живой.
                min_price=band.lo, max_price=band.hi,
                proxy=lane_address() or band.proxy,
            )
        except (CSFloatRateLimited, CSFloatError) as e:
            # Отдаём то, что успели набрать, а не теряем всё.
            #
            # Раньше исключение улетало наружу и полоса пропадала целиком. На
            # проде это стоило дорого: одна полоса словила 429, следом общий
            # кулдаун убил остальные, и прогон вернул НОЛЬ лотов при том, что
            # семь запросов уже отработали и 350 лотов были на руках. За эти
            # запросы бюджет уже списан — выбрасывать их результат бессмысленно
            # вдвойне.
            band.done, band.reason = True, f"оборвалась ({scrub(str(e))})"
            band.broke = True
            broken.append(e)

            # Короткое замыкание на прогон. Если маршрута нет, он одинаково
            # отсутствует у всех полос, и каждой незачем выяснять это заново
            # по восемь попыток. Замер 2026-09-16: шесть полос по восемь
            # отказов — 53 секунды на то, чтобы узнать одно и то же.
            #
            # Это НЕ приговор пулу: ничего не откладывается, знание живёт
            # ровно до конца этого прогона. Следующий начнёт с чистого листа.
            if len(broken) >= ROUTE_GIVE_UP_BANDS and not any(b.lots for b in runs):
                for other in runs:
                    if not other.done:
                        other.done = True
                        other.reason = "маршрут закрыт, выяснено соседней полосой"
            return
        except Exception as e:  # noqa: BLE001 — чужая полоса не должна страдать
            log.warning("csfloat: полоса %s — непредвиденная ошибка: %r", band.label, e)
            band.done, band.reason = True, f"сбой ({e!r})"
            band.broke = True
            broken.append(e)
            return

        band.requests += 1
        band.cursor = cursor
        band.lots.extend(listings)

        # Лот без created_at считаем новым: пропустить настоящую находку хуже,
        # чем лишний раз её пересмотреть.
        fresh = [
            l for l in listings
            if not watermark or not l.created_at or l.created_at > watermark
        ]
        band.fresh += len(fresh)
        band.fresh_last = len(fresh)

        if not listings:
            band.done, band.reason = True, "пусто"
        elif not cursor:
            band.done, band.reason = True, "страницы кончились"
        elif watermark and len(fresh) < len(listings):
            band.done, band.reason = True, "дошла до просмотренного"

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=60),
        headers=_API_HEADERS,
    ) as session:
        spent = 0
        while spent < budget:
            active = [b for b in runs if not b.done]
            if not active:
                break
            # Если бюджета на всех уже не хватает, круг достаётся тем полосам,
            # у которых прошлая страница принесла больше нового.
            active.sort(key=lambda b: b.fresh_last, reverse=True)

            # ПЕРВЫЙ КРУГ — ОДНОЙ ПОЛОСОЙ, РАЗВЕДКОЙ.
            #
            # Полосы всё равно не идут параллельно: их выстраивает общий
            # throttle по ключу, один запрос в секунду. Значит запуск всех
            # восьми разом ничего не ускоряет, зато когда маршрута нет,
            # КАЖДАЯ честно платит свои восемь попыток — и короткое замыкание
            # не успевает сработать, потому что все уже в полёте. Замер
            # 2026-09-16: 53 секунды на то, чтобы восемь полос узнали одно и
            # то же.
            #
            # Разведка снимает обе беды разом. Маршрут закрыт — узнаём это за
            # одну полосу и выходим. Маршрут открыт — первая же полоса находит
            # рабочий логин, и остальные семь садятся на него сразу
            # (см. _sticky_lane), вместо того чтобы искать его каждая сама.
            take = active[:1] if spent == 0 else active[:budget - spent]
            spent += len(take)
            await asyncio.gather(*(one_page(b, session) for b in take))

    out: list[CSFloatListing] = []
    for band in runs:
        out.extend(band.lots)

    # Один и тот же лот может прийти из двух полос, если цена ровно на границе.
    unique: dict[str, CSFloatListing] = {}
    for listing in out:
        unique[listing.listing_id] = listing

    # НОЛЬ ЛОТОВ ПРИ СОРВАННЫХ ПОЛОСАХ — ЭТО ОТКАЗ, А НЕ ПУСТОЙ РЫНОК.
    #
    # Полосы глотают свои ошибки намеренно: одна сорвавшаяся не должна уносить
    # уже набранное соседями, и это правильно. Но когда сорвались ВСЕ и на
    # руках ничего, наружу уходило спокойное «просмотрено 0 лотов» — то есть
    # провал связи докладывался как «на рынке пусто». Именно так это и
    # выглядело у пользователя: «просмотрено 0 лот(ов) за 53 с, кандидатов 0».
    #
    # Частичный результат по-прежнему отдаём молча: там есть что показать.
    if not unique and broken:
        raise broken[0]

    # Метка на следующий прогон — самый свежий лот, который полоса видела.
    #
    # Двигаем её и тогда, когда полосу оборвал бюджет, а не отсечка. Между
    # нашей остановкой и прошлой меткой при этом остаются непросмотренные лоты,
    # и они пропадут навсегда — но это меньшее из двух зол. Недооценённый лот
    # живёт минуты и его выкупают; пропущенные — это как раз самые старые из
    # новых, то есть почти наверняка уже неактуальные. Не двигать метку значило
    # бы на каждом прогоне заново пережёвывать этот протухший хвост вместо
    # свежих поступлений. Полосы, обрезанные бюджетом, видно в логе ниже.
    if by_recency:
        for band in runs:
            newest = max((l.created_at for l in band.lots if l.created_at), default=None)
            if newest:
                _BAND_WATERMARKS[band.key] = newest

    per_band = "; ".join(
        f"{b.label}: {b.requests}зап/{b.fresh}нов ({b.reason or 'обрезана бюджетом'})"
        for b in runs
    )
    downloaded, exact = take_downloaded_bytes()
    mb = downloaded / 1024 / 1024
    log.info(
        "csfloat: широкий скан — %d запрос(ов) из %d, собрал %d лотов (%d уникальных), "
        "из них новых %d. Скачано %.1f МБ%s. По полосам: %s",
        spent, budget, len(out), len(unique), sum(b.fresh for b in runs),
        mb, "" if exact else " (оценка сверху)", per_band,
    )
    return list(unique.values())


async def fetch_market(
    *,
    pages: int = 4,
    sort_by: str = "most_recent",
    min_price: float | None = None,
    max_price: float | None = None,
    stop_below_discount: float | None = None,
    stop_at_created: str | None = None,
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

            # Скан по свежести: доходим до лотов, которые видели в прошлый раз,
            # и останавливаемся. Дальше идёт только уже просмотренное.
            #
            # Это делает глубину самонастраивающейся: на оживлённом рынке
            # прогон качает столько страниц, сколько успело появиться нового, а
            # в затишье укладывается в одну. Фиксированное число страниц либо
            # недобирало бы новинки, либо каждый раз перекачивало одно и то же.
            if stop_at_created and sort_by == "most_recent":
                reached = [l for l in listings if l.created_at and l.created_at <= stop_at_created]
                if reached:
                    log.info(
                        "csfloat: дошёл до уже просмотренных лотов на странице %d "
                        "(граница %s) — дальше только старое",
                        page + 1, stop_at_created,
                    )
                    break

            # Ранняя остановка. При sort_by="highest_discount" лоты идут по
            # убыванию скидки к справочной цене (проверено логом: 51% у первого
            # лота страницы, 0% у последнего). Значит как только хвост страницы
            # ушёл ниже порога отбора, все следующие страницы заведомо ниже —
            # качать их бессмысленно.
            #
            # Это и позволяет ставить большое число страниц: глубина берётся
            # там, где выгодные лоты действительно есть, а на пустом рынке
            # прогон стоит одну-две страницы вместо десяти.
            if stop_below_discount is not None and sort_by == "highest_discount":
                priced = [l for l in listings if l.reference_price]
                if priced:
                    tail = min(
                        (l.reference_price - l.price) / l.reference_price * 100
                        for l in priced
                    )
                    if tail < stop_below_discount:
                        log.info(
                            "csfloat: остановился на странице %d — скидки упали до %.1f%% "
                            "(порог %.1f%%), дальше только хуже",
                            page + 1, tail, stop_below_discount,
                        )
                        break
    return out
