"""
Клиент CSFloat Market API — вторая площадка для сравнения цен со Steam.

Зачем: на CSFloat расплачиваются живыми деньгами, на Steam — запертым балансом
кошелька, поэтому цены систематически расходятся. Бот ищет лоты, которые на
CSFloat заметно дешевле стимовской цены.

Ключевое отличие от steam_client.py: тут НЕ надо ходить в Steam за ценой для
сравнения. CSFloat отдаёт её сам в каждом лоте — item.scm.price (цена Steam
Community Market, в центах) и item.scm.volume (объём продаж, то есть
ликвидность). Плюс сразу приходят цены наклеек и готовый float_value, так что
ни декодирование inspect-ссылок, ни наш прайс-лист стикеров тут не нужны.
Весь арбитраж считается из одного ответа.

Документация: https://docs.csfloat.com (исходник — github.com/csfloat/docs).
Ключ берётся в профиле csfloat.com на вкладке developer и задаётся переменной
окружения CSFLOAT_API_KEY (в код не зашивается).

ВАЖНО про ключ и GET /listings. По документации CSFloat ключ нужен только тем
эндпоинтам, которые это прямо заявляют ("Endpoints that require an API Key will
state so"): у POST /v1/listings в примере стоит "Authorization: <API-KEY>" и
подпись "Requires an authorization header", а у GET /v1/listings пример — голый
`curl "https://csfloat.com/api/v1/listings"` без всякой авторизации. То есть
список лотов публичный, и наш ключ на нём, скорее всего, вообще не смотрят:
для CSFloat мы тут анонимный трафик. Заголовок всё равно шлём (не мешает и
может давать headroom), но рассчитывать, что ключ снимет ограничения именно
здесь, нельзя — упираемся мы не в ключ, а в защиту публичного эндпоинта.

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

import aiohttp

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


async def _note_429(retry_after: str | None, headers: dict, body: str = "") -> tuple[float, bool]:
    """Возвращает (пауза_в_секундах, is_ip_block)."""
    global _cooldown_until, _consecutive_429

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
        if retry_after:  # сервис прямо сказал, сколько ждать — верим ему, а не формуле
            try:
                seconds = max(seconds, float(retry_after))
            except ValueError:
                pass
        verdict = "похоже на реальную квоту"
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
        "CSFloat 429: наш запрос — User-Agent=%r, ключ: %s",
        CSFLOAT_USER_AGENT, key_fingerprint(),
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

    await _throttle()
    url = f"{CSFLOAT_BASE_URL}/listings"
    async with session.get(
        url, params=params, headers={**_API_HEADERS, "Authorization": CSFLOAT_API_KEY}
    ) as resp:
        if resp.status == 429:
            body = ""
            try:
                body = await resp.text()
            except Exception:
                pass
            seconds, is_ip_block = await _note_429(
                resp.headers.get("Retry-After"), dict(resp.headers), body
            )
            raise CSFloatRateLimited(
                f"CSFloat ответил 429 — запросы приостановлены на {seconds / 60:.0f} мин.",
                is_ip_block=is_ip_block,
            )
        if resp.status in (401, 403):
            body = (await resp.text())[:200]
            raise CSFloatError(
                f"CSFloat отклонил ключ (HTTP {resp.status}). Проверь CSFLOAT_API_KEY "
                f"на Render — он берётся в профиле csfloat.com, вкладка developer. Ответ: {body!r}"
            )
        if resp.status != 200:
            body = (await resp.text())[:200]
            raise CSFloatError(f"CSFloat вернул HTTP {resp.status}: {body!r}")

        await _note_ok()
        # Остаток лимита логируем — это то, чего так не хватало со Steam:
        # там мы про лимит узнавали только по факту бана.
        remaining = resp.headers.get("x-ratelimit-remaining") or resp.headers.get("X-RateLimit-Remaining")
        if remaining is not None:
            log.info("csfloat: остаток лимита по заголовку = %s", remaining)

        data = await resp.json()

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
    pages ограничивает объём: 4 страницы по 50 = 200 свежих лотов за прогон.
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
