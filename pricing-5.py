"""
Определение цены стикера по коду, вытащенному из имени файла иконки
(см. steam_client.STICKER_RE — код вида "paris2023:sig_dupreeh_champion").

ИСТОЧНИКИ ЦЕНЫ, по приоритету:
1. Skinport (/v1/items) — открытый API, без ключа, весь каталог CS2 одним
   запросом. Основной источник теперь, т.к. Steam priceoverview/market-search
   банит датацентровые IP (Render) целиком — см. диагностику от 2026-08-11.
2. Steam (priceoverview -> market/search) — резерв для того, чего нет
   на Skinport (низколиквидные/очень редкие стикеры). Может по-прежнему
   429-ить, если IP всё ещё забанен — тогда просто ничего не находит,
   но не валит остальной пайплайн.

Это не влияет на другой источник данных — листинги скинов (какие стикеры
на них наклеены) по-прежнему приходят либо через steam_client.fetch_all_listings,
либо через вручную присланный Steam JSON (bot-16.handle_document). Здесь
только цена самого стикера.
"""

import asyncio
import json
import logging
import re
import time
from pathlib import Path

import aiohttp

from sticker_catalog import get_catalog
from storage import get_price, set_price

log = logging.getLogger("steam_bot.pricing")

OVERRIDES_PATH = Path(__file__).parent / "sticker_overrides.json"
CACHE_TTL_SECONDS = 12 * 60 * 60  # 12 часов — цены на стикеры не скачут ежеминутно

MAX_RETRIES_429 = 3
RETRY_BASE_DELAY = 8  # секунд, растёт экспоненциально: 8, 16, 32
REQUEST_DELAY = 1.2  # пауза между запросами к Steam priceoverview
SEARCH_REQUEST_DELAY = 4.0  # Steam market/search лимитируется жёстче

# --- Ручной прайс-лист от пользователя (Steam market/search/render, категория
# стикеров, скачано вручную через телефон/браузер — не банится, в отличие от
# сервера). Проверяется ПЕРВЫМ, раньше Skinport: это настоящие Steam-цены,
# точнее кросс-маркетной оценки. ---
MANUAL_PRICE_CACHE_PATH = Path(__file__).parent / "manual_sticker_prices.json"

SKINPORT_ITEMS_URL = "https://api.skinport.com/v1/items"
SKINPORT_CACHE_PATH = Path(__file__).parent / "skinport_cache.json"
SKINPORT_CACHE_TTL_SECONDS = 10 * 60  # Skinport сам кэширует ответ на 5 мин на своей стороне
SKINPORT_HEADERS = {"Accept-Encoding": "br"}  # обязателен для /v1/items — иначе 400

DIAGNOSTIC_PROBE_NAME = "Sticker | Katowice 2014 (Holo)"

# collection-код из имени файла -> человекочитаемое название турнира/капсулы
COLLECTION_TO_EVENT = {
    "paris2023": "Paris 2023",
    "cph2024": "Copenhagen 2024",
    "antwerp2022": "Antwerp 2022",
    "rio2022": "Rio 2022",
    "sha2024": "Shanghai 2024",
    "rmr2020": "2020 RMR",
    "stockh2021": "Stockholm 2021",
    "berlin2019": "Berlin 2019",
    "bud2025": "Budapest 2025",
    "cologne2016": "Cologne 2016",
    "cologne2026": "Cologne 2026",
    "csgo10": "10 Year Birthday",
    "aus2025": "Austin 2025",
}

FINISH_WORDS = ("glitter", "holo", "foil", "gold", "champion", "embroidered")


def _split_code(code: str) -> tuple[str, list[str]]:
    """'sig_dupreeh_champion' -> ('dupreeh', ['champion'])"""
    parts = code.split("_")
    finishes = [p for p in parts if p in FINISH_WORDS]
    name_parts = [p for p in parts if p not in FINISH_WORDS and p != "sig"]
    return " ".join(name_parts), finishes


def _resolve_market_hash_name(sticker_key: str, overrides: dict, catalog: dict) -> tuple[str, bool]:
    """
    Возвращает (market_hash_name_или_поисковый_запрос, is_exact).
    Порядок приоритета: ручной override -> точный каталог CSGO-API -> старая эвристика (fallback).
    """
    if sticker_key in overrides:
        return overrides[sticker_key], True

    if sticker_key in catalog:
        return catalog[sticker_key], True

    collection, code = sticker_key.split(":", 1)
    name, finishes = _split_code(code)
    event = COLLECTION_TO_EVENT.get(collection, collection)
    finish_str = f" ({', '.join(f.capitalize() for f in finishes)})" if finishes else ""
    return f"{name}{finish_str} {event}", False


def _load_overrides() -> dict:
    if OVERRIDES_PATH.exists():
        return json.loads(OVERRIDES_PATH.read_text(encoding="utf-8"))
    return {}


# ---------------------------------------------------------------------------
# Ручной прайс-лист (вручную скачанный Steam market/search/render по категории
# стикеров — bot.py.handle_document распознаёт такой файл и зовёт ingest_manual_prices)
# ---------------------------------------------------------------------------

def _load_manual_prices() -> dict[str, float]:
    if not MANUAL_PRICE_CACHE_PATH.exists():
        return {}
    try:
        return json.loads(MANUAL_PRICE_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        log.exception("manual_prices: не удалось прочитать %s", MANUAL_PRICE_CACHE_PATH)
        return {}


def ingest_manual_prices(items: dict[str, float]) -> int:
    """
    Сливает новые цены в постоянный локальный прайс-лист (не TTL-кэш —
    хранится, пока сам файл не перетрут явно). Вызывается из bot.py при
    получении JSON вида market/search/render с ценами по категории стикеров.
    Возвращает количество реально добавленных/обновлённых записей.
    """
    current = _load_manual_prices()
    changed = 0
    for name, price in items.items():
        if not name or price is None:
            continue
        if current.get(name) != price:
            changed += 1
        current[name] = price

    MANUAL_PRICE_CACHE_PATH.write_text(json.dumps(current, ensure_ascii=False), encoding="utf-8")
    log.info("manual_prices: сохранено %s записей (%s новых/изменённых), всего в файле %s", len(items), changed, len(current))
    return changed


def clear_manual_prices() -> int:
    """Полностью стирает ручной прайс-лист (перед загрузкой свежего). Возвращает, сколько записей было."""
    count = len(_load_manual_prices())
    if MANUAL_PRICE_CACHE_PATH.exists():
        MANUAL_PRICE_CACHE_PATH.unlink()
    log.info("manual_prices: очищено (было %s записей)", count)
    return count


def manual_prices_count() -> int:
    return len(_load_manual_prices())


# ---------------------------------------------------------------------------
# Skinport
# ---------------------------------------------------------------------------

def _load_skinport_cache() -> tuple[dict, float] | None:
    if not SKINPORT_CACHE_PATH.exists():
        return None
    try:
        data = json.loads(SKINPORT_CACHE_PATH.read_text(encoding="utf-8"))
        return data["catalog"], data["updated_at"]
    except Exception:
        return None


def _save_skinport_cache(catalog: dict) -> None:
    SKINPORT_CACHE_PATH.write_text(
        json.dumps({"catalog": catalog, "updated_at": time.time()}, ensure_ascii=False),
        encoding="utf-8",
    )


async def _download_skinport_catalog(session: aiohttp.ClientSession) -> dict[str, dict]:
    """
    Один запрос -> весь каталог CS2 с ценами. Ключ: market_hash_name.
    Возвращает {} при любой ошибке (вызывающий код тогда падает на Steam-фолбэк).
    """
    params = {"app_id": 730, "currency": "USD", "tradable": "false"}
    try:
        async with session.get(
            SKINPORT_ITEMS_URL, params=params, headers=SKINPORT_HEADERS,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status != 200:
                log.warning("skinport: HTTP %s при получении каталога", resp.status)
                return {}
            items = await resp.json(content_type=None)
    except Exception:
        log.exception("skinport: не удалось скачать/распарсить каталог")
        return {}

    catalog = {item["market_hash_name"]: item for item in items if item.get("market_hash_name")}
    log.info("skinport: получено %s предметов в каталоге", len(catalog))
    return catalog


async def get_skinport_catalog(session: aiohttp.ClientSession, force_refresh: bool = False) -> dict[str, dict]:
    cached = _load_skinport_cache()
    if not force_refresh and cached and (time.time() - cached[1]) < SKINPORT_CACHE_TTL_SECONDS:
        return cached[0]

    fresh = await _download_skinport_catalog(session)
    if fresh:
        _save_skinport_cache(fresh)
        return fresh

    if cached:
        return cached[0]
    return {}


def _price_from_skinport_entry(entry: dict) -> float | None:
    """median_price честнее лоя-снайпа; если пусто (нет лотов) — квантити=0, считаем не найденным."""
    if not entry or entry.get("quantity", 0) == 0:
        return None
    price = entry.get("median_price") or entry.get("min_price")
    return float(price) if price is not None else None


# ---------------------------------------------------------------------------
# Steam (фолбэк для того, чего нет на Skinport)
# ---------------------------------------------------------------------------

class RateLimited(Exception):
    """Внутренний маркер: после MAX_RETRIES_429 попыток всё ещё 429."""


async def _get_with_retry(session: aiohttp.ClientSession, url: str, params: dict, label: str):
    delay = RETRY_BASE_DELAY
    for attempt in range(1, MAX_RETRIES_429 + 1):
        async with session.get(url, params=params) as resp:
            if resp.status == 429:
                log.warning(
                    "%s: HTTP 429 (попытка %s/%s) для запроса %r",
                    label, attempt, MAX_RETRIES_429, params.get("query") or params.get("market_hash_name"),
                )
                if attempt == MAX_RETRIES_429:
                    raise RateLimited()
                await asyncio.sleep(delay)
                delay *= 2
                continue
            if resp.status != 200:
                log.warning("%s: HTTP %s для запроса %r", label, resp.status, params.get("query") or params.get("market_hash_name"))
                return None
            return await resp.json()
    raise RateLimited()


async def _price_overview(session: aiohttp.ClientSession, market_hash_name: str) -> float | None:
    url = "https://steamcommunity.com/market/priceoverview/"
    params = {"appid": 730, "currency": 1, "market_hash_name": market_hash_name}
    data = await _get_with_retry(session, url, params, "priceoverview")

    if not data or not data.get("success"):
        return None

    raw = data.get("median_price") or data.get("lowest_price")
    if not raw:
        return None
    digits = "".join(c for c in raw if c.isdigit() or c == ".")
    try:
        return float(digits)
    except ValueError:
        return None


async def _search_price(session: aiohttp.ClientSession, query: str) -> tuple[str, float] | None:
    url = "https://steamcommunity.com/market/search/render/"
    params = {
        "query": query, "start": 0, "count": 1,
        "search_descriptions": 0, "appid": 730, "norender": 1,
    }
    data = await _get_with_retry(session, url, params, "market/search")

    if not data:
        return None
    results = data.get("results") or []
    if not results:
        return None

    item = results[0]
    price_cents = item.get("sell_price")
    if price_cents is None:
        return None
    return item.get("name", query), price_cents / 100.0


async def _fetch_from_steam(session: aiohttp.ClientSession, key: str, name_or_query: str, is_exact: bool) -> tuple[str, float, bool]:
    """Возвращает (имя, цена, ok). ok=False -> рейт-лимит, не кэшировать как 0.0."""
    try:
        if is_exact:
            price = await _price_overview(session, name_or_query)
            if price is not None:
                return name_or_query, price, True
            found = await _search_price(session, name_or_query)
            return (found[0], found[1], True) if found else (name_or_query, 0.0, True)

        found = await _search_price(session, name_or_query)
        return (found[0], found[1], True) if found else (name_or_query, 0.0, True)

    except RateLimited:
        return name_or_query, 0.0, False


async def _fetch_one_price(session: aiohttp.ClientSession, key: str, overrides: dict, catalog: dict) -> tuple[str, float]:
    """
    Обратная совместимость со старой сигнатурой — её напрямую импортирует
    prewarm.py. Логика та же: Skinport -> Steam, но без разделения на
    "не найдено" / "рейт-лимит" (тут по старому контракту всегда возвращаем
    число, 0.0 в худшем случае). Основной путь get_sticker_prices() эту
    функцию не использует — там своя, более аккуратная логика с ретраями.
    """
    name_or_query, is_exact = _resolve_market_hash_name(key, overrides, catalog)

    if is_exact:
        skinport_catalog = await get_skinport_catalog(session)
        sp_price = _price_from_skinport_entry(skinport_catalog.get(name_or_query))
        if sp_price is not None:
            return name_or_query, sp_price

    if await _diagnose_steam_rate_limit(session):
        return name_or_query, 0.0

    matched_name, price, _ok = await _fetch_from_steam(session, key, name_or_query, is_exact)
    return matched_name, price


async def _diagnose_steam_rate_limit(session: aiohttp.ClientSession) -> bool:
    """
    Один быстрый пробный запрос (без ретраев) — проверяет, забанен ли
    сейчас IP целиком. Возвращает True, если забанен (429 на priceoverview).
    На основе этого get_sticker_prices решает, стоит ли вообще пытаться
    достучаться до Steam по остальным ключам в этом прогоне, или сразу
    считать их "не найдено" и не жечь по 20-30 сек на ретраи впустую.
    """
    url = "https://steamcommunity.com/market/priceoverview/"
    params = {"appid": 730, "currency": 1, "market_hash_name": DIAGNOSTIC_PROBE_NAME}
    try:
        async with session.get(url, params=params) as resp:
            if resp.status == 429:
                log.warning("ДИАГНОСТИКА: Steam priceoverview 429 — IP всё ещё забанен, Steam-фолбэк пропускается в этом прогоне")
                return True
            if resp.status == 200:
                data = await resp.json()
                if not data.get("success"):
                    log.warning("ДИАГНОСТИКА: priceoverview 200 но success=false для %r", DIAGNOSTIC_PROBE_NAME)
                    return False
                log.info("ДИАГНОСТИКА: Steam priceoverview отвечает нормально — фолбэк работает")
                return False
    except Exception:
        log.exception("ДИАГНОСТИКА: не удалось выполнить пробный запрос к priceoverview")
    return False


# ---------------------------------------------------------------------------
# Основная точка входа — сигнатура не менялась, вызывающий код (в т.ч.
# обработка вручную присланного Steam-JSON в bot-16.handle_document) не трогаем.
# ---------------------------------------------------------------------------

async def get_sticker_prices(sticker_keys: set[str]) -> dict[str, float]:
    """
    На входе — множество ключей вида 'paris2023:sig_dupreeh_champion'.
    На выходе — {ключ: цена в USD}.
    Порядок поиска цены: кэш -> Skinport -> Steam (только для того, чего нет на Skinport).
    Ключи, упёршиеся в Steam-рейт-лимит, не кэшируются нулём — подхватятся в следующем прогоне.
    """
    overrides = _load_overrides()
    catalog = await get_catalog()
    manual_prices = _load_manual_prices()

    now = time.time()
    result: dict[str, float] = {}
    to_fetch = []

    for key in sticker_keys:
        cached = await get_price(key)
        if cached and (now - cached["updated_at"]) < CACHE_TTL_SECONDS:
            result[key] = cached["price"]
        else:
            to_fetch.append(key)

    if not to_fetch:
        return result

    # Ручной прайс-лист проверяем сразу, без сети — это настоящие Steam-цены,
    # присланные пользователем вручную (собраны с телефона, не банятся).
    still_to_fetch = []
    for key in to_fetch:
        name_or_query, is_exact = _resolve_market_hash_name(key, overrides, catalog)
        if is_exact and name_or_query in manual_prices:
            price = manual_prices[name_or_query]
            await set_price(key, name_or_query, price, CACHE_TTL_SECONDS)
            result[key] = price
        else:
            still_to_fetch.append(key)

    if manual_prices and len(still_to_fetch) < len(to_fetch):
        log.info("get_sticker_prices: %s ключей закрыто ручным прайс-листом", len(to_fetch) - len(still_to_fetch))

    to_fetch = still_to_fetch
    if not to_fetch:
        return result

    steam_fallback_needed = []
    rate_limited_count = 0

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        skinport_catalog = await get_skinport_catalog(session)
        if not skinport_catalog:
            log.warning("get_sticker_prices: каталог Skinport пуст/недоступен, все ключи уйдут сразу на Steam-фолбэк")

        for key in to_fetch:
            name_or_query, is_exact = _resolve_market_hash_name(key, overrides, catalog)

            sp_price = _price_from_skinport_entry(skinport_catalog.get(name_or_query)) if is_exact else None
            if sp_price is not None:
                await set_price(key, name_or_query, sp_price, CACHE_TTL_SECONDS)
                result[key] = sp_price
            else:
                steam_fallback_needed.append((key, name_or_query, is_exact))

        if steam_fallback_needed:
            log.info("get_sticker_prices: %s ключей не нашлись на Skinport, идут на Steam-фолбэк", len(steam_fallback_needed))
            is_banned = await _diagnose_steam_rate_limit(session)

            if is_banned:
                # Один быстрый пробный запрос уже показал 429 — весь IP забанен.
                # Гонять по 3 ретрая с экспоненциальным backoff на каждый из
                # оставшихся ключей бессмысленно (только жжёт 20-30+ минут впустую
                # на заведомо провальные попытки) — сразу помечаем как "не найдено"
                # в этом прогоне. Ничего не кэшируем нулём, чтобы при следующем
                # /scan (когда бан, возможно, снимется) эти ключи пересчитались заново.
                log.warning(
                    "get_sticker_prices: IP забанен целиком — пропускаю Steam-ретраи "
                    "для %s ключей без попыток (будут пересчитаны, когда бан снимется)",
                    len(steam_fallback_needed),
                )
                rate_limited_count += len(steam_fallback_needed)
            else:
                for key, name_or_query, is_exact in steam_fallback_needed:
                    matched_name, price, ok = await _fetch_from_steam(session, key, name_or_query, is_exact)
                    if ok:
                        await set_price(key, matched_name, price, CACHE_TTL_SECONDS)
                        result[key] = price
                    else:
                        rate_limited_count += 1
                    await asyncio.sleep(REQUEST_DELAY if is_exact else SEARCH_REQUEST_DELAY)

    if rate_limited_count:
        log.warning(
            "get_sticker_prices: %s ключей не удалось узнать (Steam недоступен) "
            "(не закэшированы нулём, будут повторены в следующем прогоне)",
            rate_limited_count,
        )

    return result
