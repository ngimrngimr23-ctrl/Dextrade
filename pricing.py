"""
Определение цены стикера по коду, вытащенному из имени файла иконки
(см. steam_client.STICKER_RE — код вида "paris2023:sig_dupreeh_champion").

ВАЖНАЯ ОГОВОРКА:
Steam НЕ отдаёт человекочитаемое имя стикера в листингах Market — только иконки.
Поэтому мы восстанавливаем примерное название и ищем его через
Market Search API, который сам подбирает ближайший предмет и отдаёт цену.
Это эвристика: на редких/новых кодах может промахнуться.
Файл sticker_overrides.json — твой способ поправить конкретные коды руками,
если бот подобрал не тот стикер (см. README).
"""

import json
import logging
import re
import time
from pathlib import Path

import aiohttp

log = logging.getLogger("steam_bot.pricing")

from sticker_catalog import get_catalog
from storage import get_price, set_price

OVERRIDES_PATH = Path(__file__).parent / "sticker_overrides.json"
CACHE_TTL_SECONDS = 12 * 60 * 60  # 12 часов — цены на стикеры не скачут ежеминутно

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

    # Fallback: каталог мог не успеть обновиться под самый свежий релиз стикеров —
    # угадываем по коду, как раньше, но это уже редкий случай, а не основной путь.
    collection, code = sticker_key.split(":", 1)
    name, finishes = _split_code(code)
    event = COLLECTION_TO_EVENT.get(collection, collection)
    finish_str = f" ({', '.join(f.capitalize() for f in finishes)})" if finishes else ""
    return f"{name}{finish_str} {event}", False


def _load_overrides() -> dict:
    if OVERRIDES_PATH.exists():
        return json.loads(OVERRIDES_PATH.read_text(encoding="utf-8"))
    return {}


async def _price_overview(session: aiohttp.ClientSession, market_hash_name: str) -> float | None:
    """
    Точное имя уже известно (из каталога/override) -> берём priceoverview.
    Он даёт lowest_price и median_price; median честнее для нашей задачи,
    чем "самый дешёвый активный лот" из Market Search.
    """
    url = "https://steamcommunity.com/market/priceoverview/"
    params = {"appid": 730, "currency": 1, "market_hash_name": market_hash_name}
    async with session.get(url, params=params) as resp:
        if resp.status != 200:
            log.warning("priceoverview: HTTP %s для %s", resp.status, market_hash_name)
            return None
        ctype = resp.content_type
        if ctype != "application/json":
            body_start = (await resp.text())[:200]
            log.warning(
                "priceoverview: не JSON (%s) для %s, похоже на блокировку Steam: %r",
                ctype, market_hash_name, body_start,
            )
            return None
        data = await resp.json()

    if not data.get("success"):
        log.warning("priceoverview: success=false для %s (%r)", market_hash_name, data)
        return None

    raw = data.get("median_price") or data.get("lowest_price")
    if not raw:
        return None
    # формат вида "$1.23" -> 1.23
    digits = "".join(c for c in raw if c.isdigit() or c == ".")
    try:
        return float(digits)
    except ValueError:
        return None


async def _search_price(session: aiohttp.ClientSession, query: str) -> tuple[str, float] | None:
    """Fallback для случаев без точного имени — как и раньше, ищем по подобранному запросу."""
    url = "https://steamcommunity.com/market/search/render/"
    params = {
        "query": query,
        "start": 0,
        "count": 1,
        "search_descriptions": 0,
        "appid": 730,
        "norender": 1,
    }
    async with session.get(url, params=params) as resp:
        if resp.status != 200:
            log.warning("market/search: HTTP %s для запроса %r", resp.status, query)
            return None
        ctype = resp.content_type
        if ctype != "application/json":
            body_start = (await resp.text())[:200]
            log.warning(
                "market/search: не JSON (%s) для запроса %r, похоже на блокировку Steam: %r",
                ctype, query, body_start,
            )
            return None
        data = await resp.json()

    results = data.get("results") or []
    if not results:
        log.warning("market/search: пусто для запроса %r", query)
        return None

    item = results[0]
    price_cents = item.get("sell_price")
    if price_cents is None:
        return None
    return item.get("name", query), price_cents / 100.0


async def _fetch_one_price(session: aiohttp.ClientSession, key: str, overrides: dict, catalog: dict) -> tuple[str, float]:
    name_or_query, is_exact = _resolve_market_hash_name(key, overrides, catalog)
    if is_exact:
        price = await _price_overview(session, name_or_query)
        if price is not None:
            return name_or_query, price
        # priceoverview иногда пуст для совсем неликвидных стикеров — пробуем search тем же именем
        found = await _search_price(session, name_or_query)
        return (found[0], found[1]) if found else (name_or_query, 0.0)

    found = await _search_price(session, name_or_query)
    return (found[0], found[1]) if found else (name_or_query, 0.0)


async def get_sticker_prices(sticker_keys: set[str]) -> dict[str, float]:
    """
    На входе — множество ключей вида 'paris2023:sig_dupreeh_champion'.
    На выходе — {ключ: цена в USD}. Отсутствующие/не найденные просто не попадут в словарь.
    """
    overrides = _load_overrides()
    catalog = await get_catalog()
    now = time.time()
    result: dict[str, float] = {}
    to_fetch = []

    for key in sticker_keys:
        cached = await get_price(key)
        if cached and (now - cached["updated_at"]) < CACHE_TTL_SECONDS:
            result[key] = cached["price"]
        else:
            to_fetch.append(key)

    if to_fetch:
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            for key in to_fetch:
                matched_name, price = await _fetch_one_price(session, key, overrides, catalog)
                await set_price(key, matched_name, price, CACHE_TTL_SECONDS)
                result[key] = price
                # Steam лимитирует и priceoverview, и search — не долбим часто
                import asyncio
                await asyncio.sleep(1.2)

    zero_count = sum(1 for v in result.values() if v == 0.0)
    log.info(
        "get_sticker_prices: %s ключей, %s с нулевой ценой (не найдено/заблокировано)",
        len(result), zero_count,
    )
    return result
    
