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

ИЗМЕНЕНИЯ (диагностика 429 на market/search, 2026-08):
- если каталог (get_catalog) пустой — это раньше молча отправляло ВСЕ ключи
  в market/search (жёсткий рейт-лимит) вместо priceoverview (мягкий).
  Теперь это логируется явно.
- добавлен retry с backoff на 429 для обоих эндпоинтов.
- 0.0 больше не кэшируется при 429/сетевой ошибке — только при реальном
  "предмет не найден" (200 OK, но пустой результат), чтобы не запирать
  цену в ноль на 12 часов из-за временного рейт-лимита.
- добавлена одноразовая диагностика при старте get_sticker_prices:
  сравнивает, отвечает ли priceoverview на известном предмете, пока
  market/search уже 429-ит — чтобы понять, забанен IP целиком или
  только на search.
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
REQUEST_DELAY = 1.2  # пауза между запросами (используется для priceoverview)
SEARCH_REQUEST_DELAY = 4.0  # market/search лимитируется жёстче — пауза больше

# Предмет, который точно существует и стабильно торгуется — для диагностики
# "жив ли вообще priceoverview с этого IP", независимо от каталога стикеров.
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


class RateLimited(Exception):
    """Внутренний маркер: после MAX_RETRIES_429 попыток всё ещё 429."""


async def _get_with_retry(session: aiohttp.ClientSession, url: str, params: dict, label: str):
    """
    GET с retry на 429 (экспоненциальный backoff). Возвращает распарсенный JSON
    или None при не-429 ошибке. Бросает RateLimited, если все попытки съели 429 —
    это отдельный случай от "предмета не существует", чтобы вызывающий код не
    кэшировал 0.0 как будто предмет реально не найден.
    """
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
    """
    Точное имя уже известно (из каталога/override) -> берём priceoverview.
    Он даёт lowest_price и median_price; median честнее для нашей задачи,
    чем "самый дешёвый активный лот" из Market Search.
    """
    url = "https://steamcommunity.com/market/priceoverview/"
    params = {"appid": 730, "currency": 1, "market_hash_name": market_hash_name}
    data = await _get_with_retry(session, url, params, "priceoverview")

    if not data or not data.get("success"):
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


async def _fetch_one_price(session: aiohttp.ClientSession, key: str, overrides: dict, catalog: dict) -> tuple[str, float, bool]:
    """
    Возвращает (имя, цена, ok). ok=False означает "не удалось узнать цену
    из-за рейт-лимита/ошибки сети" — такое НЕ должно кэшироваться как 0.0,
    в отличие от настоящего "предмет не найден на маркете".
    """
    name_or_query, is_exact = _resolve_market_hash_name(key, overrides, catalog)
    try:
        if is_exact:
            price = await _price_overview(session, name_or_query)
            if price is not None:
                return name_or_query, price, True
            # priceoverview иногда пуст для совсем неликвидных стикеров — пробуем search тем же именем
            found = await _search_price(session, name_or_query)
            if found:
                return found[0], found[1], True
            return name_or_query, 0.0, True  # реально не найдено (200 OK, пусто)

        found = await _search_price(session, name_or_query)
        if found:
            return found[0], found[1], True
        return name_or_query, 0.0, True  # реально не найдено

    except RateLimited:
        return name_or_query, 0.0, False


async def _diagnose_rate_limit(session: aiohttp.ClientSession) -> None:
    """
    Одноразовая проверка при старте партии: жив ли priceoverview с этого IP
    независимо от каталога/search. Если он тоже 429-ит — банится сам IP
    (датацентровый Render-адрес), а не только market/search.
    Если priceoverview отвечает нормально, а search 429-ит — лимит именно
    на search, и решение — просто не ходить туда без необходимости.
    """
    url = "https://steamcommunity.com/market/priceoverview/"
    params = {"appid": 730, "currency": 1, "market_hash_name": DIAGNOSTIC_PROBE_NAME}
    try:
        async with session.get(url, params=params) as resp:
            if resp.status == 429:
                log.warning("ДИАГНОСТИКА: priceoverview ТОЖЕ вернул 429 — похоже, забанен сам IP, не только market/search")
            elif resp.status == 200:
                data = await resp.json()
                if data.get("success"):
                    log.info("ДИАГНОСТИКА: priceoverview отвечает нормально (%s) — бан похоже только на market/search", DIAGNOSTIC_PROBE_NAME)
                else:
                    log.warning("ДИАГНОСТИКА: priceoverview вернул 200, но success=false для %r", DIAGNOSTIC_PROBE_NAME)
            else:
                log.warning("ДИАГНОСТИКА: priceoverview вернул неожиданный статус %s", resp.status)
    except Exception:
        log.exception("ДИАГНОСТИКА: не удалось выполнить пробный запрос к priceoverview")


async def get_sticker_prices(sticker_keys: set[str]) -> dict[str, float]:
    """
    На входе — множество ключей вида 'paris2023:sig_dupreeh_champion'.
    На выходе — {ключ: цена в USD}. Отсутствующие/не найденные просто не попадут в словарь.
    Ключи, для которых запрос упёрся в рейт-лимит (а не "предмет реально не найден"),
    тоже не попадут в результат и не закэшируются нулём — их подхватит следующий прогон.
    """
    overrides = _load_overrides()
    catalog = await get_catalog()

    if not catalog:
        log.warning(
            "get_sticker_prices: каталог стикеров ПУСТ — все %s ключей уйдут в market/search "
            "(жёсткий рейт-лимит) вместо priceoverview. Проверь sticker_catalog: "
            "не упало ли скачивание с GitHub и не эфемерный ли диск под кэшем.",
            len(sticker_keys),
        )

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
        rate_limited_count = 0
        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            await _diagnose_rate_limit(session)

            for key in to_fetch:
                name_or_query, is_exact = _resolve_market_hash_name(key, overrides, catalog)
                matched_name, price, ok = await _fetch_one_price(session, key, overrides, catalog)

                if ok:
                    await set_price(key, matched_name, price, CACHE_TTL_SECONDS)
                    result[key] = price
                else:
                    rate_limited_count += 1

                # market/search лимитируется жёстче priceoverview — пауза больше именно для него
                await asyncio.sleep(REQUEST_DELAY if is_exact else SEARCH_REQUEST_DELAY)

        if rate_limited_count:
            log.warning(
                "get_sticker_prices: %s ключей из %s не удалось узнать из-за рейт-лимита "
                "(не закэшированы нулём, будут повторены в следующем прогоне)",
                rate_limited_count, len(to_fetch),
            )

    return result
    
