"""
Поиск предметов CS2 по названию через открытую статическую базу
ByMykel/CSGO-API (https://github.com/ByMykel/CSGO-API).

Это НЕ Steam API — данные хостятся на GitHub (raw.githubusercontent.com),
поэтому не подвержены блокировке датацентровых IP (Render и т.п.), с
которой сталкивается сам Steam Market. Данные собираются из игровых
файлов (items_game.txt) и обновляются периодически, так что самые
свежие кейсы могут появиться в базе с задержкой в несколько дней.

Важно: в опубликованном виде (что на bymykel.github.io/bymykel.com,
что на raw.githubusercontent.com) реально выложено только 2 языка —
en и zh-CN. Русского (ru) там нет, поэтому ищем по английским
названиям (например "AK-47 | Slate", а не "AK-47 | Сланец").
"""

import time

import aiohttp

CSGO_API_BASE = "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api"
_CACHE_TTL = 6 * 3600  # обновляем раз в 6 часов, база меняется редко

# language -> (timestamp_загрузки, список_предметов)
_cache: dict[str, tuple[float, list[dict]]] = {}


async def _load_items(language: str = "en") -> list[dict]:
    now = time.time()
    cached = _cache.get(language)
    if cached and now - cached[0] < _CACHE_TTL:
        return cached[1]

    # skins_not_grouped.json — каждая пара скин+износ отдельной записью,
    # ровно то, что нужно для соответствия market_hash_name на Steam Market
    url = f"{CSGO_API_BASE}/{language}/skins_not_grouped.json"
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()

    _cache[language] = (now, data)
    return data


async def search_items(query: str, language: str = "en", count: int = 10) -> list[dict]:
    """
    Ищет предметы по названию (на английском — см. примечание в шапке
    файла) в статической базе (не Steam!). Возвращает список
    {"name": <английское название>, "hash_name": <market_hash_name>},
    отсортированный от более точных совпадений к менее точным.
    """
    items = await _load_items(language)
    q = query.strip().lower()
    if not q:
        return []

    scored = []
    for item in items:
        name = item.get("name") or ""
        hash_name = item.get("market_hash_name")
        if not hash_name:
            continue
        name_lower = name.lower()
        if q in name_lower:
            # точное совпадение всей строки и совпадения покороче — выше приоритет
            score = 0 if name_lower == q else len(name_lower)
            scored.append((score, name, hash_name))

    scored.sort(key=lambda t: t[0])

    seen = set()
    out = []
    for _, name, hash_name in scored:
        if hash_name in seen:
            continue
        seen.add(hash_name)
        out.append({"name": name, "hash_name": hash_name})
        if len(out) >= count:
            break
    return out
