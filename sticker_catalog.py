"""
Точный каталог "код иконки стикера -> market_hash_name" из открытого проекта
ByMykel/CSGO-API (статический JSON, обновляется мейнтейнерами вручную/по CI,
никаких рейт-лимитов и логина не требует).

Формат их данных (поле "image" у каждого стикера) — это ровно тот же путь,
что мы уже достаём регэкспом steam_client.STICKER_RE из иконок в листингах:

    https://cdn.steamstatic.com/apps/730/icons/econ/stickers/emskatowice2014/titan_1355_37.<hash>.png
                                                              ^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^
                                                              collection      code

Поэтому парсим их image тем же регэкспом и получаем словарь:
    "emskatowice2014:titan_1355_37" -> "Sticker | Titan | Katowice 2014"

Это ТОЧНОЕ имя, никакого угадывания. Старая эвристика в pricing.py остаётся
только как fallback на случай, если каталог ещё не успели обновить под
свежий релиз стикеров.
"""

import json
import time
from pathlib import Path

import aiohttp

from steam_client import STICKER_RE

CATALOG_CACHE_PATH = Path(__file__).parent / "sticker_catalog_cache.json"
CATALOG_TTL_SECONDS = 24 * 60 * 60  # каталог стикеров меняется редко — раз в сутки достаточно

SOURCE_URLS = [
    "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/stickers.json",
    "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/sticker_slabs.json",
]


def _extract_key(image_url: str) -> str | None:
    m = STICKER_RE.search(image_url)
    if not m:
        return None
    collection, code = m.groups()
    return f"{collection}:{code}"


async def _download_catalog() -> dict[str, str]:
    catalog: dict[str, str] = {}
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        for url in SOURCE_URLS:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        continue
                    items = await resp.json()
            except Exception:
                # источник временно недоступен — не валим весь бот, просто без этого файла
                continue

            for item in items:
                image = item.get("image")
                name = item.get("market_hash_name")
                if not image or not name:
                    continue
                key = _extract_key(image)
                if key:
                    catalog[key] = name

    return catalog


def _load_cache() -> tuple[dict[str, str], float] | None:
    if not CATALOG_CACHE_PATH.exists():
        return None
    try:
        data = json.loads(CATALOG_CACHE_PATH.read_text(encoding="utf-8"))
        return data["catalog"], data["updated_at"]
    except Exception:
        return None


def _save_cache(catalog: dict[str, str]) -> None:
    CATALOG_CACHE_PATH.write_text(
        json.dumps({"catalog": catalog, "updated_at": time.time()}, ensure_ascii=False),
        encoding="utf-8",
    )


async def get_catalog(force_refresh: bool = False) -> dict[str, str]:
    """
    Отдаёт словарь "коллекция:код" -> точное market_hash_name.
    Кэш на диске на 24 часа, чтобы не дёргать GitHub на каждый /scan.
    Если скачивание не удалось, а старый кэш есть — отдаём старый кэш
    (лучше устаревшая, но рабочая карта, чем вообще без неё).
    """
    cached = _load_cache()
    if not force_refresh and cached and (time.time() - cached[1]) < CATALOG_TTL_SECONDS:
        return cached[0]

    fresh = await _download_catalog()
    if fresh:
        _save_cache(fresh)
        return fresh

    # скачивание не удалось — используем что было, пусть и протухшее
    if cached:
        return cached[0]
    return {}
