"""
Точный каталог "код иконки стикера -> market_hash_name" из открытого проекта
ByMykel/CSGO-API (статический JSON, обновляется мейнтейнерами вручную/по CI,
никаких рейт-лимитов и логина не требует).

Ключ строим из поля "original.image_inventory" — оно есть у каждого стикера
(и в stickers.json, и в sticker_slabs.json) и выглядит как:

    "econ/stickers/emskatowice2014/titan_1355_37"
                    ^^^^^^^^^^^^^^^ ^^^^^^^^^^^^^^
                    collection      code

Это ровно тот же collection/code, что steam_client.STICKER_RE достаёт
регэкспом из иконок в листингах — только без хэша и без завязки на конкретный
CDN. ВАЖНО: поле "image" (сама картинка) с 2026 у обычных стикеров указывает
на репозиторий ByMykel/counter-strike-image-tracker с урезанным именем файла
(без code/хэша вообще) — регэксп по нему почти всегда промахивается, поэтому
"image" используется только как последний fallback, если "original" вдруг
отсутствует.

Получаем словарь:
    "emskatowice2014:titan_1355_37" -> "Sticker | Titan | Katowice 2014"

Это ТОЧНОЕ имя, никакого угадывания. Старая эвристика в pricing.py остаётся
только как fallback на случай, если каталог ещё не успели обновить под
свежий релиз стикеров.
"""

import json
import logging
import time
from pathlib import Path

import aiohttp

log = logging.getLogger("steam_bot.sticker_catalog")

from steam_client import STICKER_RE

CATALOG_CACHE_PATH = Path(__file__).parent / "sticker_catalog_cache.json"
CATALOG_TTL_SECONDS = 24 * 60 * 60  # каталог стикеров меняется редко — раз в сутки достаточно

SOURCE_URLS = [
    "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/stickers.json",
    "https://raw.githubusercontent.com/ByMykel/CSGO-API/main/public/api/en/sticker_slabs.json",
]


def _extract_key(item: dict) -> str | None:
    # Основной путь: original.image_inventory вида "econ/stickers/<collection>/<code>".
    inv_path = (item.get("original") or {}).get("image_inventory")
    if inv_path:
        parts = inv_path.split("/")
        if len(parts) >= 2:
            collection, code = parts[-2], parts[-1]
            return f"{collection}:{code}"

    # Fallback на случай, если original когда-нибудь пропадёт из данных —
    # старый регэксп по URL картинки (работает не для всех записей, см. докстринг).
    image = item.get("image")
    if image:
        m = STICKER_RE.search(image)
        if m:
            collection, code = m.groups()
            return f"{collection}:{code}"

    return None


async def _download_catalog() -> dict[str, str]:
    catalog: dict[str, str] = {}
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        for url in SOURCE_URLS:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        log.warning("sticker_catalog: HTTP %s для %s", resp.status, url)
                        continue
                    # raw.githubusercontent.com отдаёт JSON с Content-Type: text/plain,
                    # поэтому просим aiohttp не проверять mimetype строго (та же история,
                    # что и в csgo_api.py)
                    items = await resp.json(content_type=None)
            except Exception:
                # источник временно недоступен — не валим весь бот, просто без этого файла
                log.exception("sticker_catalog: не удалось скачать/распарсить %s", url)
                continue

            added = 0
            skipped_no_name = 0
            for item in items:
                name = item.get("market_hash_name")
                if not name:
                    # часть базовых/промо-стикеров не продаётся на маркете вообще —
                    # market_hash_name у них null, это нормально, не баг
                    skipped_no_name += 1
                    continue
                key = _extract_key(item)
                if key:
                    catalog[key] = name
                    added += 1

            log.info(
                "sticker_catalog: %s записей из %s (пропущено без market_hash_name: %s)",
                added, url, skipped_no_name,
            )

    log.info("sticker_catalog: итого в каталоге %s ключей", len(catalog))
    return catalog


def _load_cache() -> tuple[dict[str, str], float] | None:
    """
    Дисковый кэш каталога, разобранный один раз на версию файла.

    Раньше файл читался и парсился заново на каждый предмет прогона (через
    get_catalog -> get_sticker_prices). Каталог обновляется раз в сутки, так
    что смысла в этом не было никакого — см. тот же приём в pricing._read_json_memo.
    """
    stamp = _file_stamp(CATALOG_CACHE_PATH)
    if stamp is None:
        return None

    global _CACHE_MEMO
    if _CACHE_MEMO is not None and _CACHE_MEMO[0] == stamp:
        return _CACHE_MEMO[1]

    try:
        data = json.loads(CATALOG_CACHE_PATH.read_text(encoding="utf-8"))
        parsed = (data["catalog"], data["updated_at"])
    except Exception:
        return None

    _CACHE_MEMO = (stamp, parsed)
    return parsed


def _file_stamp(path: Path) -> tuple[int, int] | None:
    """(mtime в нс, размер) — отпечаток файла. None, если файла нет."""
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


_CACHE_MEMO: tuple[tuple[int, int], tuple[dict[str, str], float]] | None = None


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
    
