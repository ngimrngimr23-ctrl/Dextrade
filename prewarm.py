"""
Фоновый пре-варминг цен стикеров.

Идея: /scan не должен ждать живых запросов к Steam для стикеров, которые
уже встречались раньше. Эта задача сама, в фоне, обновляет цены тех ключей,
которым скоро (< REFRESH_MARGIN_SECONDS до истечения TTL) протухнет кэш —
так что к моменту, когда пользователь снова просканирует предмет с этими
стикерами, цена уже свежая и /scan не спотыкается о паузы между запросами
к Steam.

Не качает цены на ВСЕ стикеры из каталога (их тысячи, это бесполезно и
долго) — только на те, что реально встречались в ваших сканах (т.е. уже
есть запись в хранилище, см. storage.all_known_keys()).
"""

import asyncio
import logging
import time

from pricing import CACHE_TTL_SECONDS, _load_overrides, _fetch_one_price
from sticker_catalog import get_catalog
from storage import get_price, set_price, all_known_keys

import aiohttp

log = logging.getLogger("prewarm")

LOOP_INTERVAL_SECONDS = 30 * 60  # раз в полчаса проверяем, что скоро протухнет
REFRESH_MARGIN_SECONDS = 2 * 60 * 60  # обновляем, если до протухания < 2 часов
REQUEST_DELAY = 1.5


async def _prewarm_once():
    now = time.time()
    keys = await all_known_keys()
    if not keys:
        log.info("prewarm: нечего обновлять")
        return

    stale_keys = []
    for key in keys:
        cached = await get_price(key)
        if not cached or (now - cached["updated_at"]) > (CACHE_TTL_SECONDS - REFRESH_MARGIN_SECONDS):
            stale_keys.append(key)

    if not stale_keys:
        log.info("prewarm: все %d стикер(ов) ещё свежие", len(keys))
        return

    log.info("prewarm: обновляю %d стикер(ов)", len(stale_keys))
    overrides = _load_overrides()
    catalog = await get_catalog()

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        for key in stale_keys:
            try:
                matched_name, price = await _fetch_one_price(session, key, overrides, catalog)
                await set_price(key, matched_name, price, CACHE_TTL_SECONDS)
            except Exception:
                log.exception("prewarm: не смог обновить %s", key)
            await asyncio.sleep(REQUEST_DELAY)


async def prewarm_loop():
    while True:
        try:
            await _prewarm_once()
        except Exception:
            log.exception("prewarm_loop упал, пробую дальше")
        await asyncio.sleep(LOOP_INTERVAL_SECONDS)
