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
from steam_client import steam_cooldown_remaining
from storage import get_prices_batch, set_price, all_known_keys

import aiohttp

log = logging.getLogger("prewarm")

LOOP_INTERVAL_SECONDS = 30 * 60  # раз в полчаса проверяем, что скоро протухнет
REFRESH_MARGIN_SECONDS = 2 * 60 * 60  # обновляем, если до протухания < 2 часов
REQUEST_DELAY = 1.5

# Пауза ПЕРЕД первым проходом после старта процесса.
# Зачем: Render передеплоивает бота на каждый git push, а прогон стартовал
# мгновенно при запуске. В день активной отладки это 8+ рестартов, и каждый
# тут же принимался обновлять цены — то есть вместо одного неспешного фонового
# прохода Steam получал очереди запросов с одного IP весь день. Запас времени
# тут ничего не стоит: обновляем то, чему до протухания ещё REFRESH_MARGIN
# (2 часа), так что десять минут погоды не делают, а серию быстрых
# передеплоев отсекают целиком.
INITIAL_DELAY_SECONDS = 10 * 60


async def _prewarm_once():
    # scope="pricing" — пре-варминг ходит только за ценой стикеров
    # (priceoverview/market-search), у этой области свой кулдаун, отдельный
    # от листингов/вотчлиста (см. steam_client.py).
    cooldown = steam_cooldown_remaining(scope="pricing")
    if cooldown > 0:
        # Steam всё ещё на кулдауне после 429 — раньше пре-варминг всё равно
        # лез за Steam-фолбэком по каждому непокрытому csgotrader.app ключу,
        # игнорируя кулдаун, и тем самым лишний раз тыкал забаненный IP
        # каждые LOOP_INTERVAL_SECONDS. Теперь просто ждём.
        log.info("prewarm: пропускаю — кулдаун цен стикеров ещё %.0f мин", cooldown / 60)
        return

    now = time.time()
    keys = await all_known_keys()
    if not keys:
        log.info("prewarm: нечего обновлять")
        return

    cached_map = await get_prices_batch(keys)
    stale_keys = [
        key
        for key in keys
        if not cached_map.get(key)
        or (now - cached_map[key]["updated_at"]) > (CACHE_TTL_SECONDS - REFRESH_MARGIN_SECONDS)
    ]

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
    # Именно ДО цикла, а не внутри: иначе каждый рестарт процесса начинает
    # прогон немедленно (см. INITIAL_DELAY_SECONDS).
    log.info("prewarm: первый прогон через %.0f мин после старта", INITIAL_DELAY_SECONDS / 60)
    await asyncio.sleep(INITIAL_DELAY_SECONDS)
    while True:
        try:
            await _prewarm_once()
        except Exception:
            log.exception("prewarm_loop упал, пробую дальше")
        await asyncio.sleep(LOOP_INTERVAL_SECONDS)
