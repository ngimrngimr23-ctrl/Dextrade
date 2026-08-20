"""
Цены сторонних площадок с prices.csgotrader.app — источник без лимитов.

Зачем отдельно от CSFloat. Весь арбитраж через API CSFloat упирается в квоту:
200 запросов в час на ключ, одна страница — 50 лотов, то есть потолок 10 000
лотов в час на всё про всё. Плюс блокировки по репутации адреса, из-за которых
понадобились резидентные прокси.

Здесь ничего этого нет. Это статические JSON-файлы на CDN, их отдают как
обычный ассет: ни ключа, ни квоты, ни антибота. Один запрос — и на руках цены
ВСЕГО каталога CS2 по площадке. Сравнение со Steam делается локально, поэтому
можно проверять все 34 тысячи предметов за прогон, а не выборку.

Чем за это платим, честно:
  * это АГРЕГАТ по предмету («минимальная цена на площадке»), а не конкретный
    лот. Нет ни флоата, ни наклеек, ни ссылки на сам лот;
  * файлы обновляются примерно раз в час, поэтому часть находок к моменту
    уведомления уже разберут;
  * состав площадок задаёт csgotrader, а не мы: какие файлы есть, выясняется
    опытом (см. discover_markets).

Для сигнала «предмет X на площадке Y дешевле Steam на N%» этого достаточно —
дальше человек открывает площадку и смотрит сам.
"""

from __future__ import annotations

import asyncio
import logging
import time
from urllib.parse import quote

import aiohttp

log = logging.getLogger("steam_bot.markets")

BASE_URL = "https://prices.csgotrader.app/latest"

# Кандидаты в площадки. Список из описания расширения CSGOTrader (там указаны
# провайдеры цен: Steam, CS.MONEY, Bitskins, LOOT.FARM, CSGO.TM) плюс пара
# очевидных вариантов написания. Какие из файлов существуют на самом деле,
# проверяется запросом — угадывать по именам смысла нет.
MARKET_CANDIDATES: dict[str, tuple[str, ...]] = {
    "Bitskins": ("bitskins.json",),
    "CS.MONEY": ("csmoney.json", "cs_money.json"),
    "LOOT.FARM": ("lootfarm.json", "loot_farm.json"),
    "CSGO.TM": ("csgotm.json", "market_csgo.json", "csgo_tm.json"),
    "Buff163": ("buff163.json",),
    "Skinport": ("skinport.json",),
    "Waxpeer": ("waxpeer.json",),
    "ShadowPay": ("shadowpay.json",),
}

# Ключи, под которыми в этих файлах встречается цена. Формат у площадок разный:
# где-то просто число, где-то вложенный объект. Перебираем известные варианты,
# а не закладываемся на один — иначе смена формата у одной площадки молча
# обнулила бы её, ровно как это случилось с item.scm у CSFloat.
_PRICE_KEYS = ("price", "starting_at", "lowest_price", "safe", "safe_price", "avg")


def _extract_price(value) -> float | None:
    """
    Достать цену из записи любого из встречающихся форматов.

    Возвращает None молча только для пустых значений; всё остальное, чего не
    смогли разобрать, пусть считает вызывающий — по количеству нераспознанных
    записей сразу видно, что формат сменился.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    if isinstance(value, str):
        try:
            price = float(value.replace("$", "").replace(",", "").strip())
        except ValueError:
            return None
        return price if price > 0 else None
    if isinstance(value, dict):
        for key in _PRICE_KEYS:
            if key in value:
                price = _extract_price(value[key])
                if price is not None:
                    return price
    return None


async def fetch_market(session: aiohttp.ClientSession, filename: str) -> dict[str, float]:
    """Цены одной площадки: market_hash_name -> цена в долларах. {} при любой ошибке."""
    url = f"{BASE_URL}/{filename}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status != 200:
                return {}
            raw = await resp.json(content_type=None)
    except Exception:
        log.info("markets: %s недоступен", filename)
        return {}

    if not isinstance(raw, dict):
        return {}

    prices: dict[str, float] = {}
    unparsed = 0
    for name, entry in raw.items():
        price = _extract_price(entry)
        if price is None:
            unparsed += 1
        else:
            prices[name] = price

    if unparsed and prices:
        log.info(
            "markets: %s — разобрано %d цен, не разобралось %d записей",
            filename, len(prices), unparsed,
        )
    return prices


async def _exists(session: aiohttp.ClientSession, filename: str) -> bool:
    """
    Есть ли такой файл — запросом HEAD, без скачивания тела.

    Первая версия проверяла существование обычным GET, то есть качала файл
    целиком ради ответа «да/нет». На восьми площадках с вариантами написания
    это до тринадцати полных загрузок по несколько мегабайт, после чего
    найденные качались ВТОРОЙ раз. На проде команда просто молчала минутами.
    """
    try:
        async with session.head(
            f"{BASE_URL}/{filename}",
            timeout=aiohttp.ClientTimeout(total=15),
            allow_redirects=True,
        ) as resp:
            return resp.status == 200
    except Exception:
        return False


async def discover_markets(session: aiohttp.ClientSession) -> dict[str, str]:
    """
    Какие площадки доступны: человекочитаемое имя -> имя файла.

    Состав файлов задаём не мы, поэтому имена не угадываются, а проверяются.
    Проверка идёт параллельно и через HEAD, так что стоит доли секунды и
    нисколько трафика.
    """
    async def probe(market: str, filenames: tuple[str, ...]):
        for filename in filenames:
            if await _exists(session, filename):
                return market, filename
        return market, None

    results = await asyncio.gather(
        *(probe(m, f) for m, f in MARKET_CANDIDATES.items())
    )
    found = {market: filename for market, filename in results if filename}

    if found:
        log.info("markets: доступны — %s", ", ".join(f"{m} ({f})" for m, f in found.items()))
    else:
        log.warning(
            "markets: ни один файл площадок не открылся — возможно, состав на "
            "prices.csgotrader.app изменился, см. MARKET_CANDIDATES"
        )
    return found


# Кэш скачанных цен: имя файла -> (цены, когда скачали).
#
# TTL равен периоду обновления самого источника — csgotrader пересобирает файлы
# примерно раз в час, и качать их чаще бессмысленно: придут те же числа. Кэш
# живёт в памяти процесса, после рестарта скачается заново.
_cache: dict[str, tuple[dict[str, float], float]] = {}
CACHE_TTL_SECONDS = 60 * 60

_discovered: dict[str, str] | None = None


async def load_markets(
    session: aiohttp.ClientSession, force_refresh: bool = False
) -> dict[str, dict[str, float]]:
    """
    Цены всех доступных площадок: имя площадки -> {предмет: цена}.

    Список площадок определяется один раз за процесс (он меняется редко), сами
    файлы качаются параллельно и кладутся в кэш на час.
    """
    global _discovered
    if _discovered is None:
        _discovered = await discover_markets(session)
    if not _discovered:
        return {}

    now = time.time()

    async def load_one(market: str, filename: str):
        cached = _cache.get(filename)
        if not force_refresh and cached and (now - cached[1]) < CACHE_TTL_SECONDS:
            return market, cached[0]
        prices = await fetch_market(session, filename)
        if prices:
            _cache[filename] = (prices, now)
        elif cached:
            # Скачать не вышло — лучше отдать вчерашние числа, чем ничего.
            return market, cached[0]
        return market, prices

    results = await asyncio.gather(
        *(load_one(m, f) for m, f in _discovered.items())
    )
    return {market: prices for market, prices in results if prices}


class MarketOffer:
    """Находка: предмет дешевле на площадке, чем в Steam."""

    __slots__ = ("market", "market_hash_name", "market_price", "steam_price", "discount_pct")


    def __init__(self, market: str, name: str, market_price: float, steam_price: float):
        self.market = market
        self.market_hash_name = name
        self.market_price = market_price
        self.steam_price = steam_price
        self.discount_pct = (steam_price - market_price) / steam_price * 100

    def net_after_fee(self, fee_multiplier: float) -> float:
        """Сколько останется при перепродаже в Steam за вычетом комиссии."""
        return self.steam_price * fee_multiplier - self.market_price

    @property
    def steam_url(self) -> str:
        """
        Страница предмета на Steam Market. quote с safe="" обязателен: в именах
        есть и пробелы, и '|', и скобки, а незакодированный '|' Telegram в
        ссылку не превращает.
        """
        return (
            "https://steamcommunity.com/market/listings/730/"
            + quote(self.market_hash_name, safe="")
        )


def compare(
    steam_prices: dict[str, float],
    market_prices: dict[str, float],
    market: str,
    *,
    min_discount_pct: float,
    min_price: float | None = None,
    max_price: float | None = None,
) -> list[MarketOffer]:
    """
    Сравнить цены площадки со Steam по ВСЕМУ пересечению каталогов.

    Здесь нет ни выборки, ни потолка на число проверенных предметов: оба
    словаря уже в памяти, сравнение локальное и стоит миллисекунды. В этом и
    смысл источника — не надо решать, какую тысячу предметов посмотреть.
    """
    offers: list[MarketOffer] = []
    for name, market_price in market_prices.items():
        steam_price = steam_prices.get(name)
        if not steam_price or steam_price <= 0:
            continue
        if min_price is not None and market_price < min_price:
            continue
        if max_price is not None and market_price > max_price:
            continue
        offer = MarketOffer(market, name, market_price, steam_price)
        if offer.discount_pct >= min_discount_pct:
            offers.append(offer)

    offers.sort(key=lambda o: o.discount_pct, reverse=True)
    log.info(
        "markets: %s — общих со Steam предметов %d, прошло порог %.0f%%: %d",
        market,
        sum(1 for n in market_prices if n in steam_prices),
        min_discount_pct, len(offers),
    )
    return offers
