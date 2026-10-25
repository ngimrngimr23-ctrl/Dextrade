"""
Страница предмета Steam: нижний лот и история продаж — одним запросом.

Зачем. Объём продаж — обязательное условие ордера (buy_orders.plan отказывает
с UNKNOWN_VOLUME, когда его нет), а до сих пор он брался только из
priceoverview. Этот эндпоинт Steam режет жёстче всех: 2026-10-24 он стоял на
кулдауне 148 минут, и /orders отказал всем 30 финалистам — «объём продаж
неизвестен», хотя свежие цены у них были (их отдавал /render/, но без объёма).

Ручка истории (/market/pricehistory) требует логина. Но та же история встроена
в саму страницу предмета: график «Median Sale Prices» виден и без входа, и его
данные лежат в HTML переменной line1:

    var line1=[["Oct 24 2026 06: +0",10.845,"1"], ...];

Это [час, медиана сделок за этот час, сколько продано]. За последний месяц
точки почасовые, раньше — посуточные. Там же лежат первые лоты витрины
(g_rgListingInfo) с ценой в валюте зрителя. Проверено 2026-10-24 анонимным
запросом: 3422 точки с 2017 года, валюта «$», цена нижнего лота совпала с
/render/ до цента ($11.13).

Что отсюда берётся:
  • нижний лот — то же число, что priceoverview.lowest_price;
  • продано за сутки и медиана за сутки — то же, что priceoverview.volume и
    median_price, то есть страница полностью подменяет priceoverview, когда
    тот на кулдауне (см. pricing.get_steam_market_price_retrying);
  • продано за 7 дней — настоящий недельный объём, а не суточный ×7;
  • медианы по неделям месяца — стабильность цены по самим сделкам Steam, а
    не по средним прайс-листа (в средних одна продажа с дорогими наклейками
    задирает цену вдвое).

С КУКИ АККАУНТА, если они заданы (STEAM_LOGIN_SECURE). Сначала страница
ходила анонимно — залогиненному Steam показывает цены в валюте кошелька, а нам
нужны доллары. Но анонимно она так и не ответила ни разу: 2026-10-24 и
2026-10-25 все копии бота получили 429 на ПЕРВОМ же запросе страницы (и
priceoverview тоже), а /render/ с куки аккаунта в те же минуты отвечал 200 на
каждый из тридцати запросов подряд. Анонимный лимит Steam считает по адресу, а
адрес у Render общий; у запросов с логином лимит свой (см. steam_client). В
итоге /orders отказал всем тридцати финалистам — «объём продаж неизвестен».

Валюта при этом мешает меньше, чем казалось: объём и относительный разброс от
неё не зависят, а цену в долларах даёт /render/ (currency=1). Если кошелёк не
в долларах, страница отдаёт только объём и стабильность (history_usd=False,
lowest_usd=None), а цену берут из /render/. Нужна и bMarketOptOut=1 — без неё
приходит новая площадка, где line1 нет (steam_cookie_header её добавляет).

Своя область кулдауна: "history_auth" с куки и "history" без них — лимиты у
этих запросов разные, и 429 анонимной страницы не повод не спросить с логином.
429 страницы остальных областей не придерживает (останавливать вотчлист из-за
справочного запроса нельзя), а вот бан на листингах придерживает и её — это
тот же адрес и тот же сайт.
"""

from __future__ import annotations

import asyncio
import calendar
import json
import logging
import re
import time
from dataclasses import dataclass

import aiohttp

import envcfg
from http_session import get_session
import steam_client
from steam_client import (
    _UA,
    _UA_HINTS,
    _build_request,
    item_page_url,
    note_steam_429,
    note_steam_ok,
    steam_cookie_header,
    steam_cooldown_remaining,
    throttle_steam_request,
)

log = logging.getLogger("steam_bot.steam_history")

SCOPE = "history"
AUTH_SCOPE = "history_auth"
TIMEOUT_SECONDS = envcfg.env_float("STEAM_HISTORY_TIMEOUT", 20.0)
# Сколько держать страницу в памяти. История за месяц за три часа почти не
# меняется; цене нижнего лота столько доверять нельзя, поэтому каждый
# вызывающий сам говорит, насколько старая страница ему годится (max_age).
CACHE_SECONDS = envcfg.env_int("STEAM_HISTORY_TTL_MIN", 180) * 60
CACHE_MAX_ITEMS = 2000

DAY = 86400.0
WEEK = 7 * DAY
# Сколько недель сравнивать для стабильности «за месяц».
WEEKS = 4
# Точки старше этого не нужны ни одному расчёту — не держим их в памяти.
KEEP_DAYS = 35

# Steam пишет в line1 английские сокращения месяцев независимо от языка
# страницы. strptime("%b") зависел бы от локали процесса — разбираем сами.
_MONTHS = {
    name: number for number, name in enumerate(
        ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
         "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), start=1,
    )
}
_LINE1 = re.compile(r"var line1=(\[.*?\]);")
_LISTINGS = re.compile(r"var g_rgListingInfo = (\{.*?\});", re.S)
_PREFIX = re.compile(r'strFormatPrefix\s*=\s*"([^"]*)"')
_SUFFIX = re.compile(r'strFormatSuffix\s*=\s*"([^"]*)"')
# Код валюты в g_rgListingInfo — 2000 + номер валюты Steam; доллар — 1.
_USD_CURRENCY_ID = 2001


@dataclass(frozen=True)
class ItemPage:
    """Что видно на странице предмета. Цены в долларах."""

    fetched_at: float
    # Нижний лот витрины (с комиссией — столько платит покупатель). None —
    # лотов нет или цена не в долларах.
    lowest_usd: float | None
    # (момент UTC, медиана сделок за точку, продано) — только за KEEP_DAYS.
    points: tuple[tuple[float, float, int], ...]
    # На странице был график продаж. False — истории нет, и объём НЕИЗВЕСТЕН,
    # а не ноль.
    has_history: bool
    # Цены графика в долларах. Объём и относительный разброс от валюты не
    # зависят, а вот сравнивать медиану с ценой в долларах можно только так.
    history_usd: bool

    def _window(self, start_ago: float, end_ago: float = 0.0) -> list[tuple[float, int]]:
        lo = self.fetched_at - start_ago
        hi = self.fetched_at - end_ago
        return [(price, sold) for ts, price, sold in self.points
                if lo <= ts < hi and sold > 0 and price > 0]

    def sold(self, days: float) -> int | None:
        """Продано за последние days суток. None — истории на странице нет."""
        if not self.has_history:
            return None
        return sum(sold for _, sold in self._window(days * DAY))

    def median(self, days: float) -> float | None:
        """Медиана сделок за последние days суток (каждая продажа — голос)."""
        if not self.has_history:
            return None
        return _weighted_median(self._window(days * DAY))

    def weekly_medians(self, weeks: int = WEEKS) -> list[float | None]:
        """Медианы сделок по неделям, от последней к ранним. None — неделя без продаж."""
        if not self.has_history:
            return []
        return [
            _weighted_median(self._window((i + 1) * WEEK, i * WEEK))
            for i in range(weeks)
        ]

    def month_spread_pct(self) -> float | None:
        """
        Насколько гуляла цена за месяц: (max - min) / min медиан по неделям, в %.

        None — истории нет или была неделя без единой продажи: сравнивать ту
        неделю не с чем, и стабильность не подтверждена.
        """
        medians = self.weekly_medians()
        if not medians or any(m is None for m in medians):
            return None
        return (max(medians) - min(medians)) / min(medians) * 100


def _weighted_median(pairs: list[tuple[float, int]]) -> float | None:
    """Медиана, где точка с n продажами — это n сделок по её цене."""
    total = sum(sold for _, sold in pairs)
    if total <= 0:
        return None
    half = total / 2
    seen = 0
    for price, sold in sorted(pairs):
        seen += sold
        if seen >= half:
            return price
    return None


def _timestamp(label: str) -> float:
    """«Oct 24 2026 06: +0» → момент UTC."""
    month, day, year, hour = label.split()[:4]
    return float(calendar.timegm(
        (int(year), _MONTHS[month], int(day), int(hour.rstrip(":")), 0, 0, 0, 0, 0)
    ))


def _lowest_usd(html: str) -> float | None:
    match = _LISTINGS.search(html)
    if not match:
        return None
    try:
        listings = json.loads(match.group(1))
    except ValueError:
        return None
    prices = []
    for row in listings.values() if isinstance(listings, dict) else []:
        if not isinstance(row, dict) or row.get("converted_currencyid") != _USD_CURRENCY_ID:
            continue
        price = row.get("converted_price_per_unit", row.get("converted_price"))
        fee = row.get("converted_fee_per_unit", row.get("converted_fee"))
        if isinstance(price, int) and isinstance(fee, int) and price > 0:
            prices.append((price + fee) / 100)
    return min(prices) if prices else None


def parse(html: str, fetched_at: float | None = None) -> ItemPage | None:
    """
    Страница предмета → ItemPage. None — это не страница предмета (ни лотов,
    ни графика) или график не разобрался.

    График, в котором есть точки, но ни одна не разобралась, — это None, а не
    «продаж нет»: ноль продаж отказал бы предмету по ликвидности, а на деле мы
    просто не поняли формат.
    """
    now = time.time() if fetched_at is None else fetched_at
    lowest = _lowest_usd(html)

    line1 = _LINE1.search(html)
    points: list[tuple[float, float, int]] = []
    if line1:
        try:
            raw = json.loads(line1.group(1))
        except ValueError:
            log.warning("страница предмета: график продаж не разобрался как JSON")
            return None
        cutoff = now - KEEP_DAYS * DAY
        parsed = 0
        for row in raw if isinstance(raw, list) else []:
            try:
                ts = _timestamp(str(row[0]))
                price = float(row[1])
                sold = int(str(row[2]).replace(",", ""))
            except (TypeError, ValueError, IndexError, KeyError):
                continue
            parsed += 1
            if ts >= cutoff:
                points.append((ts, price, sold))
        if raw and not parsed:
            log.warning(
                "страница предмета: в графике %d точек, и ни одна не разобралась "
                "(образец %r)", len(raw), raw[0] if raw else None,
            )
            return None
    elif lowest is None:
        return None

    prefix = _PREFIX.search(html)
    suffix = _SUFFIX.search(html)
    history_usd = (
        bool(prefix) and prefix.group(1).strip() == "$"
        and (suffix is None or suffix.group(1).strip() in ("", "USD"))
    )
    return ItemPage(
        fetched_at=now,
        lowest_usd=lowest,
        points=tuple(points),
        has_history=line1 is not None,
        history_usd=history_usd,
    )


def logged_in() -> bool:
    """Ходим ли за страницей с куки аккаунта (см. докстринг модуля)."""
    return bool(steam_client.STEAM_LOGIN_SECURE)


# Первую удачную страницу и чужую валюту пишем в лог один раз за процесс:
# по ним видно, ответил ли Steam с логином и что из страницы берётся, а
# тридцать одинаковых строк на прогон ничего к этому не добавят.
_ok_logged: set[str] = set()   # области, чей первый успех уже в логе
_currency_logged = False


def active_scope() -> str:
    """Область кулдауна, в которой сейчас ходит страница."""
    return AUTH_SCOPE if logged_in() else SCOPE


_cache: dict[str, ItemPage] = {}


def cached(market_hash_name: str, max_age: float) -> ItemPage | None:
    """Страница из памяти, если она не старше max_age секунд."""
    page = _cache.get(market_hash_name)
    if page is not None and time.time() - page.fetched_at <= max_age:
        return page
    return None


def _remember(market_hash_name: str, page: ItemPage) -> None:
    _cache[market_hash_name] = page
    if len(_cache) > CACHE_MAX_ITEMS:
        horizon = time.time() - CACHE_SECONDS
        for name in [n for n, p in _cache.items() if p.fetched_at < horizon]:
            del _cache[name]
        while len(_cache) > CACHE_MAX_ITEMS:
            del _cache[next(iter(_cache))]


async def fetch(market_hash_name: str, *, max_age: float = CACHE_SECONDS) -> ItemPage | None:
    """
    Страница предмета — из памяти, если она не старше max_age, иначе запросом.

    None — узнать не вышло: область на кулдауне, Steam ответил не 200 или
    страница не та. Причина в логе; вызывающий решает, чем её заменить.
    """
    hit = cached(market_hash_name, max_age)
    if hit is not None:
        return hit
    scope = active_scope()
    if steam_cooldown_remaining(scope) > 0:
        return None

    final_url, params = _build_request(item_page_url(market_hash_name))
    headers = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        # Прямым адресом, как и /render/ с куки (steam_client.fetch_all_listings):
        # через резидентные прокси логин не шлём нигде.
        "Cookie": steam_cookie_header() if logged_in() else "bMarketOptOut=1",
        **_UA_HINTS,
    }
    # Полоса прямого адреса общая с /render/ и priceoverview: к Steam с адреса
    # Render по-прежнему уходит один запрос в MIN_REQUEST_INTERVAL.
    await throttle_steam_request(scope=scope, lane="")
    try:
        async with get_session().get(
            final_url, params=params, headers=headers,
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
        ) as resp:
            if resp.status == 429:
                log.warning(
                    "страница предмета%s: %s — HTTP 429",
                    " (с куки аккаунта)" if logged_in() else "", market_hash_name,
                )
                await note_steam_429(scope=scope, headers=dict(resp.headers))
                return None
            if resp.status != 200:
                log.info("страница предмета: %s — HTTP %s", market_hash_name, resp.status)
                return None
            html = await resp.text()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.info("страница предмета: %s — не получилось (%s)",
                 market_hash_name, type(e).__name__)
        return None

    global _currency_logged
    await note_steam_ok(scope)
    page = parse(html)
    if page is None:
        log.info(
            "страница предмета: %s — ни лотов, ни графика продаж (%d символов)",
            market_hash_name, len(html),
        )
        return None
    if scope not in _ok_logged:
        _ok_logged.add(scope)
        log.info(
            "страница предмета%s отвечает: %s — точек графика за %d дн. %d, "
            "продано за 7 дней %s, нижний лот %s",
            " с куки аккаунта" if logged_in() else "", market_hash_name, KEEP_DAYS,
            len(page.points), page.sold(7) if page.has_history else "?",
            f"${page.lowest_usd:.2f}" if page.lowest_usd is not None else "не в долларах",
        )
    if page.has_history and not page.history_usd and not _currency_logged:
        _currency_logged = True
        log.warning(
            "страница предмета: график не в долларах (валюта кошелька аккаунта) — "
            "беру из него только объём и разброс, цену даёт /render/",
        )
    _remember(market_hash_name, page)
    return page
