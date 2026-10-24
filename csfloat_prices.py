"""
Цены CSFloat по всему рынку — из прайс-листа csgotrader, одним файлом.

Зачем. Поиск лотов CSFloat (/listings) с адреса Render закрыт («disable your
VPN»), а прокси нет. Без него бот не знает, почём предмет лежит на CSFloat:
не видно, исполнится ли заявка (ниже витрины в разы — не исполнится никогда) и
не купит ли она лот сразу. Широкий скан арбитража не работает по той же причине.

csgotrader публикует цены CSFloat тем же статическим файлом, что и цены Steam,
которые бот уже берёт оттуда (prices.csgotrader.app/latest/csfloat.json): по
каждому предмету нижний лот (starting_at) и верхний ордер на покупку
(highest_order). Файл обновляется на их стороне раз в несколько часов.
Сверка 2026-10-24 с живым стаканом совпала до цента: верхний ордер на
StatTrak™ AK-47 | Legion of Anubis (Well-Worn) — $25.70 и там, и там.

Это справка, а не живой ответ: перед постановкой бот всё равно читает стакан
сам (csfloat_client.order_book_by_name). Отсюда берётся только то, чего иначе
не узнать совсем: цена витрины и первичный отбор кандидатов по всему рынку.
"""

from __future__ import annotations

import email.utils
import gzip
import json
import logging
import time
from typing import NamedTuple

import aiohttp

import envcfg
from http_session import get_session

log = logging.getLogger("steam_bot.csfloat_prices")

URL = "https://prices.csgotrader.app/latest/csfloat.json"
# Как часто перекачивать. Файл на их стороне обновляется раз в несколько
# часов, и чаще получаса спрашивать незачем.
REFRESH_SECONDS = envcfg.env_int("CSFLOAT_PRICES_REFRESH_MIN", 30) * 60
TIMEOUT_SECONDS = 30


class CSFloatPrice(NamedTuple):
    """Цены CSFloat по предмету, в центах. None — лотов (ордеров) нет."""

    low_cents: int | None   # нижний лот: столько стоит купить сразу
    top_cents: int | None   # верхний ордер на покупку: кого перебивать


class PriceList(NamedTuple):
    prices: dict[str, CSFloatPrice]
    # Когда csgotrader обновил файл (Last-Modified), а если сервер не сказал —
    # когда мы его скачали.
    updated_at: float

    def age_minutes(self) -> float:
        return max(0.0, (time.time() - self.updated_at) / 60)

    def describe(self) -> str:
        return (
            f"прайс-лист csgotrader от {time.strftime('%d.%m %H:%M', time.gmtime(self.updated_at))} "
            f"UTC ({self.age_minutes():.0f} мин назад), {len(self.prices)} предметов"
        )


_cached: PriceList | None = None
_fetched_at = 0.0


def _cents(value) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        return None
    return int(round(value * 100))


def parse(raw: bytes) -> dict[str, CSFloatPrice]:
    """Файл csgotrader → {имя: CSFloatPrice}. Пустой словарь — файл не тот."""
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    data = json.loads(raw)
    out: dict[str, CSFloatPrice] = {}
    if not isinstance(data, dict):
        return out
    for name, row in data.items():
        if not isinstance(row, dict):
            continue
        low = _cents((row.get("starting_at") or {}).get("price")) or _cents(row.get("price"))
        top = _cents((row.get("highest_order") or {}).get("price"))
        if low or top:
            out[name] = CSFloatPrice(low, top)
    return out


async def load() -> PriceList | None:
    """
    Прайс-лист CSFloat — из памяти, если скачан меньше REFRESH_SECONDS назад.

    Скачать не вышло — отдаём прежний, каким бы старым он ни был: возраст
    виден в describe(), и решать, годится ли он, будет вызывающий. None —
    не скачивали ни разу и сейчас не вышло.
    """
    global _cached, _fetched_at
    if _cached is not None and time.time() - _fetched_at < REFRESH_SECONDS:
        return _cached
    try:
        async with get_session().get(
            URL, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
        ) as resp:
            if resp.status != 200:
                log.warning("цены CSFloat: csgotrader ответил HTTP %s", resp.status)
                return _cached
            raw = await resp.read()
            modified = resp.headers.get("Last-Modified")
    except Exception as e:  # noqa: BLE001 — справка, без неё бот работает как раньше
        log.warning("цены CSFloat: не скачались (%s)", type(e).__name__)
        return _cached
    try:
        prices = parse(raw)
    except ValueError as e:
        log.warning("цены CSFloat: файл не разобрался (%s)", e)
        return _cached
    if not prices:
        log.warning("цены CSFloat: в файле нет ни одной цены")
        return _cached
    updated_at = time.time()
    if modified:
        try:
            updated_at = email.utils.parsedate_to_datetime(modified).timestamp()
        except (TypeError, ValueError):
            pass
    _cached = PriceList(prices, updated_at)
    _fetched_at = time.time()
    log.info("цены CSFloat: %s", _cached.describe())
    return _cached
