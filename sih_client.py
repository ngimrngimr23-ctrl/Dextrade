"""
Цены с SIH (api.sih.market) — источник для /markets вместо csgotrader.

Чем он лучше того, что было. Раньше сравнение собиралось из ДВУХ источников:
цены площадок из прайс-листов csgotrader, цены Steam оттуда же отдельным
файлом, и сшивались они у нас локально. На проде это дало расхождение на 97%
предметов с устойчивым отношением 2.3-2.7x — такая ровная кратность означает
не разошедшийся рынок, а разные единицы или валюту, то есть системную ошибку
сшивки. Найти её, имея на руках только два спорящих числа, не выходило.

Здесь сшивать нечего. Один ответ get-items содержит и цену покупки, и цену
Steam по одному предмету, посчитанные одной стороной:

    "AWP | Asiimov (Field-Tested)": {
        "price": 93.14,          за сколько купить на площадке
        "steam": 108.20,         цена Steam по мнению того же источника
        "count": 3,              сколько лотов есть
        "market": "waxpeer",     на какой именно площадке лот
        "sell": 88.00            за сколько площадка выкупит
    }

Плюс два поля, которых у csgotrader не было вовсе: count — ликвидность на
стороне ПОКУПКИ (скидка на единственном экземпляре и скидка на трёх десятках
лотов — разные вещи, и раньше мы их не различали), и market — название
площадки, без которого сигнал «дешевле на N%» некуда отнести.

Чем платим: нужен ключ (SIH_API_KEY) и, в отличие от статики на CDN, источник
может отказать. Поэтому кэш держит последний удачный ответ и отдаёт его, если
свежий запрос не удался, — вчерашние числа полезнее пустого экрана.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import NamedTuple

import aiohttp

log = logging.getLogger("steam_bot.sih")

BASE_URL = os.environ.get("SIH_BASE_URL", "https://api.sih.market/api/v1").rstrip("/")
SIH_API_KEY = os.environ.get("SIH_API_KEY", "").strip()

# 730 — CS2. Документация знает ещё 440 (TF2) и 252490 (Rust), но бот про них
# ничего не умеет, так что параметр есть, а значение по умолчанию одно.
APP_ID_CS2 = 730

# Каталог обновляется не мгновенно, а прогон /markets стоит одного запроса —
# час это компромисс между свежестью и нежеланием долбить чужой сервис.
CACHE_TTL_SECONDS = 60 * 60

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=120)


class SihError(Exception):
    """Ответ получен, но пользоваться им нельзя."""


class SihRateLimited(SihError):
    """429. Отдельным типом, потому что лечится ожиданием, а не правкой."""

    def __init__(self, retry_after: float = 10.0):
        super().__init__(f"SIH: слишком часто, повтор через {retry_after:.0f} с")
        self.retry_after = retry_after


class SihItem(NamedTuple):
    """Одна запись каталога. Любое поле, кроме price, может отсутствовать."""

    market_hash_name: str
    price: float
    steam: float | None
    count: int
    market: str | None
    sell: float | None


def enabled() -> bool:
    return bool(SIH_API_KEY)


def key_fingerprint() -> str:
    """Хвост ключа для логов — чтобы отличать «ключ не тот» от «ключа нет»."""
    if not SIH_API_KEY:
        return "не задан"
    return f"…{SIH_API_KEY[-4:]} ({len(SIH_API_KEY)} симв.)"


def _to_float(value) -> float | None:
    """
    Цена из значения любого встречающегося вида.

    Строки разбираем наравне с числами намеренно: примеры в документации сняты
    с тестового проекта, и полагаться на то, что в проде везде придут именно
    числа, нельзя. Молча возвращаем None только для пустого и нечислового —
    сколько записей не разобралось, считает вызывающий.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    if isinstance(value, str):
        try:
            price = float(value.replace("$", "").replace(",", ".").strip())
        except ValueError:
            return None
        return price if price > 0 else None
    return None


def _to_int(value) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return int(value) if value > 0 else 0
    if isinstance(value, str):
        try:
            return max(0, int(float(value.strip())))
        except ValueError:
            return 0
    return 0


def parse_items(payload: dict) -> tuple[dict[str, SihItem], int]:
    """
    Разобрать ответ get-items. Возвращает (записи, сколько пропущено).

    Пропущенные считаем и возвращаем, а не глотаем: если формат поменяется,
    это будет видно числом в логе, а не молчаливым «ничего не нашлось».
    """
    raw = payload.get("items")
    if not isinstance(raw, dict):
        raise SihError("в ответе SIH нет объекта items — формат изменился?")

    items: dict[str, SihItem] = {}
    skipped = 0
    for name, record in raw.items():
        if not isinstance(record, dict):
            skipped += 1
            continue
        price = _to_float(record.get("price"))
        if price is None:
            skipped += 1
            continue
        market = record.get("market")
        items[name] = SihItem(
            market_hash_name=name,
            price=price,
            steam=_to_float(record.get("steam")),
            count=_to_int(record.get("count")),
            market=str(market) if market else None,
            sell=_to_float(record.get("sell")),
        )
    return items, skipped


async def _get(session: aiohttp.ClientSession, path: str, params: dict) -> dict:
    if not SIH_API_KEY:
        raise SihError("не задан SIH_API_KEY")

    url = f"{BASE_URL}/{path.lstrip('/')}"
    async with session.get(
        url,
        params=params,
        headers={"apikey": SIH_API_KEY},
        timeout=REQUEST_TIMEOUT,
    ) as resp:
        if resp.status == 429:
            try:
                body = await resp.json(content_type=None)
            except Exception:
                body = {}
            raise SihRateLimited(float(body.get("retryAfter") or 10))
        if resp.status in (401, 403):
            # Текст ошибки от SIH здесь важнее кода: он различает «ключ
            # отключён тобой» и «ключ отключён администратором», а это разные
            # действия со стороны владельца.
            try:
                body = await resp.json(content_type=None)
                detail = body.get("error") or ""
            except Exception:
                detail = ""
            raise SihError(f"SIH отклонил ключ (HTTP {resp.status}): {detail or 'без пояснения'}")
        if resp.status != 200:
            raise SihError(f"SIH ответил HTTP {resp.status}")

        payload = await resp.json(content_type=None)

    if not isinstance(payload, dict):
        raise SihError("SIH вернул не JSON-объект")
    if payload.get("success") is False:
        raise SihError(f"SIH: {payload.get('error') or 'запрос отклонён без пояснения'}")
    return payload


# Кэш каталога: app_id -> (записи, момент загрузки).
_cache: dict[int, tuple[dict[str, SihItem], float]] = {}
_lock = asyncio.Lock()


async def fetch_items(
    session: aiohttp.ClientSession,
    *,
    app_id: int = APP_ID_CS2,
    force_refresh: bool = False,
) -> dict[str, SihItem]:
    """
    Весь каталог одним запросом.

    Под замком, потому что прогон /markets и автоскан могут прийти
    одновременно, а тянуть один и тот же большой ответ дважды незачем.
    """
    async with _lock:
        cached = _cache.get(app_id)
        if not force_refresh and cached and (time.time() - cached[1]) < CACHE_TTL_SECONDS:
            return cached[0]

        try:
            payload = await _get(session, "get-items", {"appId": app_id})
            items, skipped = parse_items(payload)
        except Exception as e:
            if cached:
                age = (time.time() - cached[1]) / 60
                log.warning(
                    "sih: свежий каталог не получен (%s), отдаю кэш возрастом %.0f мин",
                    e, age,
                )
                return cached[0]
            raise

        if not items:
            raise SihError("SIH вернул пустой каталог")

        _cache[app_id] = (items, time.time())
        log.info(
            "sih: каталог получен — %d предметов, из них с ценой Steam %d, "
            "не разобрано %d, площадок %d",
            len(items),
            sum(1 for i in items.values() if i.steam),
            skipped,
            len({i.market for i in items.values() if i.market}),
        )
        return items


async def fetch_min_price(
    session: aiohttp.ClientSession,
    market_hash_name: str,
    *,
    app_id: int = APP_ID_CS2,
) -> tuple[float, int] | None:
    """
    Минимальная цена по одному предмету: (цена, сколько лотов).

    Нужно для точечной проверки находки перед отправкой — каталог кэшируется
    на час, и за это время конкретный лот вполне могли разобрать.
    """
    payload = await _get(
        session, "get-min-item", {"item": market_hash_name, "appId": app_id}
    )
    raw = payload.get("items")
    if not isinstance(raw, dict):
        return None
    record = raw.get(market_hash_name)
    if not isinstance(record, dict):
        # Ключ мог прийти с другим регистром или единственной записью под
        # другим именем — берём первую, но только если она одна.
        values = [v for v in raw.values() if isinstance(v, dict)]
        if len(values) != 1:
            return None
        record = values[0]
    price = _to_float(record.get("price"))
    if price is None:
        return None
    return price, _to_int(record.get("count"))


def split_by_market(
    items: dict[str, SihItem],
) -> tuple[dict[str, float], dict[str, dict[str, float]], dict[str, int]]:
    """
    Разложить каталог в тот вид, который уже умеет сравнивать market_prices.

    Возвращает (цены Steam, {площадка: {предмет: цена}}, {предмет: лотов}).

    Группировка по площадкам не косметическая: MarketOffer хранит название
    площадки, и без разбивки все находки склеились бы в одну безымянную кучу,
    по которой непонятно, куда идти покупать.
    """
    steam_prices: dict[str, float] = {}
    by_market: dict[str, dict[str, float]] = {}
    counts: dict[str, int] = {}

    for name, item in items.items():
        if item.steam:
            steam_prices[name] = item.steam
        counts[name] = item.count
        by_market.setdefault(item.market or "SIH", {})[name] = item.price

    return steam_prices, by_market, counts
