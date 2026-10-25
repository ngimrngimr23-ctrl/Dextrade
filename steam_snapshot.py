"""
Снимок цен Steam: самый дешёвый лот по каждому скину на момент сбора.

ОТКУДА. Steam отдаёт рынок CS2 поиском (market/search/render) по десять
предметов за запрос — скины целиком это около 1 600 запросов и полтора часа
аккуратного темпа. Боту это не по карману: те же адреса Render ходят в Steam
за листингами и ценами, и полуторачасовой обход сжёг бы им лимит на весь
день. Поэтому снимок собирается снаружи и приходит файлом: человек пересылает
боту cs2_skins_steam_prices_history.csv (или .json того же сбора).

ЗАЧЕМ. До снимка отбор кандидатов в /orders шёл по средним ценам продаж из
прайс-листа csgotrader. Замер 2026-10-24 по 15 748 скинам: самый дешёвый лот
в Steam в среднем 0.77 от средней за сутки, а у части предметов средняя
задрана одной продажей с наклейками в два с половиной раза (M4A4 | Tornado
(MW): лот $21.28 при «средней» $55.58). По таким средним бот выбирал
кандидатов, которых живая проверка потом отбрасывала, и тратил на них
лимит Steam. Снимок даёт отбору настоящую цену рынка.

ЧЕМ СНИМОК НЕ ЯВЛЯЕТСЯ — живой ценой. Через сутки часть цен уже сдвинется,
поэтому ордер по-прежнему ставится только по цене Steam не старше получаса
(bot.ORDERS_STEAM_MAX_AGE), а снимок идёт на отбор, на первичную оценку и на
проверку «сходится ли история с ценой». Старше MAX_AGE_HOURS снимок не
используется вовсе, и бот прямо об этом говорит.

Цены в снимке — то же число, что priceoverview.lowest_price и первый лот
/render/: сколько платит покупатель, с комиссией. Сверено 2026-10-24 с
живыми ценами из логов бота: совпадение до цента у 39 предметов.

ОБЪЁМ ПРОДАЖ (необязательные колонки «продано_за_сутки» и «объём_снят_utc»).
С 2026-10-25 Steam отвечает 429 на priceoverview и страницу предмета с
адресов дата-центров на ПЕРВОМ же запросе — и с Render, и с машин GitHub
Actions (проверено: 12 из 12). Без объёма /orders отказывает всем. Снаружи
priceoverview отвечает, поэтому продажи за сутки по кандидатам собираются там
же, где снимок, и приходят в том же файле. У каждой строки своё время замера:
объём снят позже цен. Старше VOLUME_MAX_AGE_HOURS он не используется.

МЕДИАНА СДЕЛОК ЗА МЕСЯЦ (необязательная колонка «медиана_30д_$»). Нижний лот
бывает пиком: 2026-10-25 бот считал FAMAS | Bad Trip (Well-Worn) от $55.32, а
медиана 778 сделок за 30 дней была $51.39. Медиану снимают там же и тогда же,
что и объём, — из истории продаж на странице предмета, — поэтому время замера
у них общее («объём_снят_utc») и срок годности тот же. Цена с комиссией, как и
всё в снимке: столько платил покупатель.
"""

from __future__ import annotations

import calendar
import csv
import io
import json
import logging
import time
from typing import NamedTuple

import envcfg
from storage import get_steam_snapshot, get_steam_snapshot_meta, save_steam_snapshot

log = logging.getLogger("steam_bot.snapshot")

# Старше этого снимок не используется: цены за трое суток уходят настолько,
# что отбор по ним хуже, чем по прайс-листу, обновляемому каждый час.
MAX_AGE_HOURS = envcfg.env_float("STEAM_SNAPSHOT_MAX_AGE_H", 72)

# Меньше — это не снимок рынка, а обрывок (или чужой файл).
MIN_ITEMS = 100

# Сколько верить продажам за сутки из файла. Это не цена: число сделок за день
# у предмета меняется медленно, а заменить его сейчас нечем (см. докстринг).
VOLUME_MAX_AGE_HOURS = envcfg.env_float("STEAM_SNAPSHOT_VOLUME_MAX_AGE_H", 48)

# Колонки CSV, которые мы понимаем: наш формат (merge_history.py) и
# английский формат самого сборщика.
_NAME_COLUMNS = ("market_hash_name",)
_PRICE_COLUMNS = ("цена_steam_сейчас_$", "steam_price_usd")
_LISTINGS_COLUMNS = ("лотов_на_рынке", "listings")
_TIME_COLUMNS = ("снято_utc", "collected_at_utc")
_VOLUME_COLUMNS = ("продано_за_сутки", "steam_sold_24h")
_VOLUME_TIME_COLUMNS = ("объём_снят_utc", "volume_collected_at_utc")
_MEDIAN_COLUMNS = ("медиана_30д_$", "steam_median_30d_usd")


class SnapshotError(ValueError):
    """Файл похож на снимок цен, но прочитать его нельзя — человеку нужен ответ."""


class Snapshot(NamedTuple):
    collected_at: float                          # epoch, UTC
    prices: dict[str, tuple[int, int | None]]    # имя -> (цена в центах, лотов)
    # имя -> (продано за сутки, когда замерено) — только у тех, кого опросили.
    volumes: dict[str, tuple[int, float]] | None = None
    # имя -> (медиана сделок за 30 дней в центах, когда замерено).
    medians: dict[str, tuple[int, float]] | None = None

    def age_hours(self, now: float | None = None) -> float:
        return max(0.0, ((now or time.time()) - self.collected_at) / 3600)

    def price_cents(self, name: str) -> int | None:
        row = self.prices.get(name)
        return row[0] if row else None

    def volume(self, name: str, now: float | None = None) -> tuple[int, float] | None:
        """(продано за сутки, возраст замера в секундах). None — нет или устарел."""
        return _fresh((self.volumes or {}).get(name), now)

    def month_median(self, name: str, now: float | None = None) -> tuple[int, float] | None:
        """(медиана сделок за 30 дней в центах, возраст замера в секундах). None — нет или устарела."""
        return _fresh((self.medians or {}).get(name), now)

    def describe(self) -> str:
        stamp = time.strftime("%d.%m %H:%M", time.gmtime(self.collected_at))
        text = (f"снимок Steam от {stamp} UTC ({self.age_hours():.0f} ч назад), "
                f"{len(self.prices)} скинов")
        if self.volumes:
            newest = max(at for _, at in self.volumes.values())
            text += (f", продажи за сутки у {len(self.volumes)} (замер "
                     f"{time.strftime('%d.%m %H:%M', time.gmtime(newest))} UTC)")
        if self.medians:
            text += f", медиана сделок за месяц у {len(self.medians)}"
        return text


def _fresh(row: tuple[int, float] | None, now: float | None) -> tuple[int, float] | None:
    """Значение из истории продаж и его возраст — если не старше VOLUME_MAX_AGE_HOURS."""
    if row is None:
        return None
    age = max(0.0, (now or time.time()) - row[1])
    if age > VOLUME_MAX_AGE_HOURS * 3600:
        return None
    return row[0], age


def _number(raw: str) -> float | None:
    """«65,00» и «65.00» -> 65.0. Пусто или мусор — None."""
    text = (raw or "").strip().replace(" ", "").replace(" ", "")
    if not text:
        return None
    try:
        return float(text.replace(",", "."))
    except ValueError:
        return None


def _parse_time(raw) -> float | None:
    """«2026-10-24 14:20» (UTC) -> epoch. None — не разобрали."""
    if not isinstance(raw, str):
        return None
    raw = raw.strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return float(calendar.timegm(time.strptime(raw, fmt)))
        except ValueError:
            continue
    return None


def _column(header: list[str], candidates: tuple[str, ...]) -> int | None:
    lowered = [h.strip().lower() for h in header]
    for name in candidates:
        if name in lowered:
            return lowered.index(name)
    return None


def _from_csv(text: str) -> Snapshot | None:
    first = text.lstrip("﻿").splitlines()[0] if text.strip() else ""
    delimiter = ";" if ";" in first else ","
    rows = list(csv.reader(io.StringIO(text.lstrip("﻿")), delimiter=delimiter))
    if not rows:
        return None
    header = rows[0]
    name_i = _column(header, _NAME_COLUMNS)
    price_i = _column(header, _PRICE_COLUMNS)
    if name_i is None or price_i is None:
        return None  # не наш файл — пусть разбирают другие ветки
    listings_i = _column(header, _LISTINGS_COLUMNS)
    time_i = _column(header, _TIME_COLUMNS)
    volume_i = _column(header, _VOLUME_COLUMNS)
    volume_time_i = _column(header, _VOLUME_TIME_COLUMNS)
    median_i = _column(header, _MEDIAN_COLUMNS)

    prices: dict[str, tuple[int, int | None]] = {}
    volumes: dict[str, tuple[int, float]] = {}
    medians: dict[str, tuple[int, float]] = {}
    collected_at = None
    for row in rows[1:]:
        if len(row) <= max(name_i, price_i):
            continue
        name = row[name_i].strip()
        price = _number(row[price_i])
        if not name or not price or price <= 0:
            continue
        listings = None
        if listings_i is not None and len(row) > listings_i:
            value = _number(row[listings_i])
            listings = int(value) if value is not None else None
        prices[name] = (int(round(price * 100)), listings)
        if collected_at is None and time_i is not None and len(row) > time_i:
            collected_at = _parse_time(row[time_i])
        # Объём без своего времени не берём: выдать его за свежий нельзя.
        if (volume_i is not None and volume_time_i is not None
                and len(row) > max(volume_i, volume_time_i)):
            sold = _number(row[volume_i])
            sold_at = _parse_time(row[volume_time_i])
            if sold is not None and sold >= 0 and sold_at is not None:
                volumes[name] = (int(sold), sold_at)
        # Медиана — с тем же временем замера: без него тоже не берём.
        if (median_i is not None and volume_time_i is not None
                and len(row) > max(median_i, volume_time_i)):
            median = _number(row[median_i])
            median_at = _parse_time(row[volume_time_i])
            if median is not None and median > 0 and median_at is not None:
                medians[name] = (int(round(median * 100)), median_at)
    return _checked(prices, collected_at, has_time_column=time_i is not None,
                    volumes=volumes, medians=medians)


def _from_json(text: str) -> Snapshot | None:
    try:
        data = json.loads(text)
    except ValueError:
        return None
    if not isinstance(data, dict) or "items" not in data:
        return None
    items = data["items"]
    prices: dict[str, tuple[int, int | None]] = {}
    volumes: dict[str, tuple[int, float]] = {}
    medians: dict[str, tuple[int, float]] = {}
    if isinstance(items, list):          # формат merge_history.py
        pairs = ((r.get("name"), r) for r in items if isinstance(r, dict))
    elif isinstance(items, dict):        # формат сборщика
        pairs = items.items()
    else:
        return None
    for name, row in pairs:
        if not isinstance(name, str) or not isinstance(row, dict):
            continue
        price = row.get("price")
        if isinstance(price, bool) or not isinstance(price, (int, float)) or price <= 0:
            continue
        listings = row.get("listings")
        prices[name] = (int(round(price * 100)),
                        listings if isinstance(listings, int) else None)
        sold, sold_at = row.get("sold_24h"), _parse_time(row.get("volume_collected_at_utc"))
        if isinstance(sold, int) and not isinstance(sold, bool) and sold >= 0 and sold_at:
            volumes[name] = (sold, sold_at)
        median = row.get("median_30d_usd")
        if (isinstance(median, (int, float)) and not isinstance(median, bool)
                and median > 0 and sold_at):
            medians[name] = (int(round(median * 100)), sold_at)
    if not prices:
        return None
    return _checked(prices, _parse_time(data.get("collected_at_utc")), has_time_column=True,
                    volumes=volumes, medians=medians)


def _checked(prices, collected_at, *, has_time_column: bool, volumes=None,
             medians=None) -> Snapshot:
    if len(prices) < MIN_ITEMS:
        raise SnapshotError(
            f"в файле всего {len(prices)} цен — это не снимок рынка. Пришли полный "
            "cs2_skins_steam_prices_history.csv"
        )
    if collected_at is None:
        # Без времени сбора цены неотличимы от свежих — а через сутки они
        # уже врут. Принять такой файл значит выдать вчерашние цены за
        # сегодняшние, поэтому отказ, а не «посчитаю от момента загрузки».
        raise SnapshotError(
            "в файле нет времени сбора" + ("" if has_time_column else
                                           " (колонки «снято_utc»)")
            + " — без него не отличить свежие цены от вчерашних. Пришли файл "
            "из последней выгрузки"
        )
    if collected_at > time.time() + 3600:
        raise SnapshotError("время сбора в файле из будущего — файл испорчен")
    now = time.time()
    volumes = {name: row for name, row in (volumes or {}).items()
               if name in prices and row[1] <= now + 3600}
    medians = {name: row for name, row in (medians or {}).items()
               if name in prices and row[1] <= now + 3600}
    return Snapshot(collected_at, prices, volumes or None, medians or None)


def parse(text: str) -> Snapshot | None:
    """
    Снимок из текста файла. None — это не снимок (разбирать дальше как обычно).

    Бросает SnapshotError, если файл явно наш, но не годится: обрывок, нет
    времени сбора. Отвечать на такой файл «это список предметов» нельзя —
    в строках снимка есть «|», и разбор списка имён принял бы их за
    15 тысяч предметов для вотчлиста.
    """
    stripped = text.lstrip("﻿").lstrip()
    if stripped.startswith("{"):
        return _from_json(stripped)
    return _from_csv(text)


async def save(snapshot: Snapshot) -> None:
    """Сохранить снимок в хранилище (StorageUnavailable — наружу)."""
    global _cached, _cached_stamp
    volumes = snapshot.volumes or {}
    medians = snapshot.medians or {}

    def stored(name: str, row: tuple) -> list:
        # [цена, лотов] или [цена, лотов, продано за сутки, когда замерено,
        # медиана за месяц] — у объёма и медианы время замера общее.
        sold, median = volumes.get(name), medians.get(name)
        if sold is None and median is None:
            return list(row)
        return list(row) + [sold[0] if sold else None, (sold or median)[1],
                            median[0] if median else None]

    await save_steam_snapshot(
        {"collected_at": snapshot.collected_at, "count": len(snapshot.prices),
         "uploaded_at": time.time()},
        {name: stored(name, row) for name, row in snapshot.prices.items()},
    )
    _cached = snapshot
    _cached_stamp = None   # своя загрузка: метку подтянет первый же latest()
    log.info("снимок Steam: сохранено %d цен (%s)", len(snapshot.prices), snapshot.describe())


_cached: Snapshot | None = None
# Метка, с которой снят _cached. Сверяем и время загрузки: файл с теми же
# ценами, но с дописанным объёмом — это новый снимок.
_cached_stamp: tuple | None = None


def _stamp(meta: dict) -> tuple:
    return meta.get("collected_at"), meta.get("uploaded_at")


async def latest() -> Snapshot | None:
    """
    Последний загруженный снимок любой давности. None — его нет или хранилище
    не ответило (это не повод ронять /orders: отбор вернётся к прайс-листу).

    Целиком снимок читается один раз на загрузку: в памяти он лежит, пока
    метка в хранилище не сменится, — проверка метки стоит одну команду.
    """
    global _cached, _cached_stamp
    try:
        meta = await get_steam_snapshot_meta()
    except Exception:  # noqa: BLE001 — см. докстринг
        log.warning("снимок Steam: метка не прочитана", exc_info=True)
        return _cached
    if not meta:
        return None
    if _cached is not None and (
        _cached_stamp == _stamp(meta)
        or (_cached_stamp is None and _cached.collected_at == meta.get("collected_at"))
    ):
        _cached_stamp = _stamp(meta)
        return _cached
    try:
        loaded = await get_steam_snapshot()
    except Exception:  # noqa: BLE001
        log.warning("снимок Steam: не прочитан", exc_info=True)
        return _cached
    if not loaded:
        return None
    meta, raw = loaded
    _cached = Snapshot(
        float(meta["collected_at"]),
        {name: (int(row[0]), row[1] if len(row) > 1 else None) for name, row in raw.items()},
        {name: (int(row[2]), float(row[3])) for name, row in raw.items()
         if len(row) >= 4 and row[2] is not None and row[3] is not None} or None,
        {name: (int(row[4]), float(row[3])) for name, row in raw.items()
         if len(row) >= 5 and row[4] is not None and row[3] is not None} or None,
    )
    _cached_stamp = _stamp(meta)
    return _cached


async def current() -> tuple[Snapshot | None, str | None]:
    """
    Снимок, годный для отбора, и пояснение, если его нет.

    (снимок, None) — используем; (None, причина) — снимка нет или он старше
    MAX_AGE_HOURS, и причину стоит показать человеку.
    """
    snapshot = await latest()
    if snapshot is None:
        return None, None
    if snapshot.age_hours() > MAX_AGE_HOURS:
        return None, (
            f"{snapshot.describe()} — старше {MAX_AGE_HOURS:.0f} ч, не использую. "
            "Пришли свежий файл цен"
        )
    return snapshot, None
