"""
Собственная история цен: настоящий минимум за период и признак активности.

Зачем понадобилась. В прайс-листе csgotrader по предмету лежат ровно четыре
поля — last_24h, last_7d, last_30d, last_90d, и все четыре это ЦЕНЫ. Ни
минимума, ни числа продаж там нет (проверено на живом ответе 2026-08-29,
не выведено из документации — её у файла нет). У Steam объём есть только за
сутки и только поштучно, то есть на каталог из 32 тысяч предметов
недоступен.

Значит и минимум за месяц, и хоть какую-то меру активности можно получить
единственным способом — накопить самим. Раз в сутки снимаем срез цен и
сводим его с уже накопленным.

Что храним по предмету (компактным списком, а не словарём — экономия на 30
тысячах записей выходит кратной):

    [минимум, максимум, дней наблюдений, дней с изменением, последняя, начало]

«Дней с изменением» — это не число продаж, и выдавать одно за другое нельзя.
Это признак: цена, меняющаяся 25 дней из 30, принадлежит живому предмету, а
неподвижная месяц — неликвиду, у которого «средняя за месяц» ничего не
значит. Для отсева второго этого достаточно.

Окно наблюдения сбрасывается каждые HISTORY_WINDOW_DAYS: иначе минимум,
взятый однажды на обвале полгода назад, остался бы «минимумом» навсегда.
"""

from __future__ import annotations

import logging
import time
from typing import NamedTuple

log = logging.getLogger("steam_bot.history")

HISTORY_WINDOW_DAYS = 30
_WINDOW_SECONDS = HISTORY_WINDOW_DAYS * 24 * 60 * 60

# Ниже этой цены предмет не отслеживаем. Копеечные позиции — это две трети
# каталога и почти весь объём хранилища, а находкой ни одна из них не станет:
# процент там огромен, а деньги никакие.
MIN_TRACKED_PRICE = 1.0

# Порядковые номера полей в записи. Список компактнее словаря почти вдвое, а
# читаемость даёт этот перечень.
_MIN, _MAX, _DAYS, _CHANGED, _LAST, _SINCE = range(6)

# Время последнего среза лежит в самой истории, под ключом, которым не может
# оказаться имя предмета. Отдельное хранилище заводить не за чем, а знать это
# время обязательно: Render передеплоивает по нескольку раз в день, и без
# проверки каждый запуск снимал бы «суточный» срез заново. Дней бы
# накапливалось втрое больше настоящих, и минимум объявлялся бы надёжным
# через три дня вместо недели.
META_KEY = "\x00meta"


class Record(NamedTuple):
    """История одного предмета за окно наблюдения."""

    low: float
    high: float
    days: int          # сколько срезов легло в эту запись
    changed_days: int  # в скольких из них цена отличалась от предыдущей
    last: float
    since: float       # когда окно началось

    @property
    def activity_pct(self) -> float:
        """
        Доля дней, когда цена менялась. НЕ число продаж — признак живости.

        Один день наблюдения ничего не значит: делить не на что, поэтому
        отдаём ноль, а не сто процентов.
        """
        return 100 * self.changed_days / (self.days - 1) if self.days > 1 else 0.0

    @property
    def drop_from_low_pct(self) -> float | None:
        """Насколько последняя цена выше накопленного минимума."""
        if self.low <= 0:
            return None
        return (self.last - self.low) / self.low * 100

    def is_mature(self, min_days: int) -> bool:
        """Достаточно ли наблюдений, чтобы минимуму можно было верить."""
        return self.days >= min_days


def last_snapshot_at(raw: dict) -> float:
    """Когда снимался прошлый срез. 0 — история пуста или ещё без отметки."""
    row = raw.get(META_KEY)
    if isinstance(row, (list, tuple)) and row:
        try:
            return float(row[0])
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def decode(raw: dict) -> dict[str, Record]:
    """Хранимый вид -> записи. Битые строки пропускаем молча, они не критичны."""
    out: dict[str, Record] = {}
    for name, row in raw.items():
        if name == META_KEY:
            continue
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        try:
            out[name] = Record(
                low=float(row[_MIN]), high=float(row[_MAX]),
                days=int(row[_DAYS]), changed_days=int(row[_CHANGED]),
                last=float(row[_LAST]), since=float(row[_SINCE]),
            )
        except (TypeError, ValueError):
            continue
    return out


def encode(records: dict[str, Record], *, now: float | None = None) -> dict:
    out: dict = {
        name: [
            round(r.low, 2), round(r.high, 2), r.days,
            r.changed_days, round(r.last, 2), round(r.since),
        ]
        for name, r in records.items()
    }
    out[META_KEY] = [round(time.time() if now is None else now)]
    return out


def coverage(records: dict[str, Record], mature_days: int) -> tuple[int, int, int]:
    """
    Насколько история уже пригодна: (всего предметов, зрелых, максимум дней).

    Нужно, чтобы честно объяснять в /dips, почему настоящего минимума ещё нет:
    «накоплено 3 дня из 7» понятно, а молчание выглядит поломкой.
    """
    if not records:
        return 0, 0, 0
    return (
        len(records),
        sum(1 for r in records.values() if r.is_mature(mature_days)),
        max(r.days for r in records.values()),
    )


class SnapshotStats(NamedTuple):
    tracked: int      # сколько предметов в истории после слияния
    added: int        # появилось впервые
    updated: int      # обновлено
    changed: int      # у скольких цена отличалась от прошлого среза
    reset: int        # у скольких окно наблюдения началось заново
    mature: int       # у скольких накоплено достаточно дней

    def describe(self, min_days: int) -> str:
        return (
            f"предметов {self.tracked} (новых {self.added}), "
            f"цена изменилась у {self.changed}, "
            f"окно перезапущено у {self.reset}, "
            f"готовых к использованию ({min_days}+ дней) {self.mature}"
        )


def merge_snapshot(
    stored: dict[str, Record],
    prices: dict[str, float],
    *,
    now: float | None = None,
    min_price: float = MIN_TRACKED_PRICE,
    mature_days: int = 7,
) -> tuple[dict[str, Record], SnapshotStats]:
    """
    Влить сегодняшние цены в накопленное. Возвращает (история, что произошло).

    Чистая функция: ни хранилища, ни сети — так её можно проверить на
    выдуманных тридцати днях за миллисекунды, не дожидаясь настоящего месяца.
    """
    now = time.time() if now is None else now
    merged: dict[str, Record] = {}
    added = updated = changed = reset = 0

    for name, price in prices.items():
        if not price or price < min_price:
            continue

        old = stored.get(name)
        if old is None:
            merged[name] = Record(price, price, 1, 0, price, now)
            added += 1
            continue

        # Окно истекло — начинаем заново от сегодняшней цены. Иначе минимум,
        # пойманный однажды на обвале, остался бы минимумом навсегда, и
        # «просадка к минимуму» перестала бы что-либо значить.
        if now - old.since >= _WINDOW_SECONDS:
            merged[name] = Record(price, price, 1, 0, price, now)
            reset += 1
            continue

        moved = abs(price - old.last) >= 0.01
        merged[name] = Record(
            low=min(old.low, price),
            high=max(old.high, price),
            days=old.days + 1,
            changed_days=old.changed_days + (1 if moved else 0),
            last=price,
            since=old.since,
        )
        updated += 1
        if moved:
            changed += 1

    # Предметы, пропавшие из сегодняшнего среза, из истории НЕ выбрасываем:
    # пропуск на день бывает и от сбоя выгрузки, а стирание накопленного
    # обнулило бы месяц наблюдений из-за одной неудачной загрузки.
    for name, old in stored.items():
        if name not in merged and now - old.since < _WINDOW_SECONDS:
            merged[name] = old

    stats = SnapshotStats(
        tracked=len(merged), added=added, updated=updated,
        changed=changed, reset=reset,
        mature=sum(1 for r in merged.values() if r.is_mature(mature_days)),
    )
    return merged, stats
