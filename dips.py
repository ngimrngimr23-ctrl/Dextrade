"""
Просадки: предметы, торгующиеся заметно дешевле своей месячной нормы.

Чем это отличается от всего остального в боте. /markets и /arbnow ищут разрыв
в ПРОСТРАНСТВЕ: один и тот же предмет прямо сейчас стоит на площадке дешевле,
чем в Steam. Купил — тут же продал, разрыв реализуется мгновенно.

Здесь разрыв во ВРЕМЕНИ: предмет дёшев относительно собственного прошлого.
Чтобы на этом заработать, цена должна вернуться к норме, а она может не
вернуться никогда. Риск принципиально другой, и путать эти две вещи нельзя.

Зато данные бесплатны. Прайс-лист csgotrader и так скачивается для цен
стикеров, и в нём по каждому предмету лежат все окна сразу:

    {"last_24h": 3.84, "last_7d": 5.85, "last_30d": 5.90, "last_90d": 6.20}

Сравнение — арифметика в памяти по 32 тысячам записей: ни одного запроса к
Steam, ни прокси, ни лимитов. Это единственная часть бота, которая работает,
когда всё остальное упирается в 429.

Оговорка про «минимум за месяц». В окнах его нет — там средняя цена
состоявшихся сделок за период. У Steam минимум лежит в pricehistory, который
требует авторизации и режется жёстче priceoverview, то есть на 32 тысячи
предметов недоступен. Поэтому минимум бот копит сам: price_history снимает
срез цен раз в сутки, и через неделю наблюдений у предмета появляется
настоящий минимум за окно. Отбор при этом остаётся на средней — история
приходит только обогащением (поля low, activity_pct), иначе команда молчала бы
до конца первого месяца.
"""

from __future__ import annotations

import logging
from typing import NamedTuple
from urllib.parse import quote

log = logging.getLogger("steam_bot.dips")

MONTH_WINDOW = "last_30d"
WEEK_WINDOW = "last_7d"
TODAY_WINDOW = "last_24h"

# Насколько недельная цена может отставать от месячной, чтобы просадку ещё
# можно было считать свежей.
#
# Смысл проверки. Одного условия «сегодня ниже месяца на N%» мало: под него
# одинаково подходят просадка и падающий нож, а это противоположные вещи.
# Различает их недельное окно:
#
#   сегодня 3.84, неделя 5.85, месяц 5.90  — держалось месяц, упало сегодня
#   сегодня 3.84, неделя 4.60, месяц 5.90  — падает третью неделю подряд
#
# В первом случае неделя почти равна месяцу, во втором — заметно ниже. Порог
# в 10% отделяет обычный шум от установившегося тренда.
MAX_WEEK_DECLINE_PCT = 10.0

# Минимальная цена предмета. На копеечных позициях процент огромен, а деньги
# никакие — та же причина, что и в market_prices.MIN_MARKET_PRICE.
MIN_PRICE = 1.0


class Dip(NamedTuple):
    """Предмет, просевший относительно месячной нормы."""

    market_hash_name: str
    today: float          # средняя за сутки — самое свежее, что есть на весь каталог
    week: float
    month: float
    drop_pct: float       # насколько сегодня ниже месячной нормы
    week_decline_pct: float   # насколько неделя ниже месяца: мера «ножевости»

    # Ниже — из собственной накопленной истории (price_history). None означает
    # «ещё не накоплено», а не «ноль»: путать эти два состояния нельзя, на
    # объёме продаж бот уже один раз на этом обжёгся.
    low: float | None = None          # настоящий минимум за окно наблюдения
    history_days: int = 0             # сколько срезов за ним стоит
    activity_pct: float | None = None  # доля дней, когда цена менялась

    @property
    def at_low(self) -> bool:
        """
        Предмет стоит на своём накопленном минимуме или ниже.

        Это ровно то, что просили изначально: «на минимумах цены за месяц».
        Пока история не накоплена, ответа нет — и False здесь означает именно
        «не знаю», поэтому наружу он идёт только вместе с history_days.
        """
        return self.low is not None and self.today <= self.low * 1.001

    @property
    def below_low_pct(self) -> float | None:
        """Насколько сегодня ниже накопленного минимума. Отрицательное — выше."""
        if not self.low:
            return None
        return (self.low - self.today) / self.low * 100

    @property
    def steam_url(self) -> str:
        return (
            "https://steamcommunity.com/market/listings/730/"
            + quote(self.market_hash_name, safe="")
        )

    def recovery_gain_pct(self, fee_multiplier: float) -> float:
        """
        Сколько останется чистыми, если цена вернётся к месячной норме.

        Считаем сразу за вычетом комиссии Steam: без неё процент обманывает.
        Просадка в 13% при комиссии 13% — это ровно ноль, а выглядит находкой.
        """
        return (self.month * fee_multiplier - self.today) / self.today * 100


def find_dips(
    details: dict,
    *,
    min_drop_pct: float,
    min_price: float = MIN_PRICE,
    max_price: float | None = None,
    max_week_decline_pct: float = MAX_WEEK_DECLINE_PCT,
    limit: int | None = None,
    history: dict | None = None,
    mature_days: int = 7,
) -> tuple[list[Dip], dict[str, int]]:
    """
    Найти просадки. Возвращает (находки, причины отсева).

    details — то, что отдаёт pricing.get_csgotrader_price_details():
    market_hash_name -> SteamPrice с полем windows.

    history — накопленное самим ботом: market_hash_name -> price_history.Record.
    Отбором оно не управляет, только обогащает находки настоящим минимумом и
    признаком активности. Так сделано намеренно: история набирается неделями,
    и если поставить её условием, команда до конца месяца молчала бы вовсе.

    Отсев считаем по причинам и возвращаем: без разбивки «нашлось три» и
    «нашлось три тысячи» выглядят одинаково непонятно, а понять, слишком ли
    строг порог, по одному числу нельзя.
    """
    history = history or {}
    dropped = {
        "нет суточного окна": 0,
        "нет месячного окна": 0,
        "дёшево": 0,
        "дорого": 0,
        "просадка ниже порога": 0,
        "падающий нож": 0,
    }
    found: list[Dip] = []

    for name, price in details.items():
        windows = getattr(price, "windows", None)
        if not windows:
            continue

        today = windows.get(TODAY_WINDOW)
        if not today:
            # Без суточного окна предмет сегодня не торговался — значит и
            # «просадки сегодня» у него быть не может.
            dropped["нет суточного окна"] += 1
            continue

        month = windows.get(MONTH_WINDOW)
        if not month or month <= 0:
            dropped["нет месячного окна"] += 1
            continue

        if today < min_price:
            dropped["дёшево"] += 1
            continue
        if max_price is not None and today > max_price:
            dropped["дорого"] += 1
            continue

        drop_pct = (month - today) / month * 100
        if drop_pct < min_drop_pct:
            dropped["просадка ниже порога"] += 1
            continue

        # Падающий нож: неделя тоже заметно ниже месяца, то есть предмет
        # снижается давно и «норма» уже не норма.
        week = windows.get(WEEK_WINDOW, month)
        week_decline = (month - week) / month * 100
        if week_decline > max_week_decline_pct:
            dropped["падающий нож"] += 1
            continue

        # Накопленное по этому предмету. Незрелую запись не подставляем вовсе:
        # «минимум за два дня» — это не минимум, а меньшая из двух цен, и
        # показывать её как минимум значит врать.
        record = history.get(name)
        mature = record is not None and record.is_mature(mature_days)

        found.append(Dip(
            market_hash_name=name,
            today=today,
            week=week,
            month=month,
            drop_pct=drop_pct,
            week_decline_pct=week_decline,
            low=record.low if mature else None,
            history_days=record.days if record is not None else 0,
            activity_pct=record.activity_pct if mature else None,
        ))

    # Сначала те, кто стоит на собственном минимуме: спрашивали именно про
    # них, а просадка от средней — лишь способ их найти. Внутри каждой группы
    # порядок по величине просадки.
    found.sort(key=lambda d: (d.at_low, d.drop_pct), reverse=True)
    at_low = sum(1 for d in found if d.at_low)
    log.info(
        "dips: просмотрено %d, прошло %d (на своём минимуме %d). Отсеяно: %s",
        len(details), len(found), at_low,
        ", ".join(f"{k} {v}" for k, v in dropped.items() if v) or "ничего",
    )
    return (found[:limit] if limit else found), dropped
