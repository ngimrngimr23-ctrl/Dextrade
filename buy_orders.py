"""
Ордера на покупку CSFloat: сколько предлагать и стоит ли вообще.

Что это и почему отдельным модулем. Здесь считается ЕДИНСТВЕННОЕ число — цена,
которую бот готов предложить за предмет, — и решается, ставить ли ордер вообще.
Всё остальное (сеть, чат, расписание) живёт снаружи. Так сделано потому, что
это первая часть бота, которая тратит настоящие деньги: логику, где ошибка
стоит денег, нужно уметь проверять целиком на столе, без единого запроса.

ТРИ ВОРОТ, через которые предмет должен пройти.

1. ЛИКВИДНОСТЬ. Ордер на предмет, который в Steam продаётся пару раз в месяц,
   исполнится неизвестно когда, а выйти из него будет не у кого. «Скидка» на
   неликвиде бумажная — это мы уже проходили в /dips.

2. УСТОЙЧИВОСТЬ ЦЕНЫ. Считаем по расхождению окон прайс-листа: если суточная и
   недельная цена разъезжаются сильнее MAX_SPREAD_PCT, то «цена Steam» — это не
   число, а диапазон, и прибыль, посчитанная от него, воображаемая.

3. ПРИБЫЛЬ С УЧЁТОМ КОМИССИИ. Продажа в Steam отдаёт продавцу лишь
   STEAM_FEE_MULTIPLIER от цены. Ордер имеет смысл только если после комиссии
   остаётся хотя бы min_profit_pct.

КАК ВЫБИРАЕТСЯ ЦЕНА. У каждого предмета на CSFloat свой стакан ордеров, и
исполняется первым самый высокий. Поэтому:

    предлагаем = min(верхний_чужой_ордер + 1 цент, наш_потолок_прибыли)

Если перебить верхнего можно только выйдя за потолок — ордер не ставим вовсе.
Лучше не купить, чем купить без прибыли: смысл всей затеи в марже, а не в
обладании предметом.

Когда чужих ордеров нет, предлагаем сразу потолок. Это по-прежнему гарантирует
min_profit_pct (он в потолок и заложен), но даёт наибольший шанс, что продавец
выберет именно нас.

ЦЕНЫ ВЕЗДЕ В ЦЕНТАХ, потому что CSFloat принимает их в центах, а дробные
доллары в промежуточных расчётах — прямой путь к ошибке на копейку в невыгодную
сторону.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_FLOOR
from typing import NamedTuple

import envcfg

# Сколько продавец получает от цены Steam после комиссии. То же число, что в
# analyzer — комиссия одна на весь бот.
STEAM_FEE_MULTIPLIER = 0.87

# Та же комиссия для внутренних расчётов, но ТОЧНАЯ.
#
# Не педантизм. На float потолок цены садится ровно на границу и промахивается
# мимо неё в невыгодную сторону: при цене Steam $1.60 выходит 139.2/1.2 = 116.0,
# floor даёт 116, а обратная проверка прибыли — 19.99999999999999% при цели 20%.
# Найдено property-тестом на трёх сотнях цен подряд. Одна четырнадцатая цифра
# после запятой ничего не стоит на графике и стоит денег на сделке, поэтому вся
# арифметика центов идёт на Decimal.
_FEE = Decimal("0.87")
_HUNDRED = Decimal(100)

# Значения по умолчанию. Все переопределяются настройками чата.
DEFAULT_MIN_PROFIT_PCT = envcfg.env_float("ORDER_MIN_PROFIT_PCT", 20.0)
# Ликвидность считаем В НЕДЕЛЮ — теми же единицами, что и /dips. Суточные и
# недельные числа в разных командах однажды обязательно сравнят друг с другом.
DEFAULT_MIN_VOLUME_PER_WEEK = envcfg.env_int("ORDER_MIN_WEEK_VOLUME", 21)
DEFAULT_MAX_SPREAD_PCT = envcfg.env_float("ORDER_MAX_SPREAD_PCT", 15.0)
DEFAULT_MAX_ORDER_USD = envcfg.env_float("ORDER_MAX_USD", 20.0)


class OrderPlan(NamedTuple):
    """Что бот предложил бы за предмет. Все цены — в центах."""

    market_hash_name: str
    price_cents: int          # сколько предлагаем
    ceiling_cents: int        # потолок, выше которого прибыли уже нет
    steam_price_cents: int    # цена продажи в Steam, от которой считали
    profit_pct: float         # прибыль после комиссии при нашей цене
    rival_cents: int | None   # верхний чужой ордер, если он есть
    why: str                  # человеческое объяснение цены


def net_after_fee(steam_price_cents: int) -> Decimal:
    """Сколько останется на руках после комиссии Steam, в центах."""
    return Decimal(int(steam_price_cents)) * _FEE


def profit_pct_at(price_cents: int, steam_price_cents: int) -> float:
    """Прибыль в процентах, если купить за price и продать в Steam."""
    if price_cents <= 0:
        return 0.0
    got = (net_after_fee(steam_price_cents) - Decimal(int(price_cents)))
    return float(got / Decimal(int(price_cents)) * _HUNDRED)


def ceiling_for(steam_price_cents: int, min_profit_pct: float) -> int:
    """
    Дороже этого покупать нельзя — прибыль просядет ниже целевой.

    Округляем ВНИЗ: лишний цент здесь всегда не в нашу пользу.
    """
    if steam_price_cents <= 0:
        return 0
    divisor = _HUNDRED + Decimal(str(min_profit_pct))
    exact = net_after_fee(steam_price_cents) * _HUNDRED / divisor
    return int(exact.to_integral_value(rounding=ROUND_FLOOR))


# Причина отказа «объёма нет». Константой, потому что её разбирает /orders,
# чтобы объяснить, ОТКУДА взялось незнание, — а сверка по литералу в двух
# файлах живёт ровно до первой правки формулировки.
UNKNOWN_VOLUME = "объём продаж неизвестен — на деньгах не гадаем"


def plan(
    market_hash_name: str,
    *,
    steam_price_cents: int,
    volume_per_week: int | None,
    spread_pct: float | None,
    rival_orders_cents: list[int] | None = None,
    min_profit_pct: float = DEFAULT_MIN_PROFIT_PCT,
    min_volume_per_week: int = DEFAULT_MIN_VOLUME_PER_WEEK,
    max_spread_pct: float = DEFAULT_MAX_SPREAD_PCT,
    max_order_usd: float = DEFAULT_MAX_ORDER_USD,
) -> tuple[OrderPlan | None, str]:
    """
    Решить, ставить ли ордер, и по какой цене.

    Возвращает (план или None, причина). Причина заполняется ВСЕГДА — и когда
    отказ, и когда согласие: по ней потом видно, чем бот руководствовался, без
    раскопок в коде.

    volume_per_week=None означает «не знаем», и это НЕ то же самое, что ноль.
    Неизвестное отсеиваем — на деньгах домысливать нельзя, — и говорим об этом
    отдельной причиной (UNKNOWN_VOLUME). Единственное исключение —
    min_volume_per_week=0: это прямо сказанное «ликвидность не проверяй».
    """
    if steam_price_cents <= 0:
        return None, "нет цены Steam — сравнивать не с чем"

    if volume_per_week is None:
        # Порог 0 — прямо сказанное «ликвидность не проверяй», и только тогда
        # неизвестный объём проходит. Разница принципиальная: проверку
        # выключил человек, а не мы решили за него, что и так сойдёт.
        #
        # Отдельная дверь нужна потому, что объёма может не быть по причине,
        # не имеющей отношения к предмету: Steam отдаёт его только через
        # priceoverview, а тот регулярно лежит под 429. В такие часы отказ
        # «объём неизвестен» — это новость про Steam, а не про ликвидность,
        # и человеку должно быть чем её обойти, если он понимает, на что идёт.
        if min_volume_per_week > 0:
            return None, UNKNOWN_VOLUME
    elif volume_per_week < min_volume_per_week:
        return None, (
            f"продаётся {volume_per_week} шт/нед при пороге {min_volume_per_week} — "
            "выйти обратно будет не у кого"
        )

    if spread_pct is not None and spread_pct > max_spread_pct:
        return None, (
            f"цена скачет: окна расходятся на {spread_pct:.0f}% при пороге "
            f"{max_spread_pct:.0f}% — прибыль от такой цены воображаемая"
        )

    # Два ограничения сверху, и они РАЗНЫЕ по смыслу. Первое — где кончается
    # целевая прибыль, второе — сколько мы вообще готовы вложить в один ордер.
    # Складывать их в одно число нельзя: тогда отказ «прибыль кончается на $20»
    # врёт, если на самом деле упёрлись в лимит вложения. Сообщение должно
    # называть настоящую причину — чинить потом надо разные настройки.
    profit_ceiling = ceiling_for(steam_price_cents, min_profit_pct)
    if profit_ceiling < 1:
        return None, "слишком дёшево: после комиссии не остаётся и цента"

    cap_cents = int(max_order_usd * 100)
    ceiling = min(profit_ceiling, cap_cents)
    bound = "лимит на ордер" if cap_cents < profit_ceiling else "целевая прибыль"

    rivals = sorted(rival_orders_cents or [], reverse=True)
    top = rivals[0] if rivals else None

    if top is None:
        price = ceiling
        why = (
            "чужих ордеров нет — ставим по потолку, чтобы взяли первыми "
            f"(потолок задаёт {bound})"
        )
    else:
        price = top + 1
        if price > ceiling:
            limit_note = (
                f"лимит на один ордер ${max_order_usd:.2f}"
                if bound == "лимит на ордер"
                else f"прибыль кончается на ${profit_ceiling / 100:.2f}"
            )
            return None, (
                f"перебить верхний ордер (${top / 100:.2f}) можно только за "
                f"${price / 100:.2f}, а {limit_note}"
            )
        why = f"на цент выше верхнего чужого ордера ${top / 100:.2f}"

    return (
        OrderPlan(
            market_hash_name=market_hash_name,
            price_cents=price,
            ceiling_cents=ceiling,
            steam_price_cents=steam_price_cents,
            profit_pct=profit_pct_at(price, steam_price_cents),
            rival_cents=top,
            why=why,
        ),
        why,
    )
