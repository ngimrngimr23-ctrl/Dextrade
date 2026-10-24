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

    предлагаем = min(верхний_чужой_ордер + шаг цены, наш_потолок_прибыли)

Шаг — не цент: у CSFloat своя сетка цен для ордеров (см. PRICE_STEPS), и
цену вне сетки сервер не примет.

Если перебить верхнего можно только выйдя за потолок — ордер не ставим вовсе.
Лучше не купить, чем купить без прибыли: смысл всей затеи в марже, а не в
обладании предметом.

Когда чужих ордеров нет, предлагаем сразу потолок. Это по-прежнему гарантирует
min_profit_pct (он в потолок и заложен), но даёт наибольший шанс, что продавец
выберет именно нас.

ЗАЯВКА НЕ ДОРОЖЕ ВИТРИНЫ. Если наша цена дотягивает до самого дешёвого лота
на CSFloat, ордер исполнится в момент постановки — CSFloat сам купит этот лот
(«Your order was completed and instantly bought 1 item»). Прибыль при этом
настоящая, но переплачивать сверх цены лота незачем: цена срезается до
ближайшей допустимой не ниже витрины, и план помечается как мгновенный.

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
# Насколько может гулять цена за месяц: разброс средних за сутки, неделю и
# месяц (pricing.SteamPrice.month_spread_pct). Десять процентов на живых
# данных 2026-10-24 проходят две трети скинов $1–65 с продажами за сутки.
DEFAULT_MAX_MONTH_SPREAD_PCT = envcfg.env_float("ORDER_MAX_MONTH_SPREAD_PCT", 10.0)

# СЕТКА ЦЕН ОРДЕРОВ CSFLOAT: (цена ДО которой действует шаг, шаг), всё в центах.
#
# Ввели её против ботов, которые перебивали друг друга по центу («Say goodbye
# to 1-cent wars»), и это меняет арифметику перебивания: «верхний + 1 цент»
# на предмете за $20 сервер отклонит, туда можно только $20.10.
#
# Таблица официальная — из интерфейса самого CSFloat (диалог «Buy Order Price
# Increments», файл переводов assets/i18n/en.json):
#
#     ниже $5         $0.01
#     $5 – $10        $0.05
#     $10 – $100      $0.10
#     $100 – $500     $1.00
#     $500 – $1,000   $5.00
#     выше $1,000     $10.00
#
# Границы удобные: каждая кратна шагам по обе стороны, поэтому на самой
# границе ($10.00) цена допустима в любой трактовке, и округление никогда не
# выбрасывает в соседний ряд неправильным шагом.
PRICE_STEPS: tuple[tuple[int | None, int], ...] = (
    (500, 1),
    (1000, 5),
    (10000, 10),
    (50000, 100),
    (100000, 500),
    (None, 1000),
)

# Насколько должна упасть цена, чтобы снижать уже стоящий ордер.
#
# Снижение — это отмена и новый ордер, то есть два запроса из квоты ключа и
# риск остаться вовсе без ордера, если второй не пройдёт. Ради двух шагов
# сетки это не делается: нужно сэкономить заметную долю цены.
REPRICE_MIN_SAVING_PCT = envcfg.env_float("ORDER_REPRICE_MIN_SAVING_PCT", 3.0)


class OrderPlan(NamedTuple):
    """Что бот предложил бы за предмет. Все цены — в центах."""

    market_hash_name: str
    price_cents: int          # сколько предлагаем
    ceiling_cents: int        # потолок, выше которого прибыли уже нет
    steam_price_cents: int    # цена продажи в Steam, от которой считали
    profit_pct: float         # прибыль после комиссии при нашей цене
    rival_cents: int | None   # верхний чужой ордер, если он есть
    why: str                  # человеческое объяснение цены
    # Цена дотягивает до самого дешёвого лота на CSFloat: ордер купит его в
    # момент постановки, ждать продавца не придётся.
    instant: bool = False


def price_step(price_cents: int) -> int:
    """Шаг сетки CSFloat для этой цены, в центах."""
    for upper, step in PRICE_STEPS:
        if upper is None or price_cents < upper:
            return step
    return PRICE_STEPS[-1][1]


def is_valid_price(price_cents: int) -> bool:
    """Ложится ли цена на сетку CSFloat — то есть примет ли её сервер."""
    return price_cents >= 1 and price_cents % price_step(price_cents) == 0


def floor_valid(price_cents: int) -> int:
    """
    Ближайшая допустимая цена НЕ ВЫШЕ данной. 0 — такой нет.

    Вниз — потому что это всегда потолок: выше него прибыль кончается, и
    округление в большую сторону съело бы её молча.
    """
    if price_cents < 1:
        return 0
    return price_cents - price_cents % price_step(price_cents)


def at_least(price_cents: int) -> int:
    """Ближайшая допустимая цена НЕ НИЖЕ данной."""
    price = max(1, int(price_cents))
    # Округление вверх может перешагнуть границу ряда — тогда шаг меняется, и
    # проверяем ещё раз. Больше одного перехода не бывает: граница кратна
    # шагам обоих рядов.
    while not is_valid_price(price):
        step = price_step(price)
        price += step - price % step
    return price


def above(rival_cents: int) -> int:
    """Самая дешёвая допустимая цена СТРОГО выше чужого ордера."""
    return at_least(int(rival_cents) + 1)


def steam_sale_cents(ask: float | None, median: float | None) -> int | None:
    """
    По какой цене предмет реально продастся в Steam, в центах. None — не знаем.

    Нижний лот (ask) — сколько сейчас стоит купить, а значит и примерно за
    сколько продашь, встав рядом с ним. Но на тонком предмете нижний лот бывает
    одинокой завышенной заявкой, по которой никто не покупает, — тогда честнее
    медиана настоящих сделок за сутки. Берём МЕНЬШЕЕ из двух: ошибка вниз
    стоит недобранной прибыли, ошибка вверх — убытка на каждом исполненном
    ордере.

    Обычно меньшее — сам ask: по замеру на двадцати предметах нижний лот ниже
    медианы сделок примерно на 13% (см. storage.set_steam_price). Медиана
    срезает цену только там, где нижний лот выбивается вверх.
    """
    values = [float(v) for v in (ask, median) if v and float(v) > 0]
    if not values:
        return None
    return int(round(min(values) * 100))


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
UNKNOWN_STABILITY = "нет истории цен за месяц — стабильность не проверить"


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
    ask_cents: int | None = None,
    require_spread: bool = False,
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

    ask_cents — самый дешёвый лот на CSFloat, если известен. Нужен, чтобы не
    предлагать больше, чем просит продавец: такая заявка исполнится сразу,
    и всё, что сверх цены лота, — переплата.

    spread_pct — разброс цены за месяц (pricing.SteamPrice.month_spread_pct).
    require_spread=True — неизвестный разброс считается отказом: стабильность
    требуют подтвердить, а не предположить.
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

    if spread_pct is None and require_spread:
        return None, UNKNOWN_STABILITY
    if spread_pct is not None and spread_pct > max_spread_pct:
        return None, (
            f"цена за месяц гуляет на {spread_pct:.0f}% (сутки, неделя, месяц) "
            f"при пороге {max_spread_pct:g}% — продавать через неделю будем "
            "неизвестно почём"
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
        # Потолок — не обязательно допустимая цена: сетка CSFloat ниже $5
        # идёт по центу, выше — пятаками, гривенниками и долларами. Округляем
        # ВНИЗ, иначе вылезем за прибыль.
        price = floor_valid(ceiling)
        if price < 1:
            return None, "слишком дёшево: на сетке цен CSFloat нет цены ниже потолка"
        why = (
            "чужих ордеров нет — ставим по потолку, чтобы взяли первыми "
            f"(потолок задаёт {bound})"
        )
    else:
        price = above(top)
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
        step = price - top
        why = (
            f"на цент выше верхнего чужого ордера ${top / 100:.2f}" if step == 1
            else f"на шаг сетки CSFloat (${step / 100:.2f}) выше верхнего "
                 f"чужого ордера ${top / 100:.2f}"
        )

    instant = False
    if ask_cents and price >= ask_cents:
        # Заявка дотягивает до витрины — CSFloat купит лот сразу. Предлагать
        # больше цены лота незачем: ставим ближайшую допустимую цену не ниже
        # витрины, но и не ниже того, что нужно, чтобы обойти соперника.
        crossing = at_least(ask_cents)
        if top is not None:
            crossing = max(crossing, above(top))
        price = min(price, crossing)
        instant = True
        why = (
            f"лот на CSFloat уже за ${ask_cents / 100:.2f} — ордер по этой цене "
            "купит его сразу, переплачивать до потолка незачем"
        )

    return (
        OrderPlan(
            market_hash_name=market_hash_name,
            price_cents=price,
            ceiling_cents=ceiling,
            steam_price_cents=steam_price_cents,
            profit_pct=profit_pct_at(price, steam_price_cents),
            rival_cents=top,
            why=why,
            instant=instant,
        ),
        why,
    )


# ---------------------------------------------------------------------------
# Поддержка уже стоящего ордера
# ---------------------------------------------------------------------------
#
# Ордер, поставленный и забытый, перестаёт работать за часы: кто-то встаёт на
# шаг выше, и все продавцы уходят к нему. Поэтому стоящий ордер регулярно
# сверяется со стаканом, и решение о нём принимается здесь — тем же правилом,
# что и при постановке: «на шаг выше верхнего, но не выше потолка».

KEEP = "keep"
RAISE = "raise"
LOWER = "lower"
CANCEL = "cancel"


class Reprice(NamedTuple):
    """Что делать со стоящим ордером. Цены в центах."""

    action: str           # KEEP / RAISE / LOWER / CANCEL
    price_cents: int      # новая цена; у KEEP и CANCEL — текущая
    why: str
    # Нас обошли, а ответить нельзя — выше потолок. Отдельным флагом, потому
    # что это не действие, а новость: о ней стоит сказать один раз.
    outbid: bool = False


def reprice(
    *,
    current_cents: int,
    ceiling_cents: int,
    rivals_cents: list[int],
    min_saving_pct: float = REPRICE_MIN_SAVING_PCT,
) -> Reprice:
    """
    Решить, что делать со стоящим ордером.

    rivals_cents — ЧУЖИЕ ордера: свой ордер из стакана вызывающий вычёркивает
    сам, иначе мы бы перебивали самих себя.

    ceiling_cents — потолок на сейчас. Он может оказаться НИЖЕ текущей цены
    (цена Steam упала) — тогда ордер снижается, а если прибыли не остаётся
    ни при какой цене — снимается.

    Чего здесь нет намеренно: подъёма к потолку, когда соперников нет.
    Сидя одни наверху, мы и так первые, и платить больше незачем.
    """
    cap = floor_valid(ceiling_cents)
    if cap < 1:
        return Reprice(
            CANCEL, current_cents,
            "прибыли не остаётся ни при какой цене — снимаю ордер",
        )

    rivals = sorted((r for r in rivals_cents if r > 0), reverse=True)
    top = rivals[0] if rivals else None

    if current_cents > ceiling_cents:
        # Потолок опустился ниже нашей цены — значит, Steam подешевел, и
        # покупка по нынешней цене уже не даёт целевой прибыли.
        target = cap if top is None else min(above(top), cap)
        return Reprice(
            LOWER, target,
            f"потолок опустился до ${ceiling_cents / 100:.2f} — снижаю ордер",
            outbid=top is not None and target <= top,
        )

    if top is not None and top >= current_cents:
        # Нас обошли или встали вровень. Вровень — тоже проигрыш: при равной
        # цене первым исполняется тот, кто встал раньше.
        want = above(top)
        if want <= cap:
            return Reprice(
                RAISE, want,
                f"обошли на ${top / 100:.2f} — встаю на шаг выше",
            )
        return Reprice(
            KEEP, current_cents,
            f"обошли на ${top / 100:.2f}; ответить можно только за "
            f"${want / 100:.2f}, а потолок ${cap / 100:.2f} — остаюсь в очереди",
            outbid=True,
        )

    if top is None:
        return Reprice(KEEP, current_cents, "чужих ордеров нет — стоим первыми")

    want = above(top)
    saving = current_cents - want
    threshold = max(2 * price_step(current_cents),
                    current_cents * min_saving_pct / 100)
    if saving >= threshold:
        return Reprice(
            LOWER, want,
            f"переплачиваем: следующий ордер ${top / 100:.2f}, первыми можно "
            f"стоять и за ${want / 100:.2f}",
        )
    return Reprice(KEEP, current_cents, f"стоим первыми, следом ${top / 100:.2f}")
