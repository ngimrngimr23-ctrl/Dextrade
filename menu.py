"""
Интерактивное меню бота: описание порогов и сборка клавиатур.

Зачем оно вообще появилось. Команд в боте накопилось 39, из них 12 — семейство
set*, и по имени команды нельзя было понять ни к какому движку она относится,
ни что означает её число. Живой пример: /setdefaults 5 7 задаёт «минимум
наклеек $5» и «доплата за наклейки не выше 7% их стоимости», причём второе
число регулярно читали как «лот дороже голого скина на 7%» — это разные вещи,
и разница в деньгах кратная.

Лечится это не переименованием, а сменой носителя. Пороги трогают редко, у
каждого числовой параметр, и синтаксис к следующему разу забывается — то есть
ровно тот случай, где кнопка с подписью и примером выигрывает у команды с
позиционными аргументами. Действия (сканировать, проверить арбитраж) остались
командами: там ты знаешь, чего хочешь, и печатать быстрее, чем тыкать.

Модуль намеренно не импортирует bot.py и ничего не знает про хранилище: здесь
только описание порогов и разметка. Чтение значений, разбор ввода и запись —
в bot.py, где живут storage и джобы. Так спецификацию можно проверять
отдельно, без поднятия всего бота.
"""

from typing import NamedTuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# Разделители в callback_data. Telegram даёт на неё 64 байта, так что ключи
# короткие: "s|st_min" вместо человекочитаемых путей.
NAV = "m"      # навигация по узлам меню
EDIT = "s"     # начать правку порога
ACT = "a"      # действие (скан, сброс кулдауна, ...)


class Setting(NamedTuple):
    """
    Один порог: как называется, что означает и что у пользователя спросить.

    hint — не «расшифровка названия», а ответ на вопрос «что изменится, если я
    это подвину». Именно его не хватало командам: /setpricefilter объяснял свой
    формат, но не объяснял, зачем нужен.
    """

    key: str
    section: str
    label: str
    kind: str          # как разбирать ввод, см. _parse_setting в bot.py
    hint: str
    example: str
    can_off: bool = True   # принимает ли «выкл»


class Section(NamedTuple):
    key: str
    title: str
    intro: str


SECTIONS: tuple[Section, ...] = (
    Section(
        "sticker",
        "🏷 Стикеры",
        "Отбор лотов по наклейкам — то, чем занят автоскан вотчлиста.",
    ),
    Section(
        "float",
        "💎 Флоат",
        "Охота за редким флоатом. Идёт по отдельному списку /float, "
        "со стикерами не связана.",
    ),
    Section(
        "arb",
        "💱 Арбитраж",
        "Лоты CSFloat, которые дешевле цены Steam. Вотчлист не нужен — "
        "сканируется весь рынок.",
    ),
    Section(
        "markets",
        "🏪 Площадки",
        "Сравнение Steam со сторонними площадками по всему каталогу (/markets).",
    ),
    Section(
        "sched",
        "⏱ Расписание",
        "Как часто бот ходит в Steam. Главный рычаг против 429.",
    ),
)


SETTINGS: tuple[Setting, ...] = (
    # --- Стикеры -----------------------------------------------------------
    Setting(
        "st_min", "sticker", "Минимум наклеек на лоте", "money",
        "Сумма рыночных цен всех наклеек на лоте. Ниже этого — лот "
        "пропускается, даже если наклейки достались даром: возиться ради "
        "пары долларов нечего.",
        "5", can_off=False,
    ),
    Setting(
        "st_markup", "sticker", "Доплата за наклейки", "pct",
        "Какую долю стоимости наклеек ты доплачиваешь сверх цены голого "
        "скина. 0% — наклейки бесплатно, 100% — платишь их полную цену "
        "(смысла нет). Это НЕ «на сколько процентов лот дороже».",
        "7", can_off=False,
    ),
    Setting(
        "st_streak", "sticker", "Доплата для стрик-лотов", "pct",
        "Отдельный порог доплаты для лотов с четырьмя и более одинаковыми "
        "наклейками подряд — они ценятся выше, и за них не жалко переплатить. "
        "Выключено — действует обычный порог.",
        "15",
    ),
    Setting(
        "st_ratio", "sticker", "Вес наклеек", "ratio",
        "Во сколько раз наклейки должны быть дороже голого скина. 2 — набор "
        "вдвое дороже самого скина. Отсекает случаи, где доплата отличная, но "
        "набор стоит копейки.",
        "2",
    ),
    Setting(
        "st_price", "sticker", "Цена лота", "pair_money",
        "Диапазон итоговой цены лота вместе с наклейками — то, что реально "
        "заплатишь. Про выгодность ничего не говорит, только про кошелёк.",
        "10 200",
    ),
    # --- Флоат -------------------------------------------------------------
    Setting(
        "fl_range", "float", "Диапазон флоата", "pair_float",
        "Ищем лоты с флоатом ниже первого числа (топ для Factory New) или "
        "выше второго (топ для Battle-Scarred). Пока не задан — флоат не "
        "проверяется вообще и лишних запросов не тратится.",
        "0.01 0.99",
    ),
    Setting(
        "fl_markup", "float", "Наценка на находку", "pct",
        "Показывать находку, только если она дороже самого дешёвого лота "
        "предмета не больше чем на N%. Отсекает случаи, где продавец уже знает "
        "про редкий флоат и заложил его в цену.",
        "15",
    ),
    # --- Арбитраж ----------------------------------------------------------
    Setting(
        "ar_disc", "arb", "Порог скидки", "pct",
        "Насколько лот на CSFloat должен быть дешевле цены Steam, чтобы бот "
        "о нём сообщил. Выключить — выключить арбитраж целиком.",
        "20",
    ),
    Setting(
        "ar_int", "arb", "Интервал автоскана", "minutes",
        "Пауза между автоматическими прогонами арбитража. Упирается в квоту "
        "CSFloat — 200 запросов в час на ключ, и прокси её не умножают.",
        "10", can_off=False,
    ),
    Setting(
        "ar_price", "arb", "Цена лота", "pair_money",
        "Диапазон цены лота на CSFloat.",
        "5 500",
    ),
    Setting(
        "ar_vol", "arb", "Ликвидность", "int",
        "Минимум продаж на Steam за сутки. Скидка на предмете, который почти "
        "не продаётся, обычно бумажная — выйти из него не получится.",
        "5",
    ),
    Setting(
        "ar_stick", "arb", "Наклейки почти даром", "pct",
        "Дополнительно ловить лоты, где сам скин не дешевле рынка, но наклейки "
        "достаются почти бесплатно. Та же логика доплаты, что у вотчлиста.",
        "10",
    ),
    # --- Площадки ----------------------------------------------------------
    Setting(
        "mk_disc", "markets", "Минимальный спред", "pct",
        "На сколько процентов площадка должна быть дешевле Steam.",
        "20", can_off=False,
    ),
    Setting(
        "mk_max", "markets", "Потолок спреда", "pct",
        "Выше этой скидки — почти всегда дефект данных, а не находка: разные "
        "площадки называют одним именем разные предметы.",
        "60", can_off=False,
    ),
    Setting(
        "mk_vol", "markets", "Продажи в Steam за сутки", "int",
        "Фильтр неликвида: сколько экземпляров предмета уходит в Steam за сутки.",
        "5", can_off=False,
    ),
    Setting(
        "mk_profit", "markets", "Минимальная прибыль", "money",
        "Сколько чистыми должно оставаться после комиссии Steam, чтобы находка "
        "стоила времени.",
        "5", can_off=False,
    ),
    Setting(
        "mk_price", "markets", "Минимальная цена предмета", "money",
        "Ниже этой цены предметы не смотрим — проценты там красивые, деньги нет.",
        "10", can_off=False,
    ),
    Setting(
        "mk_count", "markets", "Максимум лотов", "int",
        "Сколько экземпляров предмета выставлено. Сотни лотов означают ходовой "
        "товар, где разрыв цен обычно либо дефект данных, либо исчезнет раньше, "
        "чем до него дойдут руки. Находки с неизвестным количеством фильтр не "
        "трогает.",
        "50",
    ),
    Setting(
        "mk_interval", "markets", "Автопрогон", "minutes",
        "Как часто проверять площадки самому, без команды. Присылаются только "
        "новые находки. Чаще десяти минут смысла нет: каталог у источника "
        "обновляется примерно раз в час.",
        "60",
    ),
    # --- Расписание --------------------------------------------------------
    Setting(
        "wt_int", "sched", "Пауза между прогонами", "minutes",
        "Сколько ждать после конца одного прогона вотчлиста до начала "
        "следующего. Steam смотрит на суммарное число запросов в час, а не на "
        "промежуток между двумя — так что это главный рычаг против 429.",
        "25", can_off=False,
    ),
)

BY_KEY: dict[str, Setting] = {s.key: s for s in SETTINGS}
BY_SECTION: dict[str, list[Setting]] = {
    sec.key: [s for s in SETTINGS if s.section == sec.key] for sec in SECTIONS
}
SECTION_BY_KEY: dict[str, Section] = {s.key: s for s in SECTIONS}


# ---------------------------------------------------------------------------
# Клавиатуры
# ---------------------------------------------------------------------------

def _rows(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([r for r in rows if r])


def _back(node: str) -> InlineKeyboardButton:
    return InlineKeyboardButton("‹ Назад", callback_data=f"{NAV}|{node}")


def root() -> InlineKeyboardMarkup:
    """
    Главный экран. Первыми — действия, а не настройки: в девяти случаях из
    десяти меню открывают, чтобы что-то запустить, а не чтобы подкрутить порог.
    """
    return _rows(
        [InlineKeyboardButton("🔎 Сканировать сейчас", callback_data=f"{ACT}|scanall")],
        [InlineKeyboardButton("💱 Арбитраж сейчас", callback_data=f"{ACT}|arbnow")],
        [InlineKeyboardButton("🏪 Площадки сейчас", callback_data=f"{ACT}|markets")],
        [
            InlineKeyboardButton("📋 Списки", callback_data=f"{NAV}|lists"),
            InlineKeyboardButton("📊 Состояние", callback_data=f"{NAV}|state"),
        ],
        [
            InlineKeyboardButton("⚙️ Пороги", callback_data=f"{NAV}|set"),
            InlineKeyboardButton("🌐 Прокси", callback_data=f"{NAV}|proxy"),
        ],
        [InlineKeyboardButton("📄 Прайс-лист", callback_data=f"{NAV}|prices")],
    )


def sections() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(sec.title, callback_data=f"{NAV}|set:{sec.key}")]
        for sec in SECTIONS
    ]
    rows.append([_back("root")])
    return InlineKeyboardMarkup(rows)


def section(section_key: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(s.label, callback_data=f"{EDIT}|{s.key}")]
        for s in BY_SECTION[section_key]
    ]
    rows.append([_back("set")])
    return InlineKeyboardMarkup(rows)


def editing(setting: Setting) -> InlineKeyboardMarkup:
    """Экран правки: остаётся только уйти назад или, если можно, выключить."""
    row = []
    if setting.can_off:
        row.append(InlineKeyboardButton("Выключить", callback_data=f"{ACT}|off:{setting.key}"))
    row.append(_back(f"set:{setting.section}"))
    return InlineKeyboardMarkup([row])


def lists(paused: bool) -> InlineKeyboardMarkup:
    toggle = (
        InlineKeyboardButton("▶️ Включить автоскан", callback_data=f"{ACT}|resume")
        if paused
        else InlineKeyboardButton("⏸ Остановить автоскан", callback_data=f"{ACT}|pause")
    )
    return _rows(
        [InlineKeyboardButton("🔎 Сканировать сейчас", callback_data=f"{ACT}|scanall")],
        [toggle],
        [_back("root")],
    )


def state(show_reset: bool) -> InlineKeyboardMarkup:
    """
    Кнопка сброса кулдауна показывается ТОЛЬКО когда кулдаун есть.

    Так действие появляется ровно в той ситуации, ради которой существует, и
    его не приходится помнить как команду. Раньше это был /arbreset, про
    который вспоминали в последнюю очередь.
    """
    rows = []
    if show_reset:
        rows.append([InlineKeyboardButton("♻️ Сбросить кулдаун", callback_data=f"{ACT}|arbreset")])
    rows.append([InlineKeyboardButton("🔄 Обновить", callback_data=f"{NAV}|state")])
    rows.append([_back("root")])
    return InlineKeyboardMarkup(rows)


def proxy() -> InlineKeyboardMarkup:
    return _rows(
        [InlineKeyboardButton("🔍 Проверить прокси", callback_data=f"{ACT}|proxycheck")],
        [InlineKeyboardButton("🗑 Забыть добавленные", callback_data=f"{ACT}|proxyclear")],
        [_back("root")],
    )


def prices() -> InlineKeyboardMarkup:
    return _rows(
        [InlineKeyboardButton("🔬 Сверить источники цен", callback_data=f"{ACT}|pricecheck")],
        [InlineKeyboardButton("🔑 Проверить ключ SIH", callback_data=f"{ACT}|sihkey")],
        [InlineKeyboardButton("📥 Загрузить прайс-лист", callback_data=f"{ACT}|pricefile")],
        [InlineKeyboardButton("🗑 Очистить прайс-лист", callback_data=f"{ACT}|clearprices")],
        [_back("root")],
    )
