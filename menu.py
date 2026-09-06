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
        "dips",
        "📉 Просадки",
        "Предметы дешевле собственной месячной нормы (/dips). Разрыв во "
        "ВРЕМЕНИ, а не между площадками: чтобы заработать, цена должна "
        "вернуться — и она может не вернуться.",
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
        "Два числа: флоат ниже первого (топ для Factory New) или выше второго "
        "(топ для Battle-Scarred). Четыре числа задают диапазоны целиком — "
        "FN от, FN до, BS от, BS до: «0.003 0.02 0.9 0.98» отсечёт и совсем "
        "крайние флоаты, которые давно известны и стоят своих денег. Пока не "
        "задан — флоат не проверяется вообще и лишних запросов не тратится.",
        "0.01 0.99 (или 0.003 0.02 0.9 0.98)",
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
        "новые находки. Сам SIH выдержит любую частоту — там один запрос на "
        "прогон; дорога проверка находок живой ценой Steam, и при частом "
        "прогоне бот про это предупредит.",
        "60",
    ),
    # --- Просадки ----------------------------------------------------------
    Setting(
        "dp_drop", "dips", "Просадка от нормы", "pct",
        "Насколько сегодняшняя цена ниже средней за 30 дней, чтобы предмет "
        "считался просевшим. Проверяется по ЖИВОЙ цене, а не по средней за "
        "сутки: за день предмет мог успеть подорожать обратно.",
        "25", can_off=False,
    ),
    Setting(
        "dp_vol", "dips", "Продаётся в неделю", "int",
        "Минимум продаж, штук за неделю. Главный фильтр мусора: вся идея "
        "просадки в том, что цена вернётся к норме, а норма по двум сделкам "
        "за месяц — это не норма, а две сделки, и выйти из такого предмета "
        "не получится вовсе.\n"
        "Steam отдаёт объём только за сутки, поэтому недельный считается как "
        "суточный × 7 — это оценка по одному дню, а не точное число. "
        "Выключить — показывать и то, что почти не торгуется.",
        "7",
    ),
    Setting(
        "dp_price", "dips", "Цена предмета", "pair_money",
        "Диапазон цены. Раньше был жёстко зашит снизу в $1 и никак не "
        "ограничен сверху — находка на нож за $2000 приходила человеку с "
        "бюджетом в полсотни.",
        "5 300",
    ),
    Setting(
        "dp_int", "dips", "Автопрогон", "minutes",
        "Как часто искать просадки самому. Отбор по каталогу бесплатен и в "
        "лимиты Steam не упирается; дорога только живая проверка верхушки.",
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
    Главный экран: сначала запуски, потом предметные разделы, в конце
    служебное.

    Порядок не случайный. В девяти случаях из десяти меню открывают, чтобы
    что-то запустить, а не подкрутить порог, поэтому три «сейчас» стоят
    первыми. Дальше — то, чем управляют регулярно (вотчлист, флоат,
    инвентарь), и только потом состояние с настройками.

    Прокси и прайс-лист с этого экрана убраны внутрь настроек намеренно: это
    администрирование инфраструктуры, а не торговля. Держать их рядом с
    «Сканировать сейчас» — то же самое, что показывать пользователю
    внутреннее устройство бота.
    """
    return _rows(
        [InlineKeyboardButton("🔎 Сканировать сейчас", callback_data=f"{ACT}|scanall")],
        [InlineKeyboardButton("💱 Арбитраж сейчас", callback_data=f"{ACT}|arbnow")],
        [
            InlineKeyboardButton("🏪 Площадки", callback_data=f"{ACT}|markets"),
            InlineKeyboardButton("📉 Просадки", callback_data=f"{ACT}|dips"),
        ],
        [
            InlineKeyboardButton("📋 Вотчлист", callback_data=f"{NAV}|watch"),
            InlineKeyboardButton("💎 Флоат", callback_data=f"{NAV}|float"),
        ],
        [InlineKeyboardButton("📦 Инвентарь", callback_data=f"{NAV}|inv")],
        [
            InlineKeyboardButton("📊 Статус", callback_data=f"{NAV}|state"),
            InlineKeyboardButton("⚙️ Настройки", callback_data=f"{NAV}|set"),
        ],
        [InlineKeyboardButton("❓ Помощь", callback_data=f"{ACT}|help")],
    )


def watchlist(paused: bool) -> InlineKeyboardMarkup:
    """
    Экран вотчлиста: операции над списком плюс управление автосканом.

    Операции и настройки намеренно разведены. Здесь только то, что делают со
    СПИСКОМ (добавить, убрать, показать, очистить) и с его прогоном (запустить,
    поставить на паузу). Пороги отбора — что считать находкой — живут в
    настройках, потому что их трогают раз в месяц, а список правят постоянно.
    """
    toggle = (
        InlineKeyboardButton("▶️ Возобновить автоскан", callback_data=f"{ACT}|resume")
        if paused
        else InlineKeyboardButton("⏸ Пауза автоскана", callback_data=f"{ACT}|pause")
    )
    return _rows(
        [InlineKeyboardButton("🔎 Сканировать сейчас", callback_data=f"{ACT}|scanall")],
        [
            InlineKeyboardButton("➕ Добавить", callback_data=f"{ACT}|w_add"),
            InlineKeyboardButton("➖ Удалить", callback_data=f"{ACT}|w_del"),
        ],
        [
            InlineKeyboardButton("📋 Показать", callback_data=f"{ACT}|w_list"),
            InlineKeyboardButton("🗑 Очистить", callback_data=f"{NAV}|ask:w_clear"),
        ],
        [toggle],
        [InlineKeyboardButton("🔥 Приоритетные", callback_data=f"{ACT}|w_hot")],
        [_back("root")],
    )


def float_list() -> InlineKeyboardMarkup:
    """
    Экран охоты за флоатом. Тот же набор операций, что у вотчлиста, плюс
    разовая проверка «платят ли вообще за низкий флоат на этом скине».

    Называется float_list, а не float: имя float в модуле затенило бы
    встроенный тип, и первая же аннотация с ним начала бы врать.
    """
    return _rows(
        [
            InlineKeyboardButton("➕ Добавить", callback_data=f"{ACT}|f_add"),
            InlineKeyboardButton("➖ Удалить", callback_data=f"{ACT}|f_del"),
        ],
        [
            InlineKeyboardButton("📋 Показать", callback_data=f"{ACT}|f_list"),
            InlineKeyboardButton("🗑 Очистить", callback_data=f"{NAV}|ask:f_clear"),
        ],
        [InlineKeyboardButton("🔬 Проверить скин", callback_data=f"{ACT}|f_check")],
        [InlineKeyboardButton("⚙️ Пороги флоата", callback_data=f"{NAV}|set:float")],
        [_back("root")],
    )


def inventory(linked: bool) -> InlineKeyboardMarkup:
    """
    Экран инвентаря. Пока аккаунт не привязан, показывать «оценить» и
    «следить» нечестно — они всё равно ответят «сначала привяжи».
    """
    if not linked:
        return _rows(
            [InlineKeyboardButton("🔗 Привязать аккаунт", callback_data=f"{ACT}|i_link")],
            [_back("root")],
        )
    return _rows(
        [InlineKeyboardButton("💰 Оценить сейчас", callback_data=f"{ACT}|i_value")],
        [InlineKeyboardButton("🔔 Следить за ростом", callback_data=f"{ACT}|i_watch")],
        [InlineKeyboardButton("🔗 Сменить аккаунт", callback_data=f"{ACT}|i_link")],
        [_back("root")],
    )


def confirm(action: str, back_node: str) -> InlineKeyboardMarkup:
    """
    Спросить подтверждение перед необратимым действием.

    Нужно ровно из-за перехода на кнопки. Команда /watch очистить требовала
    напечатать слово «очистить» — это само по себе было подтверждением. Кнопка
    же стирает шестьсот предметов одним касанием, промахнуться по соседней
    «Показать» легко, а отмены нет.
    """
    return _rows(
        [InlineKeyboardButton("🗑 Да, очистить", callback_data=f"{ACT}|{action}")],
        [InlineKeyboardButton("‹ Отмена", callback_data=f"{NAV}|{back_node}")],
    )


def sections() -> InlineKeyboardMarkup:
    """
    Настройки: пороги отбора плюс два служебных раздела.

    Прокси и прайс-лист — не пороги, у них нет числового значения, поэтому
    они не в SECTIONS, а дописаны отдельными кнопками. Место им всё же здесь:
    это настройка инфраструктуры, и с главного экрана она уехала именно сюда.
    """
    rows = [
        [InlineKeyboardButton(sec.title, callback_data=f"{NAV}|set:{sec.key}")]
        for sec in SECTIONS
    ]
    rows.append([InlineKeyboardButton("🌐 Прокси", callback_data=f"{NAV}|proxy")])
    rows.append([InlineKeyboardButton("📄 Прайс-лист стикеров", callback_data=f"{NAV}|prices")])
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
    """
    Прежний общий экран «Списки». Оставлен рабочим: на него ведут ссылки из
    старых сообщений в чате, а кнопка в уже отправленном сообщении живёт
    вечно и после перестройки меню не обновляется. Из нового главного экрана
    сюда не попасть — там вотчлист и флоат разведены по своим экранам.
    """
    toggle = (
        InlineKeyboardButton("▶️ Включить автоскан", callback_data=f"{ACT}|resume")
        if paused
        else InlineKeyboardButton("⏸ Остановить автоскан", callback_data=f"{ACT}|pause")
    )
    return _rows(
        [InlineKeyboardButton("🔎 Сканировать сейчас", callback_data=f"{ACT}|scanall")],
        [toggle],
        [
            InlineKeyboardButton("📋 Вотчлист", callback_data=f"{NAV}|watch"),
            InlineKeyboardButton("💎 Флоат", callback_data=f"{NAV}|float"),
        ],
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
    # Логи живут именно здесь: за ними идут ровно тогда, когда смотрят
    # состояние и видят, что что-то не так.
    rows.append([
        InlineKeyboardButton("📄 Лог", callback_data=f"{ACT}|logs"),
        InlineKeyboardButton("⚠️ Только ошибки", callback_data=f"{ACT}|logs_err"),
    ])
    rows.append([InlineKeyboardButton("🔄 Обновить", callback_data=f"{NAV}|state")])
    rows.append([_back("root")])
    return InlineKeyboardMarkup(rows)


def proxy() -> InlineKeyboardMarkup:
    # «Назад» ведёт в настройки, а не на главный: теперь сюда приходят оттуда,
    # и возврат на главный терял бы место, откуда пришли.
    return _rows(
        [InlineKeyboardButton("🔍 Проверить прокси", callback_data=f"{ACT}|proxycheck")],
        [InlineKeyboardButton("➕ Добавить прокси", callback_data=f"{ACT}|p_add")],
        [InlineKeyboardButton("🗑 Забыть добавленные", callback_data=f"{ACT}|proxyclear")],
        [_back("set")],
    )


def prices() -> InlineKeyboardMarkup:
    return _rows(
        [InlineKeyboardButton("🔬 Сверить источники цен", callback_data=f"{ACT}|pricecheck")],
        [InlineKeyboardButton("🔑 Проверить ключ SIH", callback_data=f"{ACT}|sihkey")],
        [InlineKeyboardButton("📥 Загрузить прайс-лист", callback_data=f"{ACT}|pricefile")],
        [InlineKeyboardButton("🗑 Очистить прайс-лист", callback_data=f"{ACT}|clearprices")],
        [_back("set")],
    )
