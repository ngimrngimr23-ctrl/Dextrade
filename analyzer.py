"""
Логика отбора: сколько стоит набор стикеров на лоте и укладывается ли
цена лота в "минимальная цена голого скина + стоимость стикеров + не более
N% наценки сверху".
"""

import logging
from dataclasses import dataclass

from steam_client import Listing
# Единый источник правды про «свежесть» окна — там же, где цена и выбирается.
from pricing import CSGOTRADER_FRESH_WINDOWS as FRESH_PRICE_WINDOWS

log = logging.getLogger("steam_bot.analyzer")

STREAK_THRESHOLD = 4  # от скольки подряд идущих одинаковых стикеров считаем это "стриком"


@dataclass
class Offer:
    price: float
    floor_price: float
    overpay: float
    stickers_value: float
    markup_pct: float
    stickers: list[str]
    streak: int = 0  # длина самой длинной последовательности подряд одинаковых стикеров (0-5)
    inspect_link: str | None = None
    # Флоат лота, если его удалось раскодировать локально из inspect-ссылки
    # (см. cs_inspect.py). Показывается справочно на ЛЮБОМ оффере — декодирование
    # бесплатное, сетевых запросов не требует.
    float_value: float | None = None
    # True, если лот попал в подборку ИМЕННО из-за редкого флоата, а не по стикерам.
    found_by_float: bool = False


def _floor_price(listings: list[Listing]) -> float:
    """
    Базовая цена голого скина — цена самого первого лота на самой первой
    странице листингов (Steam Market всегда отдаёт лоты отсортированными
    от дешёвых к дорогим), независимо от того, есть на нём стикеры или нет.
    """
    return listings[0].price if listings else 0.0


def _max_streak(stickers: list[str]) -> int:
    """
    Длина самой длинной последовательности ПОДРЯД идущих одинаковых стикеров.
    stickers идут в порядке слотов на оружии (см. steam_client._parse_listings_html),
    поэтому совпадение соседних элементов списка = совпадение соседних слотов.
    """
    if not stickers:
        return 0
    best = cur = 1
    for i in range(1, len(stickers)):
        if stickers[i] == stickers[i - 1]:
            cur += 1
            best = max(best, cur)
        else:
            cur = 1
    return best


def find_offers(
    listings: list[Listing],
    sticker_prices: dict[str, float],
    min_stickers_value: float = 5.0,
    max_markup_pct: float = 7.0,
    streak_max_markup_pct: float | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    listing_floats: dict[str, float] | None = None,
) -> list[Offer]:
    """
    streak_max_markup_pct: отдельный порог наценки для "стрик"-лотов (от
    STREAK_THRESHOLD подряд идущих одинаковых стикеров) — None означает не
    задан, для таких лотов действует обычный max_markup_pct, как для всех.
    min_price/max_price: фильтр по итоговой цене лота (с учётом стикеров),
    любой из них можно не задавать (None = без ограничения с этой стороны).
    listing_floats: раскодированные флоаты по inspect_link — на отбор НЕ влияют,
    подставляются в оффер справочно, чтобы показать флоат в сообщении.
    """
    listing_floats = listing_floats or {}
    floor_price = _floor_price(listings)

    offers = []
    for listing in listings:
        if not listing.stickers:
            continue
        if min_price is not None and listing.price < min_price:
            continue
        if max_price is not None and listing.price > max_price:
            continue

        stickers_value = sum(sticker_prices.get(s, 0.0) for s in listing.stickers)
        if stickers_value < min_stickers_value:
            continue

        # насколько этот лот дороже голого скина — это и есть "цена, которую
        # продавец просит за стикеры" по факту; сравниваем её с реальной
        # стоимостью стикеров, а не с полной ценой лота
        overpay = listing.price - floor_price

        # markup_pct = какую долю реальной стоимости стикеров ты фактически
        # платишь сверху. 0% — стикеры достались бесплатно, 100% — платишь
        # ровно их полную стоимость (уже не выгодно), больше 100% — переплата.
        markup_pct = (overpay / stickers_value) * 100 if stickers_value else float("inf")

        streak = _max_streak(listing.stickers)
        effective_max_markup = (
            streak_max_markup_pct
            if streak >= STREAK_THRESHOLD and streak_max_markup_pct is not None
            else max_markup_pct
        )

        if markup_pct <= effective_max_markup:
            offers.append(
                Offer(
                    price=listing.price,
                    floor_price=floor_price,
                    overpay=overpay,
                    stickers_value=stickers_value,
                    markup_pct=markup_pct,
                    stickers=listing.stickers,
                    streak=streak,
                    inspect_link=listing.inspect_link,
                    float_value=listing_floats.get(listing.inspect_link) if listing.inspect_link else None,
                )
            )

    offers.sort(key=lambda o: o.markup_pct)
    return offers


def find_float_offers(
    listings: list[Listing],
    listing_floats: dict[str, float],
    float_low_max: float,
    float_high_min: float,
    max_markup_pct: float | None = None,
) -> list[Offer]:
    """
    Отдельная, не связанная со стикерами подборка — лот интересен, если его
    флоат близко к 0 (топ для Factory New) ИЛИ близко к 1 (топ для
    Battle-Scarred): float_low_max — верхняя граница "низкого" флоата,
    float_high_min — нижняя граница "высокого". listing_floats — уже
    раскодированные флоаты по inspect_link (декодируются локально в
    cs_inspect.py, сюда приходят готовыми — этот модуль сети не касается).
    Лоты без стикеров сюда тоже попадают — критерий чисто по флоату.

    max_markup_pct: насколько цена лота может быть выше самого дешёвого лота
    этого предмета (floor_price), чтобы находка всё ещё считалась "недооценённой"
    редким флоатом, а не просто дорогим лотом, который продавец и так продаёт
    по honest-цене за редкость. None — без ограничения по цене (как раньше).
    """
    floor_price = _floor_price(listings)

    offers = []
    for listing in listings:
        if not listing.inspect_link:
            continue
        float_value = listing_floats.get(listing.inspect_link)
        if float_value is None:
            continue
        if not (float_value <= float_low_max or float_value >= float_high_min):
            continue

        overpay = listing.price - floor_price
        # markup_pct = на сколько % цена лота выше самого дешёвого лота этого
        # предмета — та же идея, что и наценка по стикерам (overpay/floor_price
        # вместо overpay/stickers_value, потому что у флоата нет своей "цены",
        # с которой можно сравнить переплату).
        markup_pct = (overpay / floor_price) * 100 if floor_price else float("inf")
        if max_markup_pct is not None and markup_pct > max_markup_pct:
            continue

        offers.append(
            Offer(
                price=listing.price,
                floor_price=floor_price,
                overpay=overpay,
                stickers_value=0.0,  # флоат-подборка цену стикеров не учитывает
                markup_pct=markup_pct,
                stickers=listing.stickers,
                inspect_link=listing.inspect_link,
                float_value=float_value,
                found_by_float=True,
            )
        )

    # самые "экстремальные" (ближе к 0 или к 1) — наверх
    offers.sort(key=lambda o: min(o.float_value, 1 - o.float_value))
    return offers


# ---------------------------------------------------------------------------
# Кросс-маркет арбитраж: лот дешевле на CSFloat, чем стоит на Steam.
# Данные приходят одним ответом CSFloat (там уже есть и цена Steam, и цены
# наклеек), поэтому в Steam за ними ходить не нужно — см. csfloat_client.py.
# ---------------------------------------------------------------------------

# Steam забирает комиссию с продавца (~13% от цены, которую платит покупатель).
# Без её учёта "скидка 15%" выглядит прибылью, хотя ею не является.
STEAM_FEE_MULTIPLIER = 0.87


@dataclass
class ArbOffer:
    market_hash_name: str
    csfloat_price: float
    steam_price: float
    discount_pct: float          # на сколько % дешевле цены Steam
    net_after_fee: float         # сколько останется, если перепродать в Steam по той же цене
    listing_id: str
    steam_volume: int | None = None
    float_value: float | None = None
    wear_name: str | None = None
    stickers: list[str] = None
    stickers_value: float = 0.0
    # Наценка за наклейки по той же формуле, что у стикерного арбитража в Steam:
    # сколько сверх голой цены платишь относительно реальной стоимости набора.
    # None — если наклеек нет или их цены неизвестны.
    sticker_markup_pct: float | None = None
    stickers_unpriced: int = 0
    inspect_link: str | None = None
    watchers: int = 0
    # Окно прайс-листа, из которого взята steam_price. Показывается в
    # сообщении: цена за сутки и цена за неделю — разные основания для решения.
    steam_price_window: str | None = None

    def __post_init__(self):
        if self.stickers is None:
            self.stickers = []

    @property
    def url(self) -> str:
        return f"https://csfloat.com/item/{self.listing_id}"


def find_arbitrage_offers(
    listings,
    min_discount_pct: float,
    *,
    min_price: float | None = None,
    max_price: float | None = None,
    min_steam_volume: int | None = None,
    sticker_max_markup_pct: float | None = None,
) -> list[ArbOffer]:
    """
    listings — объекты csfloat_client.CSFloatListing.

    Отбор идёт по двум независимым основаниям, лот проходит по любому:

    1. ГОЛАЯ ЦЕНА: лот дешевле цены Steam минимум на min_discount_pct.
       Работает и для лотов с наклейками — там наклейки просто идут бонусом.

    2. НАКЛЕЙКИ (если задан sticker_max_markup_pct): та же логика, что у
       стикерного арбитража внутри Steam — сколько сверх голой цены просят за
       набор наклеек относительно его реальной стоимости. 0% значит наклейки
       достались даром. Ловит случаи, когда сам скин не дешевле рынка, но
       наклейки на нём фактически бесплатны.

    min_steam_volume отсекает неликвид: "скидка" на предмете, который на Steam
    продаётся пару раз в месяц, чаще всего бумажная — выйти из него не выйдет.
    """
    offers: list[ArbOffer] = []
    # Счётчики отсева. Без них «просмотрено 200 лотов, подошло 0» не отвечает
    # на единственный важный вопрос — какое условие всё выбросило. Ровно на
    # этом мы и застряли: порог 0.1% пропускает всё, что дешевле Steam хоть на
    # копейку, то есть ноль офферов из двух сотен лотов означал не строгий
    # отбор, а поломку в данных — но отличить это по логам было нельзя.
    dropped = {
        "нет цены Steam": 0,
        "цена Steam устарела (30/90 дней)": 0,
        "цена вне диапазона": 0,
        "мало продаж": 0,
        "объём неизвестен (не отсеян)": 0,
        "скидка ниже порога": 0,
    }
    best_discount: float | None = None

    for l in listings:
        if l.steam_price is None or l.steam_price <= 0:
            dropped["нет цены Steam"] += 1
            continue
        # Цена из окна 30/90 дней — не цена, а воспоминание о ней. Считать от
        # неё скидку нельзя: если предмет с тех пор подешевел, получится
        # выдуманная скидка, а сортировка по убыванию скидки вытащит такие
        # лоты в самый верх подборки. Ровно это и случилось 2026-08-19 —
        # все находки были 57-60% и все по неликвиду.
        if l.steam_price_window and l.steam_price_window not in FRESH_PRICE_WINDOWS:
            dropped["цена Steam устарела (30/90 дней)"] += 1
            continue  # без цены Steam сравнивать не с чем
        if min_price is not None and l.price < min_price:
            dropped["цена вне диапазона"] += 1
            continue
        if max_price is not None and l.price > max_price:
            dropped["цена вне диапазона"] += 1
            continue
        # Неизвестный объём продаж больше НЕ считается нулевым. Он приезжал из
        # того же item.scm, что и цена Steam, и вместе с ним пропал — а прежнее
        # (l.steam_volume or 0) превращало "не знаем" в "ноль продаж" и с
        # заданным min_volume выбрасывало вообще всё, ровно тем же молчаливым
        # способом, что и пропавшая цена. Неизвестное не отсеиваем, но считаем
        # отдельно, чтобы это было видно в логе, а не выглядело как пустой рынок.
        if min_steam_volume is not None and l.steam_volume is None:
            dropped["объём неизвестен (не отсеян)"] += 1
        elif min_steam_volume is not None and l.steam_volume < min_steam_volume:
            dropped["мало продаж"] += 1
            continue

        discount_pct = (l.steam_price - l.price) / l.steam_price * 100
        if best_discount is None or discount_pct > best_discount:
            best_discount = discount_pct

        # Наценка за наклейки — считаем только когда цены наклеек реально известны
        sticker_markup = None
        if l.stickers and l.stickers_value > 0:
            overpay = l.price - l.steam_price
            sticker_markup = overpay / l.stickers_value * 100

        by_price = discount_pct >= min_discount_pct
        by_stickers = (
            sticker_max_markup_pct is not None
            and sticker_markup is not None
            and sticker_markup <= sticker_max_markup_pct
        )
        if not (by_price or by_stickers):
            dropped["скидка ниже порога"] += 1
            continue

        offers.append(
            ArbOffer(
                market_hash_name=l.market_hash_name,
                csfloat_price=l.price,
                steam_price=l.steam_price,
                discount_pct=discount_pct,
                net_after_fee=l.steam_price * STEAM_FEE_MULTIPLIER - l.price,
                listing_id=l.listing_id,
                steam_volume=l.steam_volume,
                float_value=l.float_value,
                wear_name=l.wear_name,
                stickers=list(l.stickers),
                stickers_value=l.stickers_value,
                sticker_markup_pct=sticker_markup,
                stickers_unpriced=len(l.stickers) - l.stickers_priced,
                inspect_link=l.inspect_link,
                watchers=l.watchers,
                steam_price_window=l.steam_price_window,
            )
        )

    offers.sort(key=lambda o: o.discount_pct, reverse=True)

    # Логируем всегда, а не только при нуле офферов: разбивка нужна и чтобы
    # понять, что порог наконец стал разумным, и чтобы заметить перекос
    # (например, что почти всё уходит в "нет цены Steam" — это уже не настройки,
    # а изменившийся формат ответа CSFloat).
    log.info(
        "arb-фильтр: %d лот(ов) -> %d оффер(ов). Отсеяно: %s. Лучшая скидка среди "
        "лотов с ценой Steam: %s",
        len(listings), len(offers),
        ", ".join(f"{k} {v}" for k, v in dropped.items() if v) or "ничего",
        f"{best_discount:.1f}%" if best_discount is not None else "—",
    )
    if listings and dropped["нет цены Steam"] == len(listings):
        log.warning(
            "arb-фильтр: цены Steam НЕТ НИ У ОДНОГО из %d лотов — сравнивать не с чем. "
            "Это не настройки отбора: похоже, в ответе CSFloat больше нет item.scm.price "
            "(см. csfloat_client._parse_listing).",
            len(listings),
        )

    return offers

