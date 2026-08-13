"""
Логика отбора: сколько стоит набор стикеров на лоте и укладывается ли
цена лота в "минимальная цена голого скина + стоимость стикеров + не более
N% наценки сверху".
"""

from dataclasses import dataclass

from steam_client import Listing


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
) -> list[Offer]:
    """
    streak_max_markup_pct: отдельный порог наценки для "стрик"-лотов (от
    STREAK_THRESHOLD подряд идущих одинаковых стикеров) — None означает не
    задан, для таких лотов действует обычный max_markup_pct, как для всех.
    min_price/max_price: фильтр по итоговой цене лота (с учётом стикеров),
    любой из них можно не задавать (None = без ограничения с этой стороны).
    """
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
                )
            )

    offers.sort(key=lambda o: o.markup_pct)
    return offers

