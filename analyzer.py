"""
Логика отбора: сколько стоит набор стикеров на лоте и укладывается ли
цена лота в "минимальная цена голого скина + стоимость стикеров + не более
N% наценки сверху".
"""

from dataclasses import dataclass

from steam_client import Listing


@dataclass
class Offer:
    price: float
    floor_price: float
    overpay: float
    stickers_value: float
    markup_pct: float
    stickers: list[str]
    inspect_link: str | None = None


def _floor_price(listings: list[Listing]) -> float:
    """
    Базовая цена голого скина — цена самого первого лота на самой первой
    странице листингов (Steam Market всегда отдаёт лоты отсортированными
    от дешёвых к дорогим), независимо от того, есть на нём стикеры или нет.
    """
    return listings[0].price if listings else 0.0


def find_offers(
    listings: list[Listing],
    sticker_prices: dict[str, float],
    min_stickers_value: float = 5.0,
    max_markup_pct: float = 7.0,
) -> list[Offer]:
    floor_price = _floor_price(listings)

    offers = []
    for listing in listings:
        if not listing.stickers:
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

        if markup_pct <= max_markup_pct:
            offers.append(
                Offer(
                    price=listing.price,
                    floor_price=floor_price,
                    overpay=overpay,
                    stickers_value=stickers_value,
                    markup_pct=markup_pct,
                    stickers=listing.stickers,
                    inspect_link=listing.inspect_link,
                )
            )

    offers.sort(key=lambda o: o.markup_pct)
    return offers
    
