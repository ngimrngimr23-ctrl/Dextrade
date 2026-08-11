"""
Логика отбора: сколько стоит набор стикеров на лоте и укладывается ли
цена лота в "цена стикеров + не более N% наценки".
"""

from dataclasses import dataclass

from steam_client import Listing


@dataclass
class Offer:
    price: float
    stickers_value: float
    markup_pct: float
    stickers: list[str]


def find_offers(
    listings: list[Listing],
    sticker_prices: dict[str, float],
    min_stickers_value: float = 5.0,
    max_markup_pct: float = 7.0,
) -> list[Offer]:
    offers = []
    for listing in listings:
        if not listing.stickers:
            continue

        stickers_value = sum(sticker_prices.get(s, 0.0) for s in listing.stickers)
        if stickers_value < min_stickers_value:
            continue

        markup = listing.price - stickers_value
        markup_pct = (markup / stickers_value) * 100 if stickers_value else float("inf")

        if markup_pct <= max_markup_pct:
            offers.append(
                Offer(
                    price=listing.price,
                    stickers_value=stickers_value,
                    markup_pct=markup_pct,
                    stickers=listing.stickers,
                )
            )

    offers.sort(key=lambda o: o.markup_pct)
    return offers
