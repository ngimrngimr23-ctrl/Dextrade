"""
Работа со Steam Community Market: получение всех листингов по предмету
и разбор цены + стикеров на каждом лоте.

Запросы к Steam при наличии STEAM_PROXY_URL идут через Cloudflare Worker.
"""

import asyncio
import os
import re
import urllib.parse
from dataclasses import dataclass, field

import aiohttp


APP_ID = 730
RENDER_COUNT = 100
REQUEST_DELAY = 1.5

STEAM_PROXY_URL = os.environ.get(
    "STEAM_PROXY_URL",
    ""
).rstrip("/")


@dataclass
class Listing:
    price: float
    stickers: list[str] = field(default_factory=list)
    inspect_link: str | None = None


def market_hash_name_from_url(url: str) -> str:
    """
    Из ссылки:
    https://steamcommunity.com/market/listings/730/<name>
    получает market_hash_name.
    """

    parsed = urllib.parse.urlparse(url)
    parts = parsed.path.rstrip("/").split("/")

    name_encoded = parts[-1]

    return urllib.parse.unquote(name_encoded)


def render_url(
    market_hash_name: str,
    start: int
) -> str:
    """
    Формирует Steam Market /render URL.

    norender=1 заставляет Steam вернуть JSON
    вместо HTML-страницы.
    """

    encoded = urllib.parse.quote(
        market_hash_name,
        safe=""
    )

    return (
        f"https://steamcommunity.com/market/listings/"
        f"{APP_ID}/{encoded}/render/"
        f"?query="
        f"&start={start}"
        f"&count={RENDER_COUNT}"
        f"&country=US"
        f"&language=english"
        f"&currency=1"
        f"&norender=1"
    )


def _fetch_url(
    steam_url: str
) -> str:
    """
    Если задан STEAM_PROXY_URL,
    отправляет запрос через Cloudflare Worker.

    Например:

    STEAM_PROXY_URL=
    https://dextrade-steam-proxy.imrannnogerov.workers.dev

    превращается в:

    https://dextrade-steam-proxy.imrannnogerov.workers.dev/proxy?url=...
    """

    if not STEAM_PROXY_URL:
        return steam_url

    return (
        f"{STEAM_PROXY_URL}/proxy"
        f"?url={urllib.parse.quote(steam_url, safe='')}"
    )


PRICE_RE = re.compile(
    r"\$([\d,]+\.\d+)"
)


STICKER_RE = re.compile(
    r"stickers/([^/]+)/([a-zA-Z0-9_\-]+)\.[a-f0-9]{20,}\.png"
)


INSPECT_RE = re.compile(
    r'''href=["'](steam://[^"']*csgo_econ_action_preview[^"']*)["']'''
)


def _sticker_code_to_display(
    collection: str,
    code: str
) -> str:
    return f"{collection}:{code}"


async def fetch_all_listings(
    market_hash_name: str
) -> list[Listing]:

    listings: list[Listing] = []

    async with aiohttp.ClientSession(
        headers={
            "User-Agent":
                "Mozilla/5.0",

            "Accept":
                "application/json, text/plain, */*"
        }
    ) as session:

        start = 0
        total_count = None

        while (
            total_count is None
            or start < total_count
        ):

            steam_url = render_url(
                market_hash_name,
                start
            )

            request_url = _fetch_url(
                steam_url
            )

            async with session.get(
                request_url
            ) as resp:

                if resp.status == 429:
                    await asyncio.sleep(10)
                    continue

                resp.raise_for_status()

                content_type = (
                    resp.headers.get(
                        "Content-Type",
                        ""
                    )
                )

                if (
                    "application/json"
                    not in content_type.lower()
                ):
                    body = await resp.text()

                    snippet = (
                        body[:500]
                        .replace("\n", " ")
                        .strip()
                    )

                    raise RuntimeError(
                        "Steam вернул не JSON, "
                        f"а {content_type!r}. "
                        f"Начало ответа: {snippet!r}"
                    )

                data = await resp.json()

            total_count = data.get(
                "total_count",
                0
            )

            html = data.get(
                "results_html",
                ""
            )

            listings.extend(
                _parse_listings_html(html)
            )

            start += RENDER_COUNT

            await asyncio.sleep(
                REQUEST_DELAY
            )

    return listings


def _parse_listings_html(
    html: str
) -> list[Listing]:

    blocks = re.split(
        r'class="market_listing_row market_recent_listing_row',
        html
    )[1:]

    out = []

    for block in blocks:

        cut = len(block)

        for marker in (
            '"assets":{',
            '"listinginfo":{'
        ):
            idx = block.find(marker)

            if idx != -1:
                cut = min(
                    cut,
                    idx
                )

        b = block[:cut]

        price_m = PRICE_RE.search(b)

        if not price_m:
            continue

        price = float(
            price_m.group(1).replace(
                ",",
                ""
            )
        )

        stickers = [
            _sticker_code_to_display(
                coll,
                code
            )
            for coll, code
            in STICKER_RE.findall(b)
        ][:5]

        inspect_m = INSPECT_RE.search(b)

        inspect_link = (
            inspect_m.group(1)
            if inspect_m
            else None
        )

        out.append(
            Listing(
                price=price,
                stickers=stickers,
                inspect_link=inspect_link
            )
        )

    return out
