"""
Работа со Steam Community Market: получение всех листингов по предмету
и разбор цены + стикеров на каждом лоте.

Steam отдаёт данные через публичный (не требующий логина) эндпоинт:
    https://steamcommunity.com/market/listings/730/<market_hash_name>/render/
        ?query=&start=<N>&count=100&country=US&language=english&currency=1&format=json

За один запрос отдаёт максимум 100 лотов, поэтому листаем через start.

Render банит IP датацентра, поэтому запросы к Steam уходят через Cloudflare
Worker-прокси (см. cloudflare-worker/), если задана переменная окружения
STEAM_PROXY_URL (например https://dextrade-steam-proxy.<sub>.workers.dev).
Без неё бот по-прежнему стучится в Steam напрямую — как раньше.
"""

import asyncio
import os
import re
import urllib.parse
from dataclasses import dataclass, field

import aiohttp


APP_ID = 730  # CS2 / CS:GO
RENDER_COUNT = 100  # максимум, который отдаёт Steam за раз
REQUEST_DELAY = 1.5  # пауза между запросами к Steam, чтобы не словить 429

STEAM_PROXY_URL = os.environ.get("STEAM_PROXY_URL", "").rstrip("/")


@dataclass
class Listing:
    price: float
    stickers: list[str] = field(default_factory=list)
    inspect_link: str | None = None


def market_hash_name_from_url(url: str) -> str:
    """
    Из ссылки на лот/список лотов вида
    https://steamcommunity.com/market/listings/730/AK-47%20%7C%20Slate%20%28Field-Tested%29
    достаём market_hash_name в исходном (не url-encoded) виде.
    """
    parsed = urllib.parse.urlparse(url)
    parts = parsed.path.rstrip("/").split("/")

    # .../market/listings/730/<name>
    name_encoded = parts[-1]

    return urllib.parse.unquote(name_encoded)


def render_url(market_hash_name: str, start: int) -> str:
    """
    Формируем Steam Market /render URL.

    ВАЖНО:
    format=json заставляет Steam вернуть JSON с results_html,
    total_count и т.д., а не HTML-страницу Steam.
    """
    encoded = urllib.parse.quote(market_hash_name, safe="")

    return (
        f"https://steamcommunity.com/market/listings/"
        f"{APP_ID}/{encoded}/render/"
        f"?query="
        f"&start={start}"
        f"&count={RENDER_COUNT}"
        f"&country=US"
        f"&language=english"
        f"&currency=1"
        f"&format=json"
    )


def _fetch_url(steam_url: str) -> str:
    """
    Если задан STEAM_PROXY_URL — идём в Steam через Cloudflare Worker,
    передавая настоящий steam_url как query-параметр.

    Иначе — прямой запрос в Steam.

    render_url() всегда возвращает исходную Steam-ссылку.
    Прокси используется только для HTTP-запроса.
    """
    if not STEAM_PROXY_URL:
        return steam_url

    return (
        f"{STEAM_PROXY_URL}/proxy"
        f"?url={urllib.parse.quote(steam_url, safe='')}"
    )


# Цена первого блока.
# Это цена продажи с учётом комиссии Steam.
PRICE_RE = re.compile(r"\$([\d,]+\.\d+)")


# Стикеры зашиты в src картинки:
# .../stickers/<collection>/<code>.<hash>.png
STICKER_RE = re.compile(
    r"stickers/([^/]+)/([a-zA-Z0-9_\-]+)\.[a-f0-9]{20,}\.png"
)


# Инспект-ссылка конкретного экземпляра предмета.
INSPECT_RE = re.compile(
    r'''href=["'](steam://[^"']*csgo_econ_action_preview[^"']*)["']'''
)


def _sticker_code_to_display(collection: str, code: str) -> str:
    """
    Грубое человекочитаемое представление кода стикера
    для дальнейшего поиска цены.

    Это НЕ точное имя в Steam Market.
    В pricing.py делается поиск по нескольким вариантам названия.
    """
    return f"{collection}:{code}"


async def fetch_all_listings(
    market_hash_name: str
) -> list[Listing]:
    """
    Тянем все страницы листингов для предмета
    и парсим цену + стикеры каждого лота.
    """

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

        while total_count is None or start < total_count:

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
                    # Rate limit — ждём и пробуем
                    # тот же start ещё раз.
                    await asyncio.sleep(10)
                    continue

                resp.raise_for_status()

                content_type = (
                    resp.headers.get(
                        "Content-Type",
                        ""
                    )
                )

                if "application/json" not in content_type.lower():
                    # Steam вместо JSON отдал HTML.
                    # Показываем начало ответа,
                    # чтобы понять причину.
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
    """
    results_html — HTML-фрагмент со всеми
    строками таблицы листингов.

    Режем по границам строк и вытаскиваем
    цену + стикеры каждой отдельно,
    чтобы стикеры одного лота
    не утекали в соседний.
    """

    blocks = re.split(
        r'class="market_listing_row market_recent_listing_row',
        html
    )[1:]

    out = []

    for block in blocks:

        # Обрезаем хвостовой мусор /
        # следующий служебный блок.
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

        # Цена
        price_m = PRICE_RE.search(b)

        if not price_m:
            continue

        price = float(
            price_m.group(1)
            .replace(",", "")
        )

        # Стикеры
        stickers = [
            _sticker_code_to_display(
                coll,
                code
            )
            for coll, code
            in STICKER_RE.findall(b)
        ][:5]

        # Inspect link
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
