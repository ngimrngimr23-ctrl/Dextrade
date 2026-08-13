"""
Работа со Steam Community Market: получение всех листингов по предмету
и разбор цены + стикеров на каждом лоте.

Steam отдаёт данные через публичный (не требующий логина) эндпоинт:
    https://steamcommunity.com/market/listings/730/<market_hash_name>/render/
        ?query=&start=<N>&count=100&country=US&language=english&currency=1

За один запрос отдаёт максимум 100 лотов, поэтому листаем через start.

Render банит IP датацентра, поэтому запросы к Steam уходят через Cloudflare
Worker-прокси (см. cloudflare-worker/), если задана переменная окружения
STEAM_PROXY_URL (например https://dextrade-steam-proxy.<sub>.workers.dev).
Без неё бот по-прежнему стучится в Steam напрямую — как раньше.

Опционально: куки реальной Steam-сессии (STEAM_LOGIN_SECURE и т.п., см.
steam_cookie_header()) — авторизованные запросы получают заметно более
мягкий рейт-лимит, чем анонимные (так делают открытые Steam-market боты
на GitHub, напр. woctezuma/steam-market). ВАЖНО: куки добавляются в
заголовок запроса, который уходит к STEAM_PROXY_URL, если он задан, — то
есть попадают к твоему Cloudflare Worker, а не напрямую в Steam. Чтобы
куки реально дошли до Steam, Worker должен сам переслать заголовок
Cookie на steamcommunity.com (правь cloudflare-worker/ отдельно — этот
файл проксирует только чистую логику, не код воркера).
"""

import asyncio
import logging
import os
import re
import urllib.parse
from dataclasses import dataclass, field

import aiohttp
import yarl

log = logging.getLogger("steam_bot.steam_client")

APP_ID = 730  # CS2 / CS:GO
RENDER_COUNT = 100  # максимум, который отдаёт Steam за раз
REQUEST_DELAY = 1.5  # пауза между запросами к Steam, чтобы не словить 429
DEFAULT_MAX_LISTINGS = 300  # автосканы (/scanfile, вотчлист) по умолчанию смотрят только столько самых дешёвых лотов
MAX_RENDER_RETRIES_429 = 5  # после стольких 429 подряд НА ОДНОЙ странице — сдаёмся с понятной ошибкой, не висим вечно

STEAM_PROXY_URL = os.environ.get("STEAM_PROXY_URL", "").rstrip("/")

# Куки реальной Steam-сессии — все опциональны, бот работает и без них
# (просто более анонимно и, судя по опыту, более подвержено рейт-лимитам).
# Достать: залогиниться в steamcommunity.com в браузере -> DevTools ->
# Application/Storage -> Cookies -> steamcommunity.com.
STEAM_LOGIN_SECURE = os.environ.get("STEAM_LOGIN_SECURE", "")
STEAM_SESSION_ID = os.environ.get("STEAM_SESSION_ID", "")
STEAM_BROWSER_ID = os.environ.get("STEAM_BROWSER_ID", "")


def steam_cookie_header() -> str:
    """
    Собирает Cookie-заголовок для запросов к Steam.

    bMarketOptOut=1 — не секретная, не привязанная к аккаунту кука "выход из
    бета-теста торговой площадки" (проверено вручную: без неё легаси-эндпоинт
    /render/ отдаёт HTML новой торговой площадки вместо JSON, даже у анонимной
    сессии без логина). Шлём её всегда, безусловно.
    """
    parts = ["bMarketOptOut=1"]
    if STEAM_LOGIN_SECURE:
        parts.append(f"steamLoginSecure={STEAM_LOGIN_SECURE}")
    if STEAM_SESSION_ID:
        parts.append(f"sessionid={STEAM_SESSION_ID}")
    if STEAM_BROWSER_ID:
        parts.append(f"browserid={STEAM_BROWSER_ID}")
    return "; ".join(parts)


@dataclass
class Listing:
    price: float
    stickers: list[str] = field(default_factory=list)  # человекочитаемые имена стикеров
    inspect_link: str | None = None  # steam://...csgo_econ_action_preview... — конкретно этот экземпляр


def market_hash_name_from_url(url: str) -> str:
    """
    Из ссылки на лот/список лотов вида
    https://steamcommunity.com/market/listings/730/AK-47%20%7C%20Slate%20%28Field-Tested%29
    достаём market_hash_name в исходном (не url-encoded) виде.
    """
    parsed = urllib.parse.urlparse(url)
    parts = parsed.path.rstrip("/").split("/")
    # .../market/listings/730/<n>
    name_encoded = parts[-1]
    return urllib.parse.unquote(name_encoded)


def render_url(market_hash_name: str, start: int) -> str:
    encoded = urllib.parse.quote(market_hash_name, safe="")
    return (
        f"https://steamcommunity.com/market/listings/{APP_ID}/{encoded}/render/"
        f"?query=&start={start}&count={RENDER_COUNT}&country=US&language=english&currency=1"
    )


def _build_request(steam_url: str) -> tuple[str, dict | None]:
    """
    Если задан STEAM_PROXY_URL — идём в Steam через Cloudflare Worker (не
    забаненный IP), передавая настоящий steam_url как query-параметр.
    Иначе (по умолчанию) — прямой запрос, как раньше.
    render_url() всегда возвращает исходную steam-ссылку — она же уходит в
    сообщения бота (open in browser и т.п.), прокси используется только
    для самого HTTP-запроса.

    ВАЖНО: steam_url НЕ кодируем вручную (urllib.parse.quote) — если
    передать его как уже закодированную строку внутри итогового URL,
    aiohttp/yarl при парсинге этой строки ЗАНОВО кодирует query-часть,
    что даёт двойное кодирование (%20 -> %2520 и т.п.) и ломает прокси.
    Вместо этого отдаём steam_url отдельным параметром через params= —
    так aiohttp кодирует его ровно один раз, корректно.
    """
    if not STEAM_PROXY_URL:
        # encoded=True — иначе yarl "нормализует" уже закодированный путь и
        # раскодирует %28/%29 обратно в (), т.е. в Steam уйдёт не байт-в-байт
        # тот же путь, что открывает браузер.
        return yarl.URL(steam_url, encoded=True), None
    return f"{STEAM_PROXY_URL}/proxy", {"url": steam_url}


# Регэксп ищет цену первого блока (это как раз "их" цена продажи с учётом комиссии)
PRICE_RE = re.compile(r'\$([\d,]+\.\d+)')

# Стикеры зашиты в src картинки: .../stickers/<collection>/<code>.<hash>.png
STICKER_RE = re.compile(r'stickers/([^/]+)/([a-zA-Z0-9_\-]+)\.[a-f0-9]{20,}\.png')

# Инспект-ссылка конкретного экземпляра предмета (float/паттерн/стикеры именно
# этого лота). Это НЕ ссылка на покупку — Steam не даёт публичных ссылок на
# покупку конкретного лота — но это единственный способ однозначно привязать
# ссылку к конкретному офферу, а не к предмету вообще.
INSPECT_RE = re.compile(r'''href=["'](steam://[^"']*csgo_econ_action_preview[^"']*)["']''')


def _sticker_code_to_display(collection: str, code: str) -> str:
    """
    Грубое человекочитаемое представление кода стикера для дальнейшего поиска цены.
    Это НЕ точное имя в Steam Market (Steam использует "Sticker | Name (Finish) | Event"),
    поэтому в pricing.py делаем поиск по нескольким вариантам названия.
    Здесь просто нормализуем то, что можем достать из имени файла.
    """
    return f"{collection}:{code}"


def item_page_url(market_hash_name: str) -> str:
    """Обычная (не /render/) страница предмета на Steam Market."""
    return f"https://steamcommunity.com/market/listings/{APP_ID}/{urllib.parse.quote(market_hash_name, safe='')}"


_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)
_UA_HINTS = {
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}


def _nav_headers() -> dict:
    """Заголовки Chrome при обычной навигации по ссылке (прогрев сессии)."""
    return {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        **_UA_HINTS,
    }


def _ajax_headers(market_hash_name: str) -> dict:
    """
    Заголовки AJAX-запроса к легаси-эндпоинту /render/ — именно так его дёргает
    штатный фронтенд Steam Market (он написан на Prototype.js, отсюда
    X-Prototype-Version и X-Requested-With).

    Прошлая версия просила 'Accept: text/html,...' — то есть буквально сама
    просила у Steam HTML вместо JSON; это было ошибкой, здесь исправлено.
    """
    return {
        "User-Agent": _UA,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "X-Prototype-Version": "1.7",
        "Referer": item_page_url(market_hash_name),
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        **_UA_HINTS,
    }


# Признаки того, что Steam отдал страницу НОВОЙ (бета) торговой площадки вместо
# JSON легаси-эндпоинта. Бета включается на аккаунт/сессию, и для неё /render/
# перестаёт быть JSON-эндпоинтом — никакими заголовками/IP/прокси это не лечится,
# нужно выйти из бета-теста на том аккаунте, чьи куки использует бот.
_BETA_PAGE_MARKERS = ("/ssr/", "DesktopUI", "<!DOCTYPE html")


async def fetch_all_listings(market_hash_name: str, max_listings: int | None = DEFAULT_MAX_LISTINGS) -> list[Listing]:
    """
    Тянем страницы листингов для предмета и парсим цену + стикеры каждого лота.

    Steam отдаёт лоты отсортированными от дешёвых к дорогим, поэтому первые
    max_listings уже покрывают все реалистичные офферы — дальше идут лоты
    дороже, которые почти никогда не выгодны. max_listings=None — собрать
    вообще всё (может быть много страниц на популярных предметах).
    """
    listings: list[Listing] = []
    cookie = steam_cookie_header()

    def _with_cookie(headers: dict) -> dict:
        return {**headers, "Cookie": cookie} if cookie else headers

    log.info(
        "fetch_all_listings: предмет %r, прокси=%s, куки Steam-сессии=%s",
        market_hash_name, STEAM_PROXY_URL or "нет (прямой запрос)", "есть" if cookie else "нет",
    )

    async with aiohttp.ClientSession() as session:
        # "Прогрев" сессии обычной навигацией на страницу предмета, как это
        # сделал бы браузер, прежде чем фронтенд дёрнет /render/ через AJAX.
        warmup_url, warmup_params = _build_request(item_page_url(market_hash_name))
        try:
            async with session.get(warmup_url, params=warmup_params, headers=_with_cookie(_nav_headers())) as warmup_resp:
                await warmup_resp.read()
        except Exception:
            log.warning("fetch_all_listings: не удалось прогреть сессию, продолжаю без прогрева", exc_info=True)

        start = 0
        total_count = None
        retries_429 = 0
        while total_count is None or start < total_count:
            if max_listings is not None and start >= max_listings:
                break
            steam_url = render_url(market_hash_name, start)
            final_url, params = _build_request(steam_url)
            async with session.get(final_url, params=params, headers=_with_cookie(_ajax_headers(market_hash_name))) as resp:
                if resp.status == 429:
                    # Rate limit — ждём и пробуем ещё раз тот же start. Раньше
                    # тут не было ни лога, ни предела попыток: если Steam
                    # стабильно отвечал 429, бот молча зависал в этом цикле
                    # НАВСЕГДА — снаружи выглядело как "бот не отвечает", хотя
                    # реально просто крутился здесь без единой строчки в лог.
                    retries_429 += 1
                    log.warning(
                        "fetch_all_listings: start=%s -> HTTP 429 (попытка %s/%s), жду 10 сек",
                        start, retries_429, MAX_RENDER_RETRIES_429,
                    )
                    if retries_429 >= MAX_RENDER_RETRIES_429:
                        raise RuntimeError(
                            f"Steam стабильно отвечает 429 (Too Many Requests) на start={start} "
                            f"после {MAX_RENDER_RETRIES_429} попыток — похоже, реальный рейт-лимит, "
                            f"не разовый сбой. Попробуй ещё раз чуть позже."
                        )
                    await asyncio.sleep(10)
                    continue
                retries_429 = 0  # сбрасываем счётчик после любой успешной страницы
                resp.raise_for_status()

                content_type = resp.headers.get("Content-Type", "")
                log.info(
                    "fetch_all_listings: start=%s -> HTTP %s, Content-Type=%r, итоговый URL=%s",
                    start, resp.status, content_type, resp.url,
                )
                if "application/json" not in content_type:
                    body = await resp.text()
                    snippet = body[:300].replace("\n", " ").strip()
                    if any(marker in body[:2000] for marker in _BETA_PAGE_MARKERS):
                        # Самый частый и неочевидный случай: аккаунт, чьи куки
                        # использует бот, включён в бета-тест новой торговой
                        # площадки. Для беты /render/ перестаёт быть JSON-API и
                        # отдаёт SSR-страницу — ни заголовки, ни прокси, ни смена
                        # IP это не исправят, помогает только выход из бета-теста.
                        raise RuntimeError(
                            "Steam отдал HTML-страницу новой (бета) торговой площадки вместо JSON. "
                            "Похоже, аккаунт, чьи куки заданы в STEAM_LOGIN_SECURE, включён в бета-тест "
                            "Steam Market — для беты легаси-эндпоинт /render/ больше не отдаёт JSON. "
                            "Зайди в браузере под ЭТИМ аккаунтом, нажми «Выйти из бета-теста торговой "
                            "площадки», заново экспортируй куки и обнови их на Render. "
                            f"Начало ответа: {snippet!r}"
                        )
                    raise RuntimeError(
                        f"Steam вернул не JSON, а {content_type!r}. "
                        f"Начало ответа: {snippet!r}"
                    )

                data = await resp.json()

            total_count = data.get("total_count", 0)
            html = data.get("results_html", "")
            listings.extend(_parse_listings_html(html))

            start += RENDER_COUNT
            await asyncio.sleep(REQUEST_DELAY)

    return listings


def _parse_listings_html(html: str) -> list[Listing]:
    """
    results_html — это HTML-фрагмент со всеми строками таблицы листингов.
    Режем по границам строк и вытаскиваем цену + стикеры каждой отдельно,
    чтобы стикеры одного лота не утекали в соседний.
    """
    blocks = re.split(r'class="market_listing_row market_recent_listing_row', html)[1:]
    out = []
    for block in blocks:
        # обрезаем на случай хвостового мусора/следующего листинга
        cut = len(block)
        for marker in ('"assets":{', '"listinginfo":{'):
            idx = block.find(marker)
            if idx != -1:
                cut = min(cut, idx)
        b = block[:cut]

        price_m = PRICE_RE.search(b)
        if not price_m:
            continue
        price = float(price_m.group(1).replace(",", ""))

        stickers = [
            _sticker_code_to_display(coll, code)
            for coll, code in STICKER_RE.findall(b)
        ][:5]  # максимум 5 слотов на оружии

        inspect_m = INSPECT_RE.search(b)
        inspect_link = inspect_m.group(1) if inspect_m else None

        out.append(Listing(price=price, stickers=stickers, inspect_link=inspect_link))

    return out

