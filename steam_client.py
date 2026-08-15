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
import time
import urllib.parse
from dataclasses import dataclass, field

import aiohttp
import yarl

log = logging.getLogger("steam_bot.steam_client")

APP_ID = 730  # CS2 / CS:GO
RENDER_COUNT = 100  # максимум, который отдаёт Steam за раз
DEFAULT_MAX_LISTINGS = 100  # автосканы (/scanfile, вотчлист) смотрят столько самых дешёвых лотов = ровно 1 запрос на предмет

# --- Рейт-лимит Steam ---------------------------------------------------
# История: 4 сек -> 12 сек (2026-08-14/15, тогда 429 ловил только
# priceoverview/market-search, решили что /render/ не подвержен) -> обратно
# 4 сек (не нашли доказательств, что /render/ вообще банится) -> и тут же
# опровергнуто: в логах 2026-08-15 18:35 при 4 сек ИМЕННО /render/ (листинги)
# словил 429 — но только на 15-м запросе подряд (~56 сек ровного темпа), а
# не на 3-4-м, как раньше было с priceoverview. Значит лимит /render/, скорее
# всего, не "N сек между запросами", а что-то вроде "не больше ~15 запросов
# за скользящую минуту" — короткие пробы по 3-4 запроса при 4 сек проходят,
# а длинный прогон вотчлиста (десятки предметов) всё равно упирается в
# потолок. Вернули паузу к 12 сек — она уже проверялась на реальном 429 у
# priceoverview и с тех пор нареканий на /render/ не было.
# Ключевое: 429 у Steam это НЕ "подожди секунду", а временный бан IP (по
# отзывам — вплоть до нескольких часов), который ПРОДЛЕВАЕТСЯ каждой новой
# попыткой во время бана. Поэтому ретраить 429 нельзя — этим бот только
# закапывает себя глубже. Вместо ретраев: сразу сдаёмся и ставим кулдаун
# для соответствующей области, в течение которого её не трогаем вообще.
MIN_REQUEST_INTERVAL = 12.0  # секунд между ЛЮБЫМИ двумя запросами к Steam (глобально на весь процесс)
COOLDOWN_AFTER_429_SECONDS = 30 * 60  # получили 429 -> не трогаем Steam столько времени
COOLDOWN_MAX_SECONDS = 6 * 60 * 60  # потолок при повторных 429 подряд (бан может быть длинным)

STEAM_PROXY_URL = os.environ.get("STEAM_PROXY_URL", "").rstrip("/")

# Куки реальной Steam-сессии — все опциональны, бот работает и без них
# (просто более анонимно и, судя по опыту, более подвержено рейт-лимитам).
# Достать: залогиниться в steamcommunity.com в браузере -> DevTools ->
# Application/Storage -> Cookies -> steamcommunity.com.
STEAM_LOGIN_SECURE = os.environ.get("STEAM_LOGIN_SECURE", "")
STEAM_SESSION_ID = os.environ.get("STEAM_SESSION_ID", "")
STEAM_BROWSER_ID = os.environ.get("STEAM_BROWSER_ID", "")


class SteamRateLimited(RuntimeError):
    """Steam ответил 429 либо мы сами на кулдауне после недавнего 429."""


# Кулдаун/счётчик 429 подряд — РАЗДЕЛЬНО по областям (scope), не один общий
# на весь процесс. Причина: 2026-08-14/15 дважды подряд (даже после подъёма
# паузы между запросами и часов отдыха) именно priceoverview/market-search
# (цена стикера, pricing.py) сразу ловил 429, тогда как /render/ (листинги,
# от которых зависит весь вотчлист/сканы) — ни разу. Похоже, у Steam это два
# независимых лимита, разной жёсткости. Раньше 429 на ЛЮБОМ из них ставил
# ОДИН общий кулдаун — бан на некритичной цене стикера останавливал заодно и
# критичный сбор листингов. Теперь у каждой области свой кулдаун:
#   "listings" — /render/ (steam_client.fetch_all_listings)
#   "pricing"  — priceoverview/market-search (pricing.py, только фоновый
#                prewarm.py их трогает — см. комментарий в pricing.py)
# throttle_steam_request() (пауза между запросами) — по-прежнему общий на обе
# области: пейсинг сам по себе не был причиной банов, разделять его незачем.
#
# cooldown_until — в epoch-секундах (time.time()), НЕ time.monotonic(): его
# значение сохраняется в постоянное хранилище (storage.py) и должно остаться
# осмысленным после рестарта процесса (Render передеплоивает бота на каждый
# git push, а monotonic() при рестарте обнуляется). Без этого при каждом
# рестарте бот "забывал" бы про ещё не снятый бан Steam, тут же пробовал
# снова и продлевал реальный бан. _last_request_at — просто пейсинг внутри
# одного запуска процесса, ему персистентность не нужна, оставляем monotonic.
_request_lock = asyncio.Lock()
_last_request_at = 0.0
_cooldowns: dict[str, dict] = {}  # scope -> {"cooldown_until": float, "consecutive_429": int}


def _cooldown_state(scope: str) -> dict:
    return _cooldowns.setdefault(scope, {"cooldown_until": 0.0, "consecutive_429": 0})


def steam_cooldown_remaining(scope: str = "listings") -> float:
    """Сколько секунд ещё нельзя трогать Steam в этой области (0, если можно)."""
    return max(0.0, _cooldown_state(scope)["cooldown_until"] - time.time())


async def _persist_cooldown(scope: str) -> None:
    from storage import set_steam_cooldown  # локальный импорт — storage не обязателен для остального модуля

    state = _cooldown_state(scope)
    try:
        await set_steam_cooldown(scope, state["cooldown_until"], state["consecutive_429"])
    except Exception:
        log.exception("не смог сохранить кулдаун Steam (%s) в постоянное хранилище", scope)


async def load_persisted_cooldown() -> None:
    """
    Восстанавливает кулдаун/счётчик 429 подряд (по каждой области) после
    рестарта процесса. Вызывать один раз при старте бота, до первого
    обращения к Steam.
    """
    from storage import get_steam_cooldown

    for scope in ("listings", "pricing"):
        try:
            persisted = await get_steam_cooldown(scope)
        except Exception:
            log.exception("не смог загрузить сохранённый кулдаун Steam (%s)", scope)
            continue
        if not persisted:
            continue
        state = _cooldown_state(scope)
        state["cooldown_until"] = persisted.get("cooldown_until", 0.0)
        state["consecutive_429"] = persisted.get("consecutive_429", 0)
        remaining = steam_cooldown_remaining(scope)
        if remaining > 0:
            log.warning(
                "Восстановлен кулдаун Steam (%s) после рестарта процесса: ещё %.0f мин (%s-й 429 подряд)",
                scope, remaining / 60, state["consecutive_429"],
            )


async def note_steam_429(scope: str = "listings") -> float:
    """
    Зафиксировать 429 в указанной области: ставит кулдаун только для неё
    (растёт вдвое при 429 подряд в этой же области), остальные области не
    трогает. Возвращает длину кулдауна.
    """
    state = _cooldown_state(scope)
    state["consecutive_429"] += 1
    seconds = min(COOLDOWN_AFTER_429_SECONDS * (2 ** (state["consecutive_429"] - 1)), COOLDOWN_MAX_SECONDS)
    state["cooldown_until"] = max(state["cooldown_until"], time.time() + seconds)
    log.warning(
        "Steam (%s) вернул 429 (%s-й подряд) — кулдаун на %.0f мин для этой области",
        scope, state["consecutive_429"], seconds / 60,
    )
    await _persist_cooldown(scope)
    return seconds


async def note_steam_ok(scope: str = "listings") -> None:
    """Успешный ответ в указанной области — сбрасываем счётчик 429 подряд для неё."""
    state = _cooldown_state(scope)
    if state["consecutive_429"] != 0:
        state["consecutive_429"] = 0
        await _persist_cooldown(scope)


def raise_if_cooling_down(scope: str = "listings") -> None:
    remaining = steam_cooldown_remaining(scope)
    if remaining > 0:
        raise SteamRateLimited(
            f"Steam недавно ответил 429 (это временный бан IP, который продлевается "
            f"от новых попыток), поэтому запросы к нему приостановлены ещё на "
            f"{remaining / 60:.0f} мин. Попробуй после этого."
        )


async def throttle_steam_request() -> None:
    """Держит паузу MIN_REQUEST_INTERVAL между любыми двумя запросами к Steam."""
    global _last_request_at
    async with _request_lock:
        wait = MIN_REQUEST_INTERVAL - (time.monotonic() - _last_request_at)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_request_at = time.monotonic()


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

    raise_if_cooling_down()

    async with aiohttp.ClientSession() as session:
        # Прогревочного захода на страницу предмета здесь больше нет: он был
        # добавлен под гипотезу "первый запрос холодной сессии отдаёт HTML",
        # которая не подтвердилась (настоящей причиной был bMarketOptOut=1).
        # А лишний запрос на каждый предмет — прямой путь к 429.
        start = 0
        total_count = None
        while total_count is None or start < total_count:
            if max_listings is not None and start >= max_listings:
                break
            steam_url = render_url(market_hash_name, start)
            final_url, params = _build_request(steam_url)
            await throttle_steam_request()
            async with session.get(final_url, params=params, headers=_with_cookie(_ajax_headers(market_hash_name))) as resp:
                if resp.status == 429:
                    # НЕ ретраим: у Steam 429 — это временный бан IP, который
                    # продлевается каждой новой попыткой. Ставим глобальный
                    # кулдаун и сразу выходим, чтобы не закапываться глубже.
                    seconds = await note_steam_429()
                    raise SteamRateLimited(
                        f"Steam ответил 429 (Too Many Requests) — это временный бан IP, "
                        f"который продлевается от новых попыток, поэтому не ретраю. "
                        f"Запросы к Steam приостановлены на {seconds / 60:.0f} мин."
                    )
                await note_steam_ok()
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
            # паузы между страницами держит throttle_steam_request() выше

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

