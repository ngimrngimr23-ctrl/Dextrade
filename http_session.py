"""
Один общий aiohttp.ClientSession на весь процесс.

Зачем это отдельный модуль. Раньше почти каждая функция, которой нужна была
сеть, открывала свою сессию через `async with aiohttp.ClientSession()`:
storage._redis_cmd, storage._redis_pipeline, pricing.get_sticker_prices,
steam_client.fetch_all_listings. Выглядит аккуратно, но означает буквально
следующее: на КАЖДЫЙ запрос заново поднимается TCP-соединение и заново
проводится TLS-хендшейк. Это два лишних round-trip'а до сервера, и ничего
полезного они не делают — соединение тут же выбрасывается.

Считаем на прогоне вотчлиста в 110 предметов: один запрос к Steam и шесть
к Upstash на предмет, итого ~770 хендшейков вместо двух живых соединений.
При RTT 100 мс это примерно две с половиной минуты, потраченные ровно ни на
что. С общей сессией соединение переиспользуется (keep-alive), и хендшейк
случается один раз на хост.

Побочно чинится ещё и DNS: TCPConnector кэширует резолв в пределах СВОЕЙ
сессии, а когда сессия одноразовая — кэш всегда пустой, и на каждый запрос
идёт поход к резолверу.

Сессию нельзя создать до старта event loop'а (aiohttp привязывает её к
текущему циклу), поэтому она создаётся лениво при первом обращении.
"""

import asyncio
import logging

import aiohttp

log = logging.getLogger("steam_bot.http_session")

# Тот же UA, что и раньше в pricing/bot — просто дефолт сессии. Любой запрос,
# которому нужен свой набор заголовков (например, AJAX-заголовки Steam),
# передаёт их в session.get/post и перекрывает этот дефолт.
DEFAULT_UA = "Mozilla/5.0"

# limit — общий потолок одновременных соединений на весь процесс.
# limit_per_host — на один хост. Сканер держит в полёте несколько запросов к
# steamcommunity.com (см. конвейер в bot._run_watchlist_scan), плюс параллельно
# идут походы в Upstash — это разные хосты, у каждого свой лимит.
#
# Значения подняты под большой пул прокси. Тонкость: для запросов через прокси
# aiohttp считает limit_per_host по ЦЕЛЕВОМУ хосту, а не по прокси — то есть
# все обращения к steamcommunity.com делят один лимит, сколько бы адресов ни
# было в пуле. При прежних 8 это молча срезало параллельность ровно там, где
# пул и должен был её дать.
_CONN_LIMIT = 100
_CONN_LIMIT_PER_HOST = 32
# Сколько держать соединение открытым без запросов. Upstash и Steam рвут
# простаивающие соединения сами, 75 секунд — с запасом меньше их таймаутов.
_KEEPALIVE = 75
_DNS_CACHE = 300

_session: aiohttp.ClientSession | None = None
_session_loop: asyncio.AbstractEventLoop | None = None


def get_session() -> aiohttp.ClientSession:
    """
    Общая сессия процесса. Вызывать только из корутины — нужен запущенный loop.

    Пересоздаём, если loop сменился: в тестах (asyncio.run на каждый кейс) сессия
    от прошлого цикла уже нерабочая, и без этой проверки всё падало бы на
    «Event loop is closed» в неочевидном месте.
    """
    global _session, _session_loop

    loop = asyncio.get_running_loop()
    if _session is not None and not _session.closed and _session_loop is loop:
        return _session

    if _session is not None and not _session.closed:
        log.warning("http_session: event loop сменился — создаю сессию заново")

    connector = aiohttp.TCPConnector(
        limit=_CONN_LIMIT,
        limit_per_host=_CONN_LIMIT_PER_HOST,
        ttl_dns_cache=_DNS_CACHE,
        keepalive_timeout=_KEEPALIVE,
    )
    _session = aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": DEFAULT_UA},
        # Куки НЕ храним между запросами — намеренно.
        #
        # Пока сессия была одноразовой, банка кук жила ровно один запрос, и
        # весь код построен на этом: заголовок Cookie собирается вручную под
        # каждый запрос (steam_client.steam_cookie_header). Особенно важно, что
        # steamLoginSecure сознательно НЕ уходит через прокси — один логин,
        # гуляющий по резидентным адресам, для Steam выглядит как кража сессии.
        #
        # С обычной банкой это правило тихо сломалось бы: Steam ставит куки
        # через Set-Cookie, aiohttp запомнил бы их и подмешал в следующий
        # запрос — в том числе в тот, что идёт через прокси. DummyCookieJar
        # всё выбрасывает, то есть поведение остаётся ровно прежним.
        cookie_jar=aiohttp.DummyCookieJar(),
    )
    _session_loop = loop
    return _session


async def close_session() -> None:
    """Закрыть общую сессию (при остановке бота). Повторный вызов безопасен."""
    global _session, _session_loop

    if _session is not None and not _session.closed:
        await _session.close()
    _session = None
    _session_loop = None
