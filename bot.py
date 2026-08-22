"""
Telegram-бот.

Использование в чате с ботом:
    /scan <ссылка на предмет на Steam Market> [мин_стоимость_стикеров] [макс_наценка_%]

Пример:
    /scan https://steamcommunity.com/market/listings/730/AK-47%20%7C%20Slate%20%28Field-Tested%29 5 7

Бот сам сходит в Steam за листингами и посчитает офферы — прямой автоматический
запрос, без ручной передачи файлов (актуально с тех пор, как разобрались с
Market Beta и рейт-лимитами, см. steam_client.py). Если не указать числа —
по умолчанию 5 баксов и 7%.

Если автоматический запрос всё же не удался (например, IP временно на
кулдауне после 429) — резервный ручной путь:

    /scanfile <ссылка на предмет> [мин$] [макс%]

Бот пришлёт ссылку на страницу JSON — открываете её в своём браузере,
сохраняете как .json (Ctrl+S) и присылаете файл боту. Он спарсит, попросит
следующую страницу, если лотов больше 100, и в конце сам посчитает офферы.

Запуск:
    export TG_BOT_TOKEN=твой_токен_от_BotFather
    pip install -r requirements.txt
    python bot.py
"""

import asyncio
import datetime as dt
import hashlib
import html as html_module
import json
import logging
import os
import re
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import aiohttp
from telegram import BotCommand, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from steam_client import (
    MANUAL_REQUEST_INTERVAL,
    MIN_REQUEST_INTERVAL,
    fetch_all_listings,
    market_hash_name_from_url,
    render_url,
    _parse_listings_html,
    RENDER_COUNT,
    STEAM_PROXY_URL,
    SteamRateLimited,
    steam_cooldown_remaining,
    blocking_cooldown,
    load_persisted_cooldown,
)
from csgo_api import search_items as search_csgo_items
from pricing import (
    get_sticker_prices,
    ingest_manual_prices,
    clear_manual_prices,
    manual_prices_count,
    get_csgotrader_price_details,
    get_csgotrader_prices,
    get_steam_market_price_retrying,
    STEAM_POOL,
)
from analyzer import (
    find_offers, find_float_offers, find_arbitrage_offers,
    Offer, STREAK_THRESHOLD, STEAM_FEE_MULTIPLIER,
)
from cs_inspect import decode_inspect_link
from prewarm import prewarm_loop
from storage import (
    get_chat_defaults,
    set_chat_defaults,
    get_streak_markup,
    get_sticker_ratio,
    set_sticker_ratio,
    get_watch_gap,
    set_watch_gap,
    set_streak_markup,
    get_price_filter,
    set_price_filter,
    get_float_filter,
    set_float_filter,
    get_float_markup,
    set_float_markup,
    get_watchlist,
    set_watchlist,
    get_float_watchlist,
    set_float_watchlist,
    get_watch_paused,
    set_watch_paused,
    all_watchlist_chat_ids,
    was_offer_sent_recently,
    SENT_OFFER_TTL_SECONDS,
    get_market_settings,
    set_market_setting,
    get_steam_prices_batch,
    set_steam_price,
    get_extra_proxies,
    save_extra_proxies,
    mark_offer_sent,
    get_arb_settings,
    set_arb_setting,
    all_chat_ids_with_settings,
)
import csfloat_client
import market_prices
import proxy_pool
from csfloat_client import CSFloatError, CSFloatRateLimited

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("steam_bot")

# Состояние активных /scanfile-сессий по chat_id. В памяти процесса —
# сессия живёт, пока бот не перезапустится; для одного скана этого достаточно,
# долговременно ничего хранить тут не нужно.
_file_sessions: dict[int, dict] = {}

# Дефолты мин$ стикеров / макс% наценки по chat_id, задаются командой /setdefaults.
# Хранятся в storage.py (Upstash + локальный fallback — тот же паттерн, что и
# цены стикеров), поэтому переживают рестарт/редеплой бота, в отличие от
# обычного dict в памяти процесса.
DEFAULT_MIN_VALUE = 5.0
DEFAULT_MAX_MARKUP = 7.0

# /setfloatfilter: сколько лотов на предмет проверяем на флоат. Декодирование
# теперь полностью локальное (см. cs_inspect.py, PR с фиксом masked-формата) —
# лишних запросов не стоит, поэтому смысла ограничиваться дешёвыми лотами нет:
# берём все 100 (это и есть максимум, который Steam вообще отдаёт за раз).
FLOAT_CHECK_TOP_N = 100

# Ожидание выбора варианта после неоднозначного поиска по названию (несколько
# степеней износа и т.п.) — chat_id -> {"results": [...], "min_value":..., "max_markup":...}
_pending_search: dict[int, dict] = {}

# chat_id -> True, пока активен режим "жду прайс-лист стикеров" (включается
# командой /pricefile). Следующие документы от этого chat_id идут в
# ingest_manual_prices, а не в обычный парсинг листингов, пока не пришлют
# /scan или /scanfile (это выключает режим автоматически).
_pricefile_mode: set[int] = set()

# Вотчлист /watchadd: список предметов сканируется по расписанию джобой
# watchlist_scan_job, результат шлётся в чат только если нашлись офферы
# (без "ничего не подошло" на каждый тик — иначе бот спамил бы постоянно).
# Интервал автоскана вотчлиста больше не настраивается вручную. Сам прогон
# списка уже безопасно троттлится изнутри (throttle_steam_request — пауза
# MIN_REQUEST_INTERVAL секунд между ЛЮБЫМИ двумя запросами к Steam, см.
# steam_client.py), поэтому не нужно ЕЩЁ РАЗ закладывать это же время в
# интервал между прогонами — только дублировало бы задержку. Следующий
# прогон стартует через фиксированную паузу WATCH_GAP_MINUTES ПОСЛЕ
# завершения предыдущего (см. _schedule_watchlist_job/watchlist_scan_job),
# так что полный цикл для большого списка и так растягивается за счёт
# того, что сам прогон дольше — отдельно множить паузу на число предметов
# не нужно.
WATCH_GAP_MINUTES = 2.0  # пауза между концом одного прогона и началом следующего
WATCHLIST_JOB_PREFIX = "watchlist_scan_"
WATCHLIST_ITEM_DELAY_SECONDS = 3  # пауза между предметами внутри одного тика, чтобы не долбить Steam пачкой сразу

# --- Кросс-маркет арбитраж (CSFloat против Steam) ---------------------------
# Отдельная джоба, не связанная с вотчлистом: сканируется весь рынок CSFloat
# подряд, а не заранее заданный список предметов — ошибки в цене обычно как
# раз там, где ты бы не догадался посмотреть. К Steam при этом не ходим вообще:
# цена Steam приходит внутри ответа CSFloat (см. csfloat_client.py).
ARB_JOB_PREFIX = "arb_scan_"
# Интервал автоскана. Считается от объёма прогона и бюджета прокси — см.
# ARB_TARGET_LISTINGS. При 10 000 лотов и 7 адресах меньше ~9 минут ставить
# нельзя: квота кончится на середине часа.
ARB_INTERVAL_MINUTES = float(os.environ.get("ARB_INTERVAL_MINUTES", "10"))
# Сколько лотов просматривать за один прогон.
#
# 10 000 — это 200 запросов (потолок эндпоинта 50 лотов на страницу). Столько
# стало можно, потому что лимит CSFloat считается ПО АДРЕСУ: 200 запросов в час
# на каждый резидентный прокси.
#
# СЛЕДИ ЗА БЮДЖЕТОМ при изменении. Один прогон = target/50 запросов, и в час их
# уходит столько же, умноженное на 60/ARB_INTERVAL_MINUTES. Бюджет — 200 на
# каждый адрес в пуле. При 10 000 лотов и 7 адресах помещается 7 прогонов в
# час, то есть интервал не может быть меньше ~9 минут; при 5 минутах квота
# кончится на середине часа и остаток времени бот будет молчать.
ARB_TARGET_LISTINGS = int(os.environ.get("ARB_TARGET_LISTINGS", "1500"))

# Окно прайс-листа для ВТОРОГО мнения о цене. Только суточное, без отката на
# более старые: недельная и тем более месячная цена подтверждает не сегодняшнюю
# стоимость, а прошлую, и как проверка справки CSFloat не годится.
#
# На покрытие это больше не влияет: основной источник — reference.base_price,
# он есть у каждого лота. Отсутствие суточного окна означает лишь, что второго
# мнения по этому предмету нет.
ARB_PRICE_WINDOW = "last_24h"

# Чем сортировать выдачу CSFloat при скане.
#
# most_recent — свежевыставленные лоты. Так и надо, и вот почему.
#
# Сначала стояло highest_discount: казалось разумным просить у площадки сразу
# самое выгодное. На практике этот список почти не обновляется — за час в нём
# меняется 5-6 позиций, и скан раз за разом приносил одни и те же лоты. Причина
# простая: лот с настоящей скидкой живёт минуты, его выкупают. Значит всё, что
# ДОЛГО висит в топе по скидке, — это не выгода, а лоты, у которых сломан
# ориентир: справочная цена завышена, скидка бумажная. Сортировка по скидке
# гарантированно поднимает наверх именно их.
#
# Со свежими лотами наоборот: недооценённый лот попадает к нам в первые минуты
# после выставления, когда его ещё можно купить. Скидку считаем сами (см.
# _fill_steam_prices и find_arbitrage_offers), а не доверяем чужой сортировке.
ARB_SORT_BY = os.environ.get("ARB_SORT_BY", "most_recent")

# Максимальное расхождение между прайс-листом csgotrader и собственной
# справочной ценой CSFloat (reference.base_price), при котором цене ещё можно
# верить. Источники независимы, поэтому согласие — сильный довод, а расхождение
# вдвое означает, что один из них ошибается, и понять какой невозможно.
# 25% выбрано так, чтобы обычная разница в методике не мешала, а случай
# «$28.18 против $12» отсекался гарантированно.
ARB_SOURCE_GAP_PCT = 25.0

# Сколько кандидатов проверять живой ценой Steam за прогон.
#
# Раньше стояло 15 — под размер сообщения. Это была ошибка: отбор ранжирует
# кандидатов по ОЦЕНКЕ, а она может врать вдвое, поэтому кандидат под номером
# двадцать вполне мог оказаться лучшей находкой после проверки — и не
# проверялся никогда. Проверять надо всех кандидатов, а показывать лучших
# пятнадцать УЖЕ ПОСЛЕ проверки.
#
# Потолок всё равно нужен, потому что priceoverview отвечает по одному
# предмету, а Steam банит за темп. Восемьдесят при шести адресах — это меньше
# минуты, при одном адресе около пяти.
ARB_VERIFY_LIMIT = int(os.environ.get("ARB_VERIFY_LIMIT", "80"))

# Сколько находок с площадок проверять живой ценой Steam за раз.
MARKETS_VERIFY_LIMIT = int(os.environ.get("MARKETS_VERIFY_LIMIT", "25"))

# Потолок ЖИВЫХ запросов к Steam за один прогон — общий для обоих каналов.
#
# priceoverview лимитирован жёстко, и проверять каждого кандидата живьём
# оказалось нерабочей схемой: автоскан арбитража каждые 10 минут выжигал все
# адреса пула на полчаса, после чего /markets приходила к пустому пулу и
# молчала. Два канала дрались за один ресурс и проигрывали оба.
#
# Восемь запросов на прогон — это около 50 в час на весь бот, что Steam
# переносит спокойно. Остальное берётся из кэша, а он наполняется постепенно:
# кандидаты от прогона к прогону в основном одни и те же.
STEAM_LIVE_BUDGET = int(os.environ.get("STEAM_LIVE_BUDGET", "8"))

# Минимум продаж в Steam за сутки, чтобы находка считалась реализуемой.
# Без этого «выгода» бумажная: предмет, который не продаётся, не перепродать
# ни за какую цену. Объёма продаж в прайс-листах нет вовсе — только у Steam.
MARKETS_MIN_VOLUME = int(os.environ.get("MARKETS_MIN_VOLUME", "5"))

# Порог спреда по умолчанию, пока пользователь не задал свой через /setmarkets.
MARKETS_DEFAULT_DISCOUNT = float(os.environ.get("MARKETS_DEFAULT_DISCOUNT", "20"))

# Пропускать ли StatTrak-предметы в сканах вотчлиста (/scanall и автоскан).
# Отключается переменной окружения без правки кода: SKIP_STATTRAK=0.
SKIP_STATTRAK = os.environ.get("SKIP_STATTRAK", "1") not in ("0", "false", "no", "")
_arb_running: set[int] = set()

# chat_id -> идёт прогон вотчлиста прямо сейчас — защита от наложения тиков,
# если сканирование всех предметов не укладывается в заданный интервал.
_watchlist_running: set[int] = set()


async def _get_watch_interval(chat_id: int) -> float:
    saved = await get_watch_gap(chat_id)
    return saved if saved is not None else WATCH_GAP_MINUTES


def _schedule_watchlist_job(job_queue, chat_id: int, interval_minutes: float, delay_seconds: float | None = None) -> None:
    """
    Ставит СЛЕДУЮЩИЙ прогон вотчлиста одноразовой джобой (run_once), не
    периодической (run_repeating). watchlist_scan_job сам перепланирует
    следующий запуск через interval_minutes ПОСЛЕ своего завершения — так
    прогоны никогда не накладываются друг на друга, даже если сканирование
    всего списка не укладывается в заданный интервал (было видно в логах:
    "maximum number of running instances reached" на run_repeating).
    delay_seconds — необязательная задержка первого запуска, отличная от
    самого интервала (не используется сейчас, но на будущее).
    """
    for job in job_queue.get_jobs_by_name(f"{WATCHLIST_JOB_PREFIX}{chat_id}"):
        job.schedule_removal()
    job_queue.run_once(
        watchlist_scan_job,
        when=delay_seconds if delay_seconds is not None else interval_minutes * 60,
        data={"chat_id": chat_id},
        name=f"{WATCHLIST_JOB_PREFIX}{chat_id}",
    )


async def _get_defaults(chat_id: int) -> tuple[float, float]:
    d = await get_chat_defaults(chat_id)
    if d is None:
        return DEFAULT_MIN_VALUE, DEFAULT_MAX_MARKUP
    return d["min_value"], d["max_markup"]


def _split_args(args: list[str]) -> tuple[str, float | None, float | None]:
    """
    Разбирает аргументы команды вида <ссылка_или_название> [мин$] [макс%].
    Название предмета может состоять из нескольких слов ("AK-47 | Slate"),
    поэтому отделяем от хвоста не больше двух чисел, а всё остальное
    считаем ссылкой/названием.
    """
    remaining = list(args)
    trailing: list[float] = []
    while remaining and len(trailing) < 2:
        try:
            trailing.insert(0, float(remaining[-1]))
            remaining.pop()
        except ValueError:
            break
    query_or_url = " ".join(remaining)
    min_value = trailing[0] if len(trailing) >= 1 else None
    max_markup = trailing[1] if len(trailing) >= 2 else None
    return query_or_url, min_value, max_markup


async def _resolve_market_hash_name(
    update: Update, raw: str, mode: str, min_value: float, max_markup: float
) -> str | None:
    """
    Если raw — ссылка на Steam Market, достаёт market_hash_name из неё напрямую.
    Если это текст (на английском — русской базы у ByMykel/CSGO-API нет) —
    ищет предмет по открытой базе ByMykel/CSGO-API (не через Steam, так что
    не подвержено блокировке датацентровых IP). При нескольких подходящих вариантах (разный износ
    и т.п.) присылает пронумерованный список, сохраняет ожидание выбора
    (mode/min/max) и возвращает None — выбор придёт следующим сообщением
    и обработается в handle_text_selection.
    Возвращает market_hash_name, если однозначно определился, иначе None.
    """
    if raw.startswith("http://") or raw.startswith("https://"):
        try:
            return market_hash_name_from_url(raw)
        except Exception:
            await update.message.reply_text(
                "Не смог разобрать ссылку. Дай ссылку вида .../market/listings/730/<предмет>"
            )
            return None

    try:
        results = await search_csgo_items(raw)
    except Exception as e:
        await update.message.reply_text(
            f"Не смог найти «{raw}» в базе предметов: {e}\n\n"
            f"Попробуй прислать прямую ссылку на предмет вместо названия."
        )
        return None

    if not results:
        await update.message.reply_text(
            f"Ничего не нашёл по «{raw}». Поиск по названию работает только "
            f"на английском (например AK-47 | Slate) — база на русском не "
            f"публикуется. Либо пришли ссылку на предмет напрямую."
        )
        return None

    if len(results) == 1:
        return results[0]["hash_name"]

    # несколько вариантов — просим выбрать номер, продолжим в handle_text_selection
    chat_id = update.effective_chat.id
    _pending_search[chat_id] = {
        "results": results,
        "mode": mode,
        "min_value": min_value,
        "max_markup": max_markup,
    }
    lines = [f"Нашёл несколько вариантов по «{raw}», ответь номером:\n"]
    for i, r in enumerate(results[:8], start=1):
        lines.append(f"{i}. {r['name']}")
    await update.message.reply_text("\n".join(lines))
    return None


async def _proceed_scan(update: Update, market_hash_name: str, min_value: float, max_markup: float):
    await update.message.reply_text(f"Тяну лоты по «{market_hash_name}»… это может занять пару минут.")
    try:
        listings = await fetch_all_listings(market_hash_name)
    except Exception as e:
        log.exception("fetch_all_listings failed")
        if STEAM_PROXY_URL:
            await update.message.reply_text(
                f"Не смог получить листинги через прокси: {e}\n\n"
                f"Проверь, что воркер {STEAM_PROXY_URL} жив и доступен."
            )
        else:
            await update.message.reply_text(
                f"Не смог получить листинги: {e}\n\n"
                f"Steam блокирует запросы бота (частая история для облачных IP) — "
                f"попробуй /scanfile вместо /scan, там ты сам качаешь JSON из браузера."
            )
        return
    await _run_analysis(update, listings, min_value, max_markup, market_hash_name)


async def _proceed_scanfile(update: Update, market_hash_name: str, min_value: float, max_markup: float):
    chat_id = update.effective_chat.id
    _file_sessions[chat_id] = {
        "market_hash_name": market_hash_name,
        "min_value": min_value,
        "max_markup": max_markup,
        "listings": [],
        "next_start": 0,
        "total_count": None,
    }
    first_url = render_url(market_hash_name, 0)
    second_url = render_url(market_hash_name, RENDER_COUNT)
    await update.message.reply_text(
        f"Ок, собираю «{market_hash_name}» по файлам.\n\n"
        f"Steam всегда отдаёт максимум {RENDER_COUNT} лотов за раз, даже если попросить "
        f"больше — так что вот сразу 2 страницы (первые {RENDER_COUNT * 2} лотов), "
        f"чтобы не бегать за каждой по одной:\n\n"
        f"1. Открой в браузере:\n{first_url}\n"
        f"2. Сохрани страницу как .json (Ctrl+S) или PDF\n"
        f"3. Пришли файл сюда — результат посчитаю сразу же\n\n"
        f"И вторая страница:\n{second_url}\n"
        f"(сохрани и пришли так же, можно сразу оба файла подряд)\n\n"
        f"Дальше можно прислать ещё страниц позже, когда будет время — "
        f"офферы каждый раз пересчитаются заново."
    )


async def handle_text_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ответ номером после неоднозначного поиска по названию."""
    chat_id = update.effective_chat.id
    pending = _pending_search.get(chat_id)
    if not pending:
        return  # нет ожидающего выбора — это просто обычное сообщение, игнорируем

    text = (update.message.text or "").strip()
    try:
        idx = int(text)
    except ValueError:
        await update.message.reply_text("Ответь номером варианта из списка выше.")
        return

    results = pending["results"]
    if not (1 <= idx <= len(results)):
        await update.message.reply_text(f"Номер должен быть от 1 до {len(results)}.")
        return

    del _pending_search[chat_id]
    market_hash_name = results[idx - 1]["hash_name"]
    min_value = pending["min_value"]
    max_markup = pending["max_markup"]

    if pending["mode"] == "scan":
        await _proceed_scan(update, market_hash_name, min_value, max_markup)
    else:
        await _proceed_scanfile(update, market_hash_name, min_value, max_markup)


def _decode_floats(listings: list, limit: int | None = None) -> dict[str, float]:
    """
    Пытается раскодировать флоат ЛОКАЛЬНО (без единого сетевого запроса) —
    см. cs_inspect.py. С марта 2026 часть inspect-ссылок самодостаточна (флоат
    зашит в саму ссылку), часть ещё старого формата — для тех локально взять
    флоат неоткуда, просто пропускаем без похода куда-либо.
    limit=None — по всем переданным лотам (для уже отобранных офферов их
    единицы), иначе — только по первым limit самым дешёвым.

    Диагностика различает "нечего декодировать" (старый формат) и "декодер
    не справился" — раньше оба случая сливались в одно сообщение, из-за чего
    баг в самом декодере (пропущенное URL-декодирование) выглядел как
    "у Steam старый формат ссылок". Урок тот же, что с лимитами Steam:
    логировать факты, а не свою интерпретацию.
    """
    candidates = listings if limit is None else listings[:limit]
    to_check = [l for l in candidates if l.inspect_link]
    if not to_check:
        return {}

    result: dict[str, float] = {}
    reasons: dict[str, int] = {}
    sample_failed = None
    for listing in to_check:
        info, reason = decode_inspect_link(listing.inspect_link)
        reasons[reason] = reasons.get(reason, 0) + 1
        if info is not None:
            result[listing.inspect_link] = info["floatvalue"]
        elif sample_failed is None:
            # образец берём с ЛЮБОЙ неудачи, не только legacy — в прошлый раз
            # выборка только по legacy оставила нас без данных ровно тогда,
            # когда они были нужнее всего (все ошибки были decode_error)
            sample_failed = listing.inspect_link[:160]

    log.info(
        "cs_inspect: раскодировано %d из %d inspect-ссылок (по причинам: %s)",
        len(result), len(to_check), reasons,
    )
    if sample_failed and not result:
        log.info("cs_inspect: пример нераспознанной ссылки: %s", sample_failed)
    return result


async def _compute_offers(
    chat_id: int,
    listings,
    min_value: float,
    max_markup: float,
    *,
    check_stickers: bool = True,
    check_floats: bool = True,
    request_interval: float | None = None,
):
    """
    Общая логика для /scan, /scanfile и автоскана вотчлиста: цены стикеров -> офферы.

    check_stickers / check_floats — что именно искать в этом предмете. Автоскан
    держит два независимых списка (обычный вотчлист /watchadd и список под охоту
    за флоатом /floatadd), поэтому для конкретного предмета может быть нужно
    только одно из двух. Ручные /scan и /scanfile считают всё — там пользователь
    сам назвал предмет, значит интересно и то, и другое.
    """
    all_sticker_keys = {s for l in listings for s in l.stickers}
    sticker_prices = await get_sticker_prices(all_sticker_keys) if all_sticker_keys else {}
    streak_markup = await get_streak_markup(chat_id)
    sticker_ratio = await get_sticker_ratio(chat_id)
    min_price, max_price = await get_price_filter(chat_id)
    float_low, float_high = await get_float_filter(chat_id)
    float_markup = await get_float_markup(chat_id)

    # Флоат для ОХОТЫ (фильтр) считаем только когда фильтр задан и предмет за
    # этим следит — незачем декодировать все лоты, если результат никому не
    # нужен. А вот показать флоат на уже отобранных по стикерам офферах можно
    # всегда: декодирование локальное, сетевых запросов не делает, и офферов
    # обычно единицы.
    float_offers = []
    if check_floats and float_low is not None and float_high is not None and listings:
        top_floats = _decode_floats(listings, limit=FLOAT_CHECK_TOP_N)
        if top_floats:
            float_offers = find_float_offers(
                listings, top_floats, float_low, float_high, max_markup_pct=float_markup
            )

    offers = []
    if check_stickers:
        offers = find_offers(
            listings, sticker_prices, min_value, max_markup,
            streak_max_markup_pct=streak_markup, min_price=min_price, max_price=max_price,
            min_sticker_ratio=sticker_ratio,
        )
    if offers:
        matched_links = {o.inspect_link for o in offers if o.inspect_link}
        sticker_floats = _decode_floats([l for l in listings if l.inspect_link in matched_links])
        for offer in offers:
            if offer.inspect_link:
                offer.float_value = sticker_floats.get(offer.inspect_link)

    # Лот мог подойти сразу по обоим критериям — показываем его один раз,
    # как стикерный оффер (там больше данных, и флоат теперь тоже виден).
    seen_links = {o.inspect_link for o in offers if o.inspect_link}
    float_offers = [o for o in float_offers if o.inspect_link not in seen_links]

    return offers + float_offers, sticker_prices


def _format_offers_chunks(offers, sticker_prices, market_hash_name: str | None) -> list[str]:
    """Готовые куски текста (HTML) под лимит Telegram ~4096 символов — шлются по одному reply_text/send_message."""
    header = f"Найдено {len(offers)} офферов (цена голого скина ≈${offers[0].floor_price:.2f})"
    if market_hash_name:
        item_url = render_url(market_hash_name, 0).split("/render/")[0]
        header += f'\n<a href="{html_module.escape(item_url)}">Страница предмета на Steam Market</a>'
    header += (
        "\n\n<i>Инспект-ссылка открывает точно этот экземпляр предмета "
        "(float/паттерн/стикеры) в клиенте Steam — это не ссылка на покупку, "
        "прямых ссылок на конкретный лот Steam не даёт.</i>"
    )
    lines = [header]

    for o in offers[:20]:
        if o.found_by_float:
            # находка чисто по флоату — стикерная наценка тут не считалась вообще,
            # markup_pct — это наценка над самым дешёвым лотом предмета (см. /setfloatmarkup)
            markup_str = f" | наценка {o.markup_pct:.1f}%" if o.markup_pct != float("inf") else ""
            block = f"${o.price:.2f} | 🔍 редкий флоат {o.float_value:.5f}{markup_str}"
            if o.stickers:
                block += f"\n  <code>{html_module.escape(', '.join(o.stickers))}</code>"
        else:
            # все названия стикеров — в одном <code>, чтобы тап копировал их разом
            # (как раньше); цены стикеров — отдельной строкой ниже, вне <code>
            stickers_html = html_module.escape(", ".join(o.stickers))
            prices_str = ", ".join(f"${sticker_prices.get(s, 0.0):.2f}" for s in o.stickers)
            streak_tag = f" 🔥 стрик x{o.streak}" if o.streak >= STREAK_THRESHOLD else ""
            # флоат показываем справочно, если удалось раскодировать локально
            float_tag = f" | флоат {o.float_value:.5f}" if o.float_value is not None else ""
            block = (
                f"${o.price:.2f} | переплата над голым скином ${o.overpay:.2f} | "
                f"стикеры ≈${o.stickers_value:.2f} | наценка {o.markup_pct:.1f}%{streak_tag}{float_tag}\n"
                f"  <code>{stickers_html}</code>\n"
                f"  цены: {prices_str}"
            )
        if o.inspect_link:
            block += f'\n  <a href="{html_module.escape(o.inspect_link)}">Инспект этого лота</a>'
        lines.append(block)

    # Telegram режет сообщения по ~4096 символов — шлём частями, если офферов много
    chunks = []
    chunk = ""
    for line in lines:
        candidate = (chunk + "\n\n" + line) if chunk else line
        if len(candidate) > 3800:
            chunks.append(chunk)
            chunk = line
        else:
            chunk = candidate
    if chunk:
        chunks.append(chunk)
    return chunks


async def _run_analysis(
    update: Update, listings, min_value: float, max_markup: float, market_hash_name: str | None = None
):
    await update.message.reply_text(f"Собрано {len(listings)} лотов. Смотрю цены на стикеры…")

    offers, sticker_prices = await _compute_offers(update.effective_chat.id, listings, min_value, max_markup)

    if not offers:
        await update.message.reply_text(
            f"Ничего не подошло под критерии (стикеры от ${min_value:.2f}, наценка ≤{max_markup:.0f}%)."
        )
        return

    for chunk in _format_offers_chunks(offers, sticker_prices, market_hash_name):
        await update.message.reply_text(chunk, parse_mode="HTML", disable_web_page_preview=True)


async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    chat_id = update.effective_chat.id
    _pricefile_mode.discard(chat_id)
    def_min, def_max = await _get_defaults(chat_id)
    if not args:
        await update.message.reply_text(
            f"Формат: /scanfile <ссылка или название предмета> [мин$ стикеров={def_min:.0f}] [макс наценка%={def_max:.0f}]\n"
            f"Название — на английском: /scanfile AK-47 | Slate (Field-Tested)"
        )
        return

    query_or_url, parsed_min, parsed_max = _split_args(args)
    min_value = parsed_min if parsed_min is not None else def_min
    max_markup = parsed_max if parsed_max is not None else def_max

    market_hash_name = await _resolve_market_hash_name(update, query_or_url, "scanfile", min_value, max_markup)
    if market_hash_name is None:
        return  # либо ошибка уже сообщена, либо ждём выбора номера

    await _proceed_scanfile(update, market_hash_name, min_value, max_markup)


async def setdefaults(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    chat_id = update.effective_chat.id

    if not args:
        cur_min, cur_max = await _get_defaults(chat_id)
        await update.message.reply_text(
            f"Текущие значения по умолчанию: мин$ стикеров={cur_min:.2f}, макс наценка={cur_max:.0f}%.\n\n"
            f"Формат: /setdefaults <мин$ стикеров> <макс наценка%>\n"
            f"Пример: /setdefaults 10 5"
        )
        return

    if len(args) < 2:
        await update.message.reply_text(
            "Нужно оба значения. Формат: /setdefaults <мин$ стикеров> <макс наценка%>"
        )
        return

    try:
        min_value = float(args[0])
        max_markup = float(args[1])
    except ValueError:
        await update.message.reply_text("Оба значения должны быть числами. Пример: /setdefaults 10 5")
        return

    await set_chat_defaults(chat_id, min_value, max_markup)
    await update.message.reply_text(
        f"Ок, теперь по умолчанию: мин$ стикеров={min_value:.2f}, макс наценка={max_markup:.0f}%.\n"
        f"Действует для /scan и /scanfile без явных чисел, а также для файлов, "
        f"присланных без команды."
    )


async def setstreakmarkup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setstreakmarkup        — показать текущий порог наценки для стрик-лотов
    /setstreakmarkup <%>    — задать отдельный порог наценки для стрик-лотов
                               (от 4 подряд идущих одинаковых стикеров на оружии)
    """
    chat_id = update.effective_chat.id
    args = context.args

    if not args:
        cur = await get_streak_markup(chat_id)
        _, def_max = await _get_defaults(chat_id)
        if cur is None:
            await update.message.reply_text(
                f"Отдельный порог для стрик-лотов (от {STREAK_THRESHOLD} подряд одинаковых стикеров) "
                f"не задан — для них действует обычная наценка ({def_max:.0f}%, см. /setdefaults).\n\n"
                f"Формат: /setstreakmarkup <%>\nПример: /setstreakmarkup 15"
            )
        else:
            await update.message.reply_text(
                f"Порог наценки для стрик-лотов сейчас: {cur:.0f}%.\n"
                f"Формат: /setstreakmarkup <%>, чтобы поменять."
            )
        return

    try:
        pct = float(args[0])
    except ValueError:
        await update.message.reply_text("Наценка должна быть числом, напр. 15 или 20.")
        return

    await set_streak_markup(chat_id, pct)
    await update.message.reply_text(
        f"Ок, для стрик-лотов (от {STREAK_THRESHOLD} подряд одинаковых стикеров на оружии) "
        f"теперь отдельный порог наценки: {pct:.0f}%. Для всего остального действует обычный (/setdefaults)."
    )


async def setpricefilter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setpricefilter             — показать текущий фильтр цены лота
    /setpricefilter <мин> <макс> — задать диапазон итоговой цены лота (со стикерами)
    /setpricefilter off         — убрать фильтр
    """
    chat_id = update.effective_chat.id
    args = context.args

    if not args:
        min_price, max_price = await get_price_filter(chat_id)
        if min_price is None and max_price is None:
            await update.message.reply_text(
                "Фильтр цены лота не задан — показываются офферы любой цены.\n\n"
                "Формат: /setpricefilter <мин$> <макс$>\nПример: /setpricefilter 10 200\n"
                "/setpricefilter off — убрать фильтр"
            )
        else:
            lo = f"${min_price:.2f}" if min_price is not None else "без ограничения"
            hi = f"${max_price:.2f}" if max_price is not None else "без ограничения"
            await update.message.reply_text(f"Текущий фильтр цены лота: от {lo} до {hi}.")
        return

    if args[0].lower() == "off":
        await set_price_filter(chat_id, None, None)
        await update.message.reply_text("Фильтр цены лота убран — показываются офферы любой цены.")
        return

    if len(args) < 2:
        await update.message.reply_text(
            "Нужны оба значения. Формат: /setpricefilter <мин$> <макс$>, или /setpricefilter off"
        )
        return

    try:
        min_price = float(args[0])
        max_price = float(args[1])
    except ValueError:
        await update.message.reply_text("Оба значения должны быть числами. Пример: /setpricefilter 10 200")
        return

    if min_price > max_price:
        await update.message.reply_text("Минимум не может быть больше максимума.")
        return

    await set_price_filter(chat_id, min_price, max_price)
    await update.message.reply_text(
        f"Ок, теперь показываются офферы с итоговой ценой лота (со стикерами) "
        f"от ${min_price:.2f} до ${max_price:.2f}."
    )


async def setfloatfilter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setfloatfilter                    — показать текущий фильтр флоата
    /setfloatfilter <низкий> <высокий> — искать лоты с флоатом ≤низкий (топ для FN) или ≥высокий (топ для BS)
    /setfloatfilter off                — убрать фильтр
    Не связано со стикерами — отдельная находка, попадает в ту же подборку с пометкой 🔍.
    """
    chat_id = update.effective_chat.id
    args = context.args

    if not args:
        low, high = await get_float_filter(chat_id)
        if low is None or high is None:
            await update.message.reply_text(
                "Фильтр флоата не задан — флоат не проверяется вообще (лишних запросов не тратим).\n\n"
                "Формат: /setfloatfilter <низкий> <высокий>\nПример: /setfloatfilter 0.01 0.99 "
                "(поймает почти идеальный Factory New и предельно убитый Battle-Scarred)\n"
                f"Проверяются все лоты на предмет (до {FLOAT_CHECK_TOP_N}, сколько Steam вообще отдаёт "
                "за раз) — декодирование локальное, без сетевых запросов.\n"
                "/setfloatfilter off — убрать фильтр"
            )
        else:
            await update.message.reply_text(
                f"Текущий фильтр флоата: ≤{low:g} (топ для FN) или ≥{high:g} (топ для BS), "
                f"проверяются все лоты на предмет."
            )
        return

    if args[0].lower() == "off":
        await set_float_filter(chat_id, None, None)
        await update.message.reply_text("Фильтр флоата убран.")
        return

    if len(args) < 2:
        await update.message.reply_text(
            "Нужны оба значения. Формат: /setfloatfilter <низкий> <высокий>, или /setfloatfilter off"
        )
        return

    try:
        low = float(args[0])
        high = float(args[1])
    except ValueError:
        await update.message.reply_text("Оба значения должны быть числами от 0 до 1. Пример: /setfloatfilter 0.01 0.99")
        return

    if not (0.0 <= low <= 1.0 and 0.0 <= high <= 1.0):
        await update.message.reply_text("Флоат — число от 0 до 1.")
        return
    if low >= high:
        await update.message.reply_text("Низкий порог должен быть меньше высокого.")
        return

    await set_float_filter(chat_id, low, high)
    await update.message.reply_text(
        f"Ок, теперь ищу лоты с флоатом ≤{low:g} или ≥{high:g} среди всех лотов на предмет."
    )


async def setfloatmarkup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setfloatmarkup        — показать текущий порог
    /setfloatmarkup <макс%> — показывать флоат-находку, только если её цена не больше
                              чем на макс% выше самого дешёвого лота этого предмета
    /setfloatmarkup off    — без ограничения по цене (любая цена подходит)
    Отсекает случаи, когда продавец и так знает о редком флоате и уже заложил
    его в цену — оставляет только недооценённые находки. Требует /setfloatfilter.
    """
    chat_id = update.effective_chat.id
    args = context.args

    if not args:
        pct = await get_float_markup(chat_id)
        if pct is None:
            await update.message.reply_text(
                "Порог наценки для флоат-находок не задан — подходит любая цена.\n\n"
                "Формат: /setfloatmarkup <макс%>\nПример: /setfloatmarkup 15 — показывать находку, "
                "только если её цена не больше чем на 15% выше самого дешёвого лота этого предмета "
                "(иначе продавец уже знает о редком флоате и заложил его в цену).\n"
                "/setfloatmarkup off — убрать ограничение"
            )
        else:
            await update.message.reply_text(f"Текущий порог наценки для флоат-находок: ≤{pct:g}%.")
        return

    if args[0].lower() == "off":
        await set_float_markup(chat_id, None)
        await update.message.reply_text("Ограничение по цене для флоат-находок убрано.")
        return

    try:
        pct = float(args[0])
    except ValueError:
        await update.message.reply_text("Значение должно быть числом. Пример: /setfloatmarkup 15")
        return

    if pct < 0:
        await update.message.reply_text("Наценка не может быть отрицательной.")
        return

    await set_float_markup(chat_id, pct)
    await update.message.reply_text(
        f"Ок, теперь флоат-находки показываются, только если их цена не больше чем на {pct:g}% "
        f"выше самого дешёвого лота этого предмета."
    )


async def scanfile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    chat_id = update.effective_chat.id
    _pricefile_mode.discard(chat_id)
    def_min, def_max = await _get_defaults(chat_id)
    if not args:
        await update.message.reply_text(
            f"Формат: /scan <ссылка или название предмета> [мин$ стикеров={def_min:.0f}] [макс наценка%={def_max:.0f}]\n"
            f"Название — на английском: /scan AK-47 | Slate (Field-Tested)"
        )
        return

    query_or_url, parsed_min, parsed_max = _split_args(args)
    min_value = parsed_min if parsed_min is not None else def_min
    max_markup = parsed_max if parsed_max is not None else def_max

    market_hash_name = await _resolve_market_hash_name(update, query_or_url, "scan", min_value, max_markup)
    if market_hash_name is None:
        return  # либо ошибка уже сообщена, либо ждём выбора номера

    await _proceed_scan(update, market_hash_name, min_value, max_markup)


_SPECIAL_VARIANT_WORDS = ("stattrak", "souvenir")

# Полное название или сокращение степени износа -> каноническое название в
# скобках, как в market_hash_name. Нужно для "хвостовой" степени в /watchadd:
# если последним элементом через запятую указать степень (полностью или
# сокращённо), она применится ко всем предметам списка без своей степени.
_WEAR_ALIASES = {
    "factory new": "Factory New", "fn": "Factory New",
    "minimal wear": "Minimal Wear", "mw": "Minimal Wear",
    "field-tested": "Field-Tested", "field tested": "Field-Tested", "ft": "Field-Tested",
    "well-worn": "Well-Worn", "well worn": "Well-Worn", "ww": "Well-Worn",
    "battle-scarred": "Battle-Scarred", "battle scarred": "Battle-Scarred", "bs": "Battle-Scarred",
}


async def _resolve_for_watchlist(raw: str) -> tuple[list[str], str | None]:
    """
    Резолвит один пункт /watchadd в список market_hash_name (без интерактивного
    уточнения — батч-режим не может ждать выбор номера на каждый вариант).

    Ссылка -> ровно один результат (степень износа уже в самой ссылке).
    Название СО степенью износа в скобках, напр. "AK-47 | Slate (Field-Tested)"
    -> точное совпадение, один результат.
    Название БЕЗ степени износа, напр. "AK-47 | Redline" -> сразу ВСЕ найденные
    степени износа этого скина (кроме StatTrak/Souvenir, если их не просили
    явно) — не нужно перечислять их вручную по одной.

    Возвращает (список_market_hash_name, warning) — список пуст при ошибке.
    """
    if raw.startswith("http://") or raw.startswith("https://"):
        try:
            return [market_hash_name_from_url(raw)], None
        except Exception:
            return [], f"«{raw}»: не смог разобрать ссылку"

    try:
        results = await search_csgo_items(raw, count=10)
    except Exception as e:
        return [], f"«{raw}»: ошибка поиска ({e})"

    if not results:
        return [], f"«{raw}»: не нашёл в базе предметов"

    if "(" in raw:  # степень износа уже указана явно — точное совпадение, без разброса
        return [results[0]["hash_name"]], None

    wants_special = any(w in raw.lower() for w in _SPECIAL_VARIANT_WORDS)
    if not wants_special:
        filtered = [r for r in results if not any(w in r["hash_name"].lower() for w in _SPECIAL_VARIANT_WORDS)]
        if filtered:
            results = filtered

    return [r["hash_name"] for r in results], None


async def watchadd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /watchadd <предмет1>, <предмет2>, ... — добавить один или сразу несколько
    предметов в вотчлист (ссылка или название, через запятую). Последним
    элементом можно отдельно указать степень износа (FN/MW/FT/WW/BS или
    полностью) — применится ко всем предметам списка без своей степени.
    """
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text(
            "Формат: /watchadd <предмет1>, <предмет2>, ...\n"
            "Можно ссылку или название (на английском), через запятую для нескольких сразу.\n"
            "Пример: /watchadd AK-47 | Slate (Field-Tested), M4A4 | Asiimov (Field-Tested)\n\n"
            "Степень износа можно не расписывать на каждый предмет, а указать один раз "
            "последним элементом — подойдёт и сокращение (FN/MW/FT/WW/BS):\n"
            "/watchadd AK-47 | Redline, AWP | Redline, Field-Tested"
        )
        return

    parts = [p.strip() for p in " ".join(context.args).split(",") if p.strip()]

    global_wear = None
    if len(parts) > 1 and parts[-1].lower() in _WEAR_ALIASES:
        global_wear = _WEAR_ALIASES[parts[-1].lower()]
        parts = parts[:-1]

    current = await get_watchlist(chat_id)

    added, warnings, skipped = [], [], []
    for part in parts:
        if global_wear and "(" not in part:
            part = f"{part} ({global_wear})"
        names, warning = await _resolve_for_watchlist(part)
        if not names:
            skipped.append(warning)
            continue
        if warning:
            warnings.append(warning)
        for name in names:
            if name in current:
                skipped.append(f"«{name}»: уже в списке")
                continue
            current.append(name)
            added.append(name)

    await set_watchlist(chat_id, current)
    if context.application.job_queue and not context.application.job_queue.get_jobs_by_name(f"{WATCHLIST_JOB_PREFIX}{chat_id}"):
        _schedule_watchlist_job(context.application.job_queue, chat_id, await _get_watch_interval(chat_id))

    lines = []
    if global_wear:
        lines.append(f"Степень износа «{global_wear}» применена ко всем предметам списка без своей степени.")
    if added:
        lines.append("Добавлено:\n" + "\n".join(f"• {a}" for a in added))
    if warnings:
        lines.append("Уточни, если не то:\n" + "\n".join(f"• {w}" for w in warnings))
    if skipped:
        lines.append("Пропущено:\n" + "\n".join(f"• {s}" for s in skipped))
    interval = await _get_watch_interval(chat_id)
    lines.append(f"Всего в списке: {len(current)}. Следующий прогон — через {interval:g} мин после конца текущего/предыдущего.")
    await update.message.reply_text("\n\n".join(lines))


async def watchdel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/watchdel <номер из /watchlist или точное название> — убрать предмет из вотчлиста."""
    chat_id = update.effective_chat.id
    current = await get_watchlist(chat_id)

    if not context.args:
        await update.message.reply_text(
            "Формат: /watchdel <номер из /watchlist или точное название>\n"
            "Пример: /watchdel 2"
        )
        return
    if not current:
        await update.message.reply_text("Вотчлист пуст.")
        return

    arg = " ".join(context.args).strip()
    if arg.isdigit():
        idx = int(arg) - 1
        if not (0 <= idx < len(current)):
            await update.message.reply_text(f"Номер должен быть от 1 до {len(current)}.")
            return
        removed = current.pop(idx)
    else:
        match = next((x for x in current if x.lower() == arg.lower()), None)
        if match is None:
            await update.message.reply_text(f"«{arg}» не найден в списке. Точное название смотри в /watchlist.")
            return
        current.remove(match)
        removed = match

    await set_watchlist(chat_id, current)
    await update.message.reply_text(f"Удалено: {removed}\nОсталось в списке: {len(current)}.")


async def watchclear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/watchclear — полностью очистить вотчлист (интервал и пауза не трогаются)."""
    chat_id = update.effective_chat.id
    current = await get_watchlist(chat_id)
    if not current:
        await update.message.reply_text("Вотчлист уже пуст.")
        return

    await set_watchlist(chat_id, [])
    await update.message.reply_text(f"Вотчлист очищен — удалено {len(current)} предмет(ов).")


def _chunk_lines(lines: list[str], limit: int = 3800, sep: str = "\n") -> list[str]:
    """
    Склеивает строки через sep в куски не длиннее limit — Telegram обрывает
    сообщения длиннее 4096 символов ошибкой "Message is too long", а не сам
    режет их на части. Нужно везде, где число строк растёт вместе со списком
    пользователя (вотчлист, список охоты за флоатом) — на фиксированном
    маленьком числе строк лимит не грозит, но проверено на живом инциденте:
    при 110 предметах в вотчлисте /watchlist падал именно с этой ошибкой.
    """
    chunks, chunk = [], ""
    for line in lines:
        candidate = (chunk + sep + line) if chunk else line
        if len(candidate) > limit:
            if chunk:
                chunks.append(chunk)
            chunk = line
        else:
            chunk = candidate
    if chunk:
        chunks.append(chunk)
    return chunks


async def watchlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/watchlist — показать текущий вотчлист и интервал автоскана."""
    chat_id = update.effective_chat.id
    items = await get_watchlist(chat_id)
    interval = await _get_watch_interval(chat_id)

    if not items:
        await update.message.reply_text(
            "Вотчлист пуст. Добавь предметы: /watchadd <предмет1>, <предмет2>, ...\n"
            f"Пауза между прогонами: {interval:g} мин после конца предыдущего."
        )
        return

    lines = [f"📋 Вотчлист ({len(items)}), следующий прогон — через {interval:g} мин после конца текущего/предыдущего:"]
    for i, name in enumerate(items, start=1):
        lines.append(f"{i}. {name}")
    for chunk in _chunk_lines(lines):
        await update.message.reply_text(chunk)


# --- Отдельный список под охоту за редким флоатом (/floatadd) ---------------
# Намеренно не смешан с обычным вотчлистом: флоат имеет смысл искать на
# конкретных скинах, а не гонять по всему списку. Предмет может быть и там,
# и там — тогда в прогоне он тянется из Steam один раз и проверяется по обоим
# критериям сразу (см. _run_watchlist_scan).

async def floatadd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /floatadd <предмет1>, <предмет2>, ... — добавить предметы в список охоты за
    редким флоатом. Формат тот же, что у /watchadd: ссылка или название через
    запятую, последним элементом можно задать степень износа для всего списка.
    """
    chat_id = update.effective_chat.id
    if not context.args:
        low, high = await get_float_filter(chat_id)
        hint = (
            "\n\n⚠️ Порог флоата пока не задан — без него охота не идёт. "
            "Задай: /setfloatfilter 0.01 0.99"
            if low is None or high is None else ""
        )
        await update.message.reply_text(
            "Формат: /floatadd <предмет1>, <предмет2>, ...\n"
            "Можно ссылку или название (на английском), через запятую для нескольких сразу.\n"
            "Пример: /floatadd AK-47 | Redline (Field-Tested)\n\n"
            "Степень износа можно указать один раз последним элементом (FN/MW/FT/WW/BS):\n"
            "/floatadd AK-47 | Redline, AWP | Asiimov, Factory New\n\n"
            "Это ОТДЕЛЬНЫЙ список от /watchadd — флоат ищется только по нему." + hint
        )
        return

    parts = [p.strip() for p in " ".join(context.args).split(",") if p.strip()]

    global_wear = None
    if len(parts) > 1 and parts[-1].lower() in _WEAR_ALIASES:
        global_wear = _WEAR_ALIASES[parts[-1].lower()]
        parts = parts[:-1]

    current = await get_float_watchlist(chat_id)

    added, warnings, skipped = [], [], []
    for part in parts:
        if global_wear and "(" not in part:
            part = f"{part} ({global_wear})"
        names, warning = await _resolve_for_watchlist(part)
        if not names:
            skipped.append(warning)
            continue
        if warning:
            warnings.append(warning)
        for name in names:
            if name in current:
                skipped.append(f"«{name}»: уже в списке флоата")
                continue
            current.append(name)
            added.append(name)

    await set_float_watchlist(chat_id, current)
    if context.application.job_queue and not context.application.job_queue.get_jobs_by_name(f"{WATCHLIST_JOB_PREFIX}{chat_id}"):
        _schedule_watchlist_job(context.application.job_queue, chat_id, await _get_watch_interval(chat_id))

    lines = []
    if global_wear:
        lines.append(f"Степень износа «{global_wear}» применена ко всем предметам списка без своей степени.")
    if added:
        lines.append("Добавлено в охоту за флоатом:\n" + "\n".join(f"• {a}" for a in added))
    if warnings:
        lines.append("Уточни, если не то:\n" + "\n".join(f"• {w}" for w in warnings))
    if skipped:
        lines.append("Пропущено:\n" + "\n".join(f"• {s}" for s in skipped))

    low, high = await get_float_filter(chat_id)
    if low is None or high is None:
        lines.append("⚠️ Порог флоата не задан — охота не пойдёт. Задай: /setfloatfilter 0.01 0.99")
    lines.append(f"Всего в списке флоата: {len(current)}.")
    await update.message.reply_text("\n\n".join(lines))


async def floatdel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/floatdel <номер из /floatlist или точное название> — убрать предмет из списка флоата."""
    chat_id = update.effective_chat.id
    current = await get_float_watchlist(chat_id)

    if not context.args:
        await update.message.reply_text(
            "Формат: /floatdel <номер из /floatlist или точное название>\nПример: /floatdel 2"
        )
        return
    if not current:
        await update.message.reply_text("Список флоата пуст.")
        return

    arg = " ".join(context.args).strip()
    if arg.isdigit():
        idx = int(arg) - 1
        if not (0 <= idx < len(current)):
            await update.message.reply_text(f"Номер должен быть от 1 до {len(current)}.")
            return
        removed = current.pop(idx)
    else:
        match = next((x for x in current if x.lower() == arg.lower()), None)
        if match is None:
            await update.message.reply_text(f"«{arg}» не найден. Точное название смотри в /floatlist.")
            return
        current.remove(match)
        removed = match

    await set_float_watchlist(chat_id, current)
    await update.message.reply_text(f"Удалено из охоты за флоатом: {removed}\nОсталось: {len(current)}.")


async def floatclear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/floatclear — полностью очистить список охоты за флоатом."""
    chat_id = update.effective_chat.id
    current = await get_float_watchlist(chat_id)
    if not current:
        await update.message.reply_text("Список флоата уже пуст.")
        return

    await set_float_watchlist(chat_id, [])
    await update.message.reply_text(f"Список флоата очищен — удалено {len(current)} предмет(ов).")


async def floatlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/floatlist — показать список предметов, по которым ищется редкий флоат."""
    chat_id = update.effective_chat.id
    items = await get_float_watchlist(chat_id)
    low, high = await get_float_filter(chat_id)
    markup = await get_float_markup(chat_id)

    if not items:
        await update.message.reply_text(
            "Список охоты за флоатом пуст — флоат сейчас не ищется ни по одному предмету.\n"
            "Добавить: /floatadd <предмет>"
        )
        return

    if low is None or high is None:
        threshold = "⚠️ порог не задан (/setfloatfilter 0.01 0.99) — охота не идёт"
    else:
        threshold = f"флоат ≤{low:g} или ≥{high:g}"
        if markup is not None:
            threshold += f", наценка ≤{markup:g}%"

    lines = [f"🔍 Охота за флоатом ({len(items)}), условие: {threshold}"]
    for i, name in enumerate(items, start=1):
        lines.append(f"{i}. {name}")
    for chunk in _chunk_lines(lines):
        await update.message.reply_text(chunk)


def _offer_key(market_hash_name: str, offer: Offer) -> str:
    """
    Стабильный ключ конкретного лота — по inspect-ссылке (уникальна для
    каждого экземпляра предмета в Steam), либо, если её нет, по сочетанию
    название+цена+стикеры. Нужен, чтобы не слать один и тот же оффер
    повторно в течение SENT_OFFER_TTL_SECONDS (см. storage.py).
    """
    basis = offer.inspect_link or f"{market_hash_name}|{offer.price}|{','.join(offer.stickers)}"
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()


async def _watchlist_scan_item(
    bot, chat_id: int, market_hash_name: str, min_value: float, max_markup: float,
    *, check_stickers: bool = True, check_floats: bool = True,
) -> bool:
    """
    Возвращает True, если нашлись НОВЫЕ офферы (не присылавшиеся этому чату
    за последние 5 часов) и сообщение реально ушло в чат.
    check_stickers/check_floats — что искать: предмет мог попасть в прогон из
    обычного вотчлиста, из списка охоты за флоатом, или сразу из обоих.
    SteamRateLimited пробрасывается наверх — прогон должен остановиться целиком,
    а не продолжать долбить Steam остальными предметами во время бана.
    """
    try:
        listings = await fetch_all_listings(market_hash_name)
    except SteamRateLimited:
        raise
    except Exception as e:
        log.warning("watchlist: %s (chat_id=%s): %s", market_hash_name, chat_id, e)
        return False

    offers, sticker_prices = await _compute_offers(
        chat_id, listings, min_value, max_markup,
        check_stickers=check_stickers, check_floats=check_floats,
    )
    if not offers:
        return False  # автоскан молчит, если нечего показать — иначе спамил бы каждый тик

    keys = [_offer_key(market_hash_name, o) for o in offers]
    new_offers = [
        o for o, key in zip(offers, keys) if not await was_offer_sent_recently(chat_id, key)
    ]
    if not new_offers:
        return False  # всё это уже присылали этому чату за последние 5 часов

    chunks = _format_offers_chunks(new_offers, sticker_prices, market_hash_name)
    chunks[0] = f"🔔 {html_module.escape(market_hash_name)}\n\n{chunks[0]}"
    for chunk in chunks:
        await bot.send_message(chat_id=chat_id, text=chunk, parse_mode="HTML", disable_web_page_preview=True)
    for o in new_offers:
        await mark_offer_sent(chat_id, _offer_key(market_hash_name, o))
    return True


async def _run_watchlist_scan(
    bot, chat_id: int, request_interval: float | None = None
) -> bool | None:
    """
    Прогоняет весь вотчлист чата разом — общая логика для джобы по расписанию
    и команды /scanall. Возвращает True/False (нашлось ли хоть что-то) или
    None, если прогон не запустился (пустой список / уже идёт другой прогон).
    """
    if chat_id in _watchlist_running:
        log.info("watchlist: прогон для chat_id=%s уже идёт, пропускаю повторный запуск", chat_id)
        return None

    sticker_items, skipped_st = _drop_stattrak(await get_watchlist(chat_id))
    float_items, skipped_float = _drop_stattrak(await get_float_watchlist(chat_id))
    if skipped_st or skipped_float:
        log.info(
            "watchlist: chat_id=%s пропускаю StatTrak — %d из вотчлиста, %d из флоат-списка",
            chat_id, skipped_st, skipped_float,
        )
    if not sticker_items and not float_items:
        return None

    # Два независимых списка, но прогон один: предмет, попавший в оба, тянем из
    # Steam ОДИН раз и проверяем сразу по обоим критериям — иначе платили бы
    # двумя запросами за одни и те же лоты. Порядок: сначала обычный вотчлист,
    # затем предметы, которые нужны только под флоат.
    float_set = set(float_items)
    scan_plan = [(name, True, name in float_set) for name in sticker_items]
    sticker_set = set(sticker_items)
    scan_plan += [(name, False, True) for name in float_items if name not in sticker_set]

    _watchlist_running.add(chat_id)
    try:
        min_value, max_markup = await _get_defaults(chat_id)
        found_any = False
        for market_hash_name, check_stickers, check_floats in scan_plan:
            try:
                found = await _watchlist_scan_item(
                    bot, chat_id, market_hash_name, min_value, max_markup,
                    check_stickers=check_stickers, check_floats=check_floats,
                    request_interval=request_interval,
                )
            except SteamRateLimited as e:
                # Влетели в рейт-лимит Steam. Останавливаем весь прогон: каждая
                # следующая попытка во время бана только продлевает его.
                log.warning("watchlist: прогон chat_id=%s остановлен из-за рейт-лимита: %s", chat_id, e)
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"⏸ Автоскан остановлен на «{market_hash_name}»: {e}",
                )
                return found_any
            found_any = found_any or found
        return found_any
    finally:
        _watchlist_running.discard(chat_id)


async def watchlist_scan_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data["chat_id"]
    try:
        if await get_watch_paused(chat_id):
            log.info("watchlist: chat_id=%s на паузе (/watchpause), пропускаю прогон", chat_id)
            return
        cooldown = blocking_cooldown()
        if cooldown > 0:
            log.info(
                "watchlist: chat_id=%s пропускает прогон — кулдаун Steam ещё %.0f мин", chat_id, cooldown / 60
            )
            return
        await _run_watchlist_scan(context.bot, chat_id)
    finally:
        # Планируем следующий прогон только теперь, ПОСЛЕ завершения текущего —
        # так интервал = "N минут после окончания предыдущего", а не "каждые N
        # минут по часам", и прогоны никогда не накладываются друг на друга,
        # сколько бы времени ни занял список. Перечитываем интервал из
        # хранилища на случай, если его поменяли командой, пока шёл этот прогон.
        interval = await _get_watch_interval(chat_id)
        _schedule_watchlist_job(context.job_queue, chat_id, interval)


# ---------------------------------------------------------------------------
# Кросс-маркет арбитраж: сканируем рынок CSFloat и сравниваем с ценой Steam
# ---------------------------------------------------------------------------

def _schedule_arb_job(job_queue, chat_id: int, delay_minutes: float = ARB_INTERVAL_MINUTES) -> None:
    """Следующий прогон арбитража — одноразовой джобой, как и у вотчлиста (без наложений)."""
    for job in job_queue.get_jobs_by_name(f"{ARB_JOB_PREFIX}{chat_id}"):
        job.schedule_removal()
    job_queue.run_once(
        arb_scan_job,
        when=delay_minutes * 60,
        data={"chat_id": chat_id},
        name=f"{ARB_JOB_PREFIX}{chat_id}",
    )


def _format_arb_chunks(offers) -> list[str]:
    """Сообщения по арбитражным находкам, разбитые под лимит Telegram."""
    lines = [
        f"💱 CSFloat дешевле Steam — найдено {len(offers)}\n"
        f"<i>Цена Steam — оценка, а не текущая нижняя цена в стакане: "
        f"проверяй лот перед покупкой. "
        f"«Чистыми» = сколько останется при перепродаже в Steam по этой цене, "
        f"за вычетом комиссии Steam ~{(1 - STEAM_FEE_MULTIPLIER) * 100:.0f}%. "
        f"Помни: выручка в Steam приходит на кошелёк и не выводится.</i>"
    ]

    for o in offers[:15]:
        # Лот мог попасть сюда из-за наклеек, а не из-за цены — тогда сам скин
        # бывает и дороже Steam, и писать "дешевле на -2%" было бы враньём.
        if o.discount_pct >= 0:
            price_cmp = f"дешевле на {o.discount_pct:.1f}%"
        else:
            price_cmp = f"дороже на {abs(o.discount_pct):.1f}%"
        net = f"+${o.net_after_fee:.2f}" if o.net_after_fee >= 0 else f"-${abs(o.net_after_fee):.2f}"
        window_label = {"last_24h": "за сутки", "last_7d": "за неделю"}.get(
            o.steam_price_window or "", o.steam_price_window or ""
        )
        steam_part = f"Steam ${o.steam_price:.2f}"
        if window_label:
            steam_part += f" ({window_label})"
        # Имя в <code> — в Telegram такой блок копируется одним нажатием.
        # Нужно, чтобы название можно было тут же вставить в поиск на площадке.
        block = (
            f"<code>{html_module.escape(o.market_hash_name)}</code>\n"
            f"  CSFloat ${o.csfloat_price:.2f} | {steam_part} | {price_cmp}\n"
            f"  чистыми при перепродаже: {net}"
        )
        # Второе мнение по цене. Показываем всегда, когда оно есть: подтверждение
        # усиливает доверие к находке, а расхождение — прямой повод проверить
        # предмет руками перед покупкой.
        if o.steam_price_second_opinion:
            block += f"\n  <i>{html_module.escape(o.steam_price_second_opinion)}</i>"
        if o.float_value is not None:
            block += f"\n  флоат {o.float_value:.5f}"
        # Раньше тут стояло "продаж на Steam" — это было враньём. Число берётся
        # из reference.quantity, его считает сам CSFloat, и к продажам в Steam
        # оно отношения не имеет. Подписываем по источнику.
        if o.steam_volume is not None:
            block += f" | в обороте по CSFloat: {o.steam_volume}"
        if o.stickers:
            block += f"\n  <code>{html_module.escape(', '.join(o.stickers))}</code>"
            if o.stickers_value > 0:
                block += f"\n  наклейки ≈${o.stickers_value:.2f}"
                if o.sticker_markup_pct is not None:
                    block += f", наценка за них {o.sticker_markup_pct:.1f}%"
            if o.stickers_unpriced:
                block += f" (у {o.stickers_unpriced} цена неизвестна)"
        # Ликвидность важнее скидки: перепродать не торгующийся предмет некому,
        # и вся «выгода» остаётся бумажной.
        if o.steam_sales_recent is False:
            block += "\n  ⚠️ <i>за неделю в Steam не продавался ни разу — выйти обратно будет трудно</i>"
        if o.duplicate_count:
            block += (
                f"\n  <i>таких же лотов ещё {o.duplicate_count} — "
                f"показан самый выгодный</i>"
            )
        block += (
            f'\n  <a href="{o.url}">Купить на CSFloat</a>'
            f' · <a href="{o.steam_url}">Проверить в Steam</a>'
        )
        lines.append(block)

    return _chunk_lines(lines, sep="\n\n")


async def _verify_against_steam(offers, min_discount_pct: float) -> list:
    """
    Проверить кандидатов живой ценой Steam и пересчитать по ней.

    Зачем понадобилось. Двух источников не хватило: прайс-лист csgotrader и
    справка CSFloat расходятся вдвое и систематически. На находках 2026-08-20
    отношение держалось около 2.3 у всех подряд, причём справка почти точно
    совпадала с ценой лота — она и считается по рынку CSFloat, это не цена
    Steam. По двум источникам такое не разрешается: нужен третий, настоящий.

    Дорого это только по запросам — priceoverview отвечает по одному предмету,
    пакетного эндпоинта у Steam нет. Поэтому проверяем ТОЛЬКО то, что уже
    прошло отбор: десяток кандидатов вместо тысячи просмотренных лотов.

    Лот, который не подтвердился, выбрасывается: лучше промолчать, чем звать
    покупать по цене, которой нет.
    """
    if not offers:
        return offers
    if not STEAM_POOL.enabled() and steam_cooldown_remaining(scope="pricing") > 0:
        log.warning(
            "arb: Steam на кулдауне и прокси нет — цены оставлены непроверенными"
        )
        return offers

    todo = offers[:ARB_VERIFY_LIMIT]

    # Кэш и общий потолок живых запросов — те же, что у /markets, и по той же
    # причине: раньше этот скан выстреливал до 80 запросов каждые 10 минут,
    # выжигал все адреса пула на полчаса, и второй канал приходил к пустому
    # пулу. Ресурс Steam общий, значит и экономить его надо сообща.
    cached = await get_steam_prices_batch([o.market_hash_name for o in todo])
    fresh_budget = [o for o in todo if o.market_hash_name not in cached][:STEAM_LIVE_BUDGET]

    lanes = max(1, len(STEAM_POOL))
    log.info(
        "arb: %d кандидат(ов) из %d: в кэше %d, спрошу у Steam %d, полос %d",
        len(todo), len(offers), len(cached), len(fresh_budget), lanes,
    )

    verified = []
    unchecked = 0
    semaphore = asyncio.Semaphore(lanes)

    class Cached:
        __slots__ = ("lowest", "volume", "median")

        def __init__(self, entry):
            self.lowest, self.volume, self.median = entry["price"], entry.get("volume"), None

    async def check(offer, session):
        entry = cached.get(offer.market_hash_name)
        if entry:
            return offer, Cached(entry)
        if offer not in fresh_budget:
            return offer, None
        async with semaphore:
            try:
                live = await get_steam_market_price_retrying(session, offer.market_hash_name)
            except Exception:
                log.info("arb: %s — Steam не ответил", offer.market_hash_name)
                return offer, None
            if live and live.lowest:
                await set_steam_price(offer.market_hash_name, live.lowest, live.volume)
            return offer, live

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        checked = await asyncio.gather(*(check(o, session) for o in todo))
        for o, live in checked:

            if live is None or not live.lowest:
                # Не "не подтвердил", а "не проверяли": разница принципиальная,
                # и раньше лог утверждал первое, когда на деле было второе.
                unchecked += 1
                continue

            was, was_pct = o.steam_price, o.discount_pct
            o.steam_price = live.lowest
            o.steam_price_window = "живая цена Steam"
            o.steam_volume = live.volume
            o.steam_sales_recent = bool(live.volume)
            o.discount_pct = (live.lowest - o.csfloat_price) / live.lowest * 100
            o.net_after_fee = live.lowest * STEAM_FEE_MULTIPLIER - o.csfloat_price
            o.steam_price_second_opinion = (
                f"оценка была ${was:.2f} ({was_pct:.0f}%), Steam на самом деле ${live.lowest:.2f}"
            )

            log.info(
                "arb: %s — оценка $%.2f (%.1f%%), Steam на самом деле $%.2f "
                "(медиана $%s, продаж за сутки %s) -> скидка %.1f%%",
                o.market_hash_name, was, was_pct, live.lowest,
                f"{live.median:.2f}" if live.median else "?", live.volume or 0,
                o.discount_pct,
            )

            # Порог применяем ЗАНОВО. Отбор проходил по оценке, а она может
            # быть завышена вдвое — тогда «скидка 58%» после проверки
            # оказывается 4%, и слать такое нельзя: лот отобрали по числу,
            # которого не существует.
            if o.discount_pct < min_discount_pct:
                log.info(
                    "arb: %s — после проверки скидка %.1f%% ниже порога %.0f%%, выбрасываю",
                    o.market_hash_name, o.discount_pct, min_discount_pct,
                )
                continue
            verified.append(o)

    if unchecked:
        log.info(
            "arb: %d кандидат(ов) остались непроверенными (бюджет живых запросов %d) — "
            "их проверим в следующих прогонах, кэш накапливается",
            unchecked, STEAM_LIVE_BUDGET,
        )
    verified.sort(key=lambda x: x.discount_pct, reverse=True)
    return verified


def _warn_if_over_budget() -> None:
    """
    Сказать при старте, влезает ли настройка скана в квоту CSFloat.

    Считать это надо явно, потому что интуиция здесь подводит. Квота — 200
    запросов в час НА КЛЮЧ, и она НЕ умножается числом прокси: в проде при
    семи разных адресах счётчик шёл одной цепочкой с общим моментом сброса.
    Прокси спасают от блокировки по репутации адреса, но не от квоты.

    Без этой проверки перебор проявляется не сообщением, а пустыми прогонами
    посреди часа: бюджет выбран, все полосы падают на 429, подборка приходит
    пустой — и выглядит это как поломка отбора, а не как исчерпанный лимит.
    """
    per_scan = -(-ARB_TARGET_LISTINGS // csfloat_client.MAX_LIMIT)  # округление вверх
    scans_per_hour = 60 / ARB_INTERVAL_MINUTES if ARB_INTERVAL_MINUTES else 0
    needed = per_scan * scans_per_hour
    budget = 200  # x-ratelimit-limit, наблюдаемый на ключе

    if needed <= budget:
        log.info(
            "csfloat: бюджет в порядке — %d лотов за прогон это %d запросов, "
            "%.0f прогонов в час = %.0f из %d доступных",
            ARB_TARGET_LISTINGS, per_scan, scans_per_hour, needed, budget,
        )
        return

    safe_target = int(budget / scans_per_hour) * csfloat_client.MAX_LIMIT if scans_per_hour else 0
    safe_interval = 60 / (budget / per_scan) if per_scan else 0
    log.warning(
        "csfloat: настройка НЕ влезает в квоту — %d лотов каждые %.0f мин требуют %.0f "
        "запросов в час при доступных %d. Прогоны будут падать на 429 посреди часа. "
        "Варианты: ARB_TARGET_LISTINGS=%d при нынешнем интервале, либо "
        "ARB_INTERVAL_MINUTES=%.0f при нынешней цели",
        ARB_TARGET_LISTINGS, ARB_INTERVAL_MINUTES, needed, budget,
        safe_target, safe_interval,
    )


def _drop_stattrak(names) -> tuple[list, int]:
    """
    Убрать StatTrak-предметы из списка. Возвращает (что осталось, сколько убрали).

    Фильтруем на входе в скан, а не чистим сами списки: предметы остаются в
    вотчлисте, и если фильтр однажды выключат, они снова начнут сканироваться
    без ручного добавления заново.

    Проверка по подстроке без ™ намеренно: символ легко теряется при переносе
    имён между источниками, а "stattrak" в названии обычного скина не
    встречается.
    """
    if not SKIP_STATTRAK:
        return list(names), 0
    kept = [n for n in names if "stattrak" not in n.lower()]
    return kept, len(names) - len(kept)


async def _fill_steam_prices(listings) -> int:
    """
    Проставить лотам цену Steam из прайс-листа csgotrader.app.

    Зачем: модуль CSFloat писался в расчёте на то, что площадка сама кладёт
    цену Steam в каждый лот (item.scm.price), и весь арбитраж считался из
    одного ответа. 2026-08-19 выяснилось, что ключа scm в ответе больше нет
    вообще — ни у одного лота из 50. Отбор при этом молча выбрасывал всё
    подряд («сравнивать не с чем»), и снаружи это выглядело как слишком
    строгий порог: ноль находок при любых настройках.

    Зависеть от чужого необязательного поля тут больше незачем. Прайс-лист
    csgotrader.app бот и так качает для цен стикеров — это статический файл на
    CDN со ВСЕМ каталогом CS2 по market_hash_name, без ключа, лимита и бана по
    IP. Он же переживёт следующее изменение формата у CSFloat.

    Оговорка про смысл числа: там медиана продаж Steam за последние сутки, а не
    текущая нижняя цена в стакане. Для «дешевле рынка» это даже устойчивее
    (разовый выброс не сдвинет), но точной ценой продажи считать нельзя.
    """
    missing = [l for l in listings if l.steam_price is None]
    if not missing:
        return 0

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        prices = await get_csgotrader_price_details(session)
    if not prices:
        log.warning("arb: прайс-лист csgotrader пуст — цену Steam подставить нечем")
        return 0

    from_reference = 0
    from_pricelist = 0
    confirmed = 0
    disagree = 0
    for l in missing:
        # Ликвидность: reference.quantity от CSFloat — замена пропавшему
        # scm.volume. Приходит вместе с лотом, лишних запросов не требует.
        if l.steam_volume is None and l.reference_quantity is not None:
            l.steam_volume = l.reference_quantity

        found = prices.get(l.market_hash_name)
        # Только суточное окно, без отката на недельное и старше — см.
        # ARB_PRICE_WINDOW.
        listed = found.windows.get(ARB_PRICE_WINDOW) if found else None

        # Была ли за неделю хоть одна продажа в Steam. Окна прайс-листа
        # строятся по состоявшимся сделкам, поэтому наличие last_24h или
        # last_7d — прямое свидетельство, что предмет вообще торгуется.
        # Это единственный независимый от CSFloat признак ликвидности:
        # reference.quantity считает сам CSFloat, и к продажам в Steam он
        # отношения не имеет.
        l.steam_sales_recent = bool(
            found and any(w in found.windows for w in ("last_24h", "last_7d"))
        )

        # Основной источник — прайс-лист, справка CSFloat второе мнение.
        #
        # Порядок менялся дважды, поэтому основание записано явно. Сначала
        # основным был прайс-лист; потом на случае P2000 | Acid Etched (лот
        # $12.04, прайс-лист $28.18, справка $10.44) я решил, что прав CSFloat,
        # и поменял приоритет. Проверка на большем числе предметов показала
        # обратное: с ценой в Steam чаще сходится прайс-лист, а справка CSFloat
        # систематически ниже — что логично, это оценка площадки для СВОЕГО
        # рынка, а не цена Steam.
        #
        # Справка при этом остаётся полезной вдвойне: как второе мнение и как
        # запасной источник там, где предмета нет в прайс-листе (а нет его у
        # заметной доли лотов).
        if listed is not None:
            l.steam_price = listed
            l.steam_price_window = ARB_PRICE_WINDOW
            l.steam_price_spread_pct = found.recent_spread_pct
            from_pricelist += 1

            if l.reference_price and l.reference_price > 0:
                gap = abs(listed - l.reference_price) / l.reference_price * 100
                if gap <= ARB_SOURCE_GAP_PCT:
                    confirmed += 1
                    l.steam_price_windows = f"CSFloat подтверждает: ${l.reference_price:.2f}"
                else:
                    disagree += 1
                    l.steam_price_windows = (
                        f"CSFloat оценивает в ${l.reference_price:.2f} "
                        f"(расхождение {gap:.0f}%) — проверь перед покупкой"
                    )
            else:
                l.steam_price_windows = found.describe()
            continue

        # Предмета нет в прайс-листе — берём справку CSFloat, иначе потеряли бы
        # заметную часть рынка вовсе.
        if l.reference_price and l.reference_price > 0:
            l.steam_price = l.reference_price
            l.steam_price_window = "справка CSFloat"
            l.steam_price_windows = "в прайс-листе предмета нет — цена по оценке CSFloat"
            from_reference += 1

    log.info(
        "arb: цена подставлена для %d из %d лотов (%d по прайс-листу, %d по справке CSFloat, "
        "когда предмета в прайс-листе нет). CSFloat подтвердил %d, разошёлся на %d",
        from_pricelist + from_reference, len(missing),
        from_pricelist, from_reference, confirmed, disagree,
    )
    return from_reference + from_pricelist


async def _run_arb_scan(bot, chat_id: int) -> int | None:
    """
    Один прогон арбитража. Возвращает число отправленных находок, либо None,
    если прогон не запускался (выключено / уже идёт / кулдаун).
    """
    settings = await get_arb_settings(chat_id)
    if settings["min_discount"] is None:
        return None
    if chat_id in _arb_running:
        log.info("arb: прогон для chat_id=%s уже идёт, пропускаю", chat_id)
        return None
    if csfloat_client.cooldown_remaining() > 0:
        log.info(
            "arb: chat_id=%s пропускает прогон — кулдаун CSFloat ещё %.0f мин",
            chat_id, csfloat_client.cooldown_remaining() / 60,
        )
        return None

    _arb_running.add(chat_id)
    try:
        listings = await csfloat_client.fetch_market_wide(
            target=ARB_TARGET_LISTINGS,
            sort_by=ARB_SORT_BY,
            min_price=settings["min_price"],
            max_price=settings["max_price"],
        )
        await _fill_steam_prices(listings)
        offers = find_arbitrage_offers(
            listings,
            min_discount_pct=settings["min_discount"],
            min_price=settings["min_price"],
            max_price=settings["max_price"],
            min_steam_volume=settings["min_volume"],
            sticker_max_markup_pct=settings["sticker_markup"],
        )
        log.info(
            "arb: chat_id=%s просмотрено %s лотов, подошло %s",
            chat_id, len(listings), len(offers),
        )
        # Все окна цены по каждой находке. Это то, чего не хватило, когда цена
        # разошлась с реальной в 2.4 раза: по одному числу нельзя было понять,
        # взяли мы устаревшее окно или в файле лежит не то, что мы думаем.
        # Теперь спорную находку можно разобрать прямо по логу, не гадая.
        for o in offers[:10]:
            log.info(
                "arb: %s — CSFloat $%.2f, Steam $%.2f (%s) -> скидка %.1f%%. Окна: %s",
                o.market_hash_name, o.csfloat_price, o.steam_price,
                o.steam_price_window or "?", o.discount_pct,
                next((l.steam_price_windows for l in listings
                      if l.listing_id == o.listing_id), None) or "нет данных",
            )
        if not offers:
            return 0

        # Тот же дедуп, что у вотчлиста: один и тот же лот не присылаем повторно
        # Третий источник: спрашиваем настоящую цену у Steam по кандидатам.
        offers = await _verify_against_steam(offers, settings["min_discount"])
        if not offers:
            log.info("arb: chat_id=%s ни один кандидат не подтвердился ценой Steam", chat_id)
            return 0

        # Ключ дедупа — предмет и цена, а НЕ listing_id.
        #
        # По listing_id дедуп почти не работал: у ходового предмета десятки
        # взаимозаменяемых лотов, схлопывание оставляет лучший, и на следующем
        # прогоне лучшим оказывается другой лот — тот же товар по той же цене
        # приходил снова под новым id. С ценой в ключе повторное уведомление
        # приходит только когда предмет реально подешевел, а это как раз то,
        # о чём стоит знать.
        new_offers = []
        for o in offers:
            key = f"arb:{o.market_hash_name}:{o.csfloat_price:.2f}"
            if not await was_offer_sent_recently(chat_id, key):
                new_offers.append(o)
        if not new_offers:
            log.info(
                "arb: chat_id=%s все %d находок уже присылали — молчу (дедуп %d ч)",
                chat_id, len(offers), SENT_OFFER_TTL_SECONDS // 3600,
            )
            return 0

        for chunk in _format_arb_chunks(new_offers):
            await bot.send_message(
                chat_id=chat_id, text=chunk, parse_mode="HTML", disable_web_page_preview=True
            )
        for o in new_offers:
            await mark_offer_sent(chat_id, f"arb:{o.market_hash_name}:{o.csfloat_price:.2f}")
        return len(new_offers)
    finally:
        _arb_running.discard(chat_id)


async def arb_scan_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data["chat_id"]
    try:
        await _run_arb_scan(context.bot, chat_id)
    except CSFloatRateLimited as e:
        log.warning("arb: chat_id=%s рейт-лимит CSFloat: %s", chat_id, e)
        if e.is_ip_block:
            # Это не квота и не разовый челлендж — CSFloat блокирует IP Render
            # как VPN, время это не лечит. Ретраи бот всё равно продолжит (вдруг
            # IP сменится или блок снимут), но раз в SENT_OFFER_TTL_SECONDS
            # честно предупреждаем в чате, а не молчим про то, что арбитраж не работает.
            notice_key = "arb_ip_block_notice"
            if not await was_offer_sent_recently(chat_id, notice_key):
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "⚠️ Арбитраж CSFloat приостановлен: CSFloat блокирует IP-адрес "
                        "сервера (Render) как VPN и просит «отключить VPN или сменить сеть». "
                        "Это не временная квота — ожиданием не лечится. Бот продолжит "
                        f"пробовать раз в {csfloat_client.IP_BLOCK_COOLDOWN_SECONDS // 3600} ч "
                        "на случай, если IP сменится или блокировку снимут, но пока CSFloat "
                        "с этого сервера недоступен."
                    ),
                )
                await mark_offer_sent(chat_id, notice_key)
    except CSFloatError as e:
        log.warning("arb: chat_id=%s ошибка CSFloat: %s", chat_id, e)
        await context.bot.send_message(chat_id=chat_id, text=f"⚠️ Арбитраж: {e}")
    except Exception:
        log.exception("arb: непредвиденная ошибка в прогоне chat_id=%s", chat_id)
    finally:
        settings = await get_arb_settings(chat_id)
        if settings["min_discount"] is not None:
            _schedule_arb_job(context.job_queue, chat_id)


async def watchpause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /watchpause — остановить автоскан по расписанию (оба списка — обычный
    вотчлист и охота за флоатом — сохраняются и не трогаются, они делят одну
    и ту же джобу).
    """
    chat_id = update.effective_chat.id
    await set_watch_paused(chat_id, True)
    for job in context.application.job_queue.get_jobs_by_name(f"{WATCHLIST_JOB_PREFIX}{chat_id}"):
        job.schedule_removal()
    sticker_items = await get_watchlist(chat_id)
    float_items = await get_float_watchlist(chat_id)
    await update.message.reply_text(
        f"⏸ Автоскан остановлен: вотчлист ({len(sticker_items)} шт.) и охота за флоатом "
        f"({len(float_items)} шт.) сохранены — их можно смотреть /watchlist, /floatlist и чистить "
        f"/watchdel, /floatdel.\n"
        f"Возобновить оба сразу: /watchresume. Разовый скан вручную по-прежнему работает: /scanall."
    )


async def watchresume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /watchresume — снова включить автоскан по расписанию. Одна джоба на чат
    прогоняет ОБА списка (обычный вотчлист и охоту за флоатом) за один
    проход — включаются и выключаются вместе, отдельной команды под флоат нет.
    """
    chat_id = update.effective_chat.id
    await set_watch_paused(chat_id, False)
    interval = await _get_watch_interval(chat_id)
    _schedule_watchlist_job(context.application.job_queue, chat_id, interval)
    sticker_items, st_skipped_a = _drop_stattrak(await get_watchlist(chat_id))
    float_items, st_skipped_b = _drop_stattrak(await get_float_watchlist(chat_id))
    text = (
        f"▶️ Автоскан возобновлён: вотчлист ({len(sticker_items)} шт.) + охота за флоатом "
        f"({len(float_items)} шт.), пауза {interval:g} мин между прогонами."
    )
    if st_skipped_a or st_skipped_b:
        # Говорим прямо, что предметы не потерялись, а исключены фильтром —
        # иначе уменьшившийся счётчик выглядит как пропажа из списка.
        text += (
            f"\n\nStatTrak исключён из сканов: пропускаю {st_skipped_a + st_skipped_b} шт. "
            "Из списков они не удалены."
        )
    cooldown = blocking_cooldown()
    if cooldown > 0:
        text += (
            f"\n\n⚠️ Но Steam сейчас на кулдауне после 429, и свободных прокси нет — "
            f"первые {cooldown / 60:.0f} мин прогоны будут пропускаться.\n"
            f"Состояние пула: {STEAM_POOL.describe()}"
        )
    elif steam_cooldown_remaining() > 0:
        # Кулдаун есть, но он не мешает: прямой адрес переждёт, а запросы
        # пойдут с прокси. Сказать об этом стоит — иначе строчка «кулдаун» в
        # /status выглядит как поломка, хотя всё работает.
        text += (
            f"\n\nПрямой адрес на кулдауне после 429, но это не мешает: "
            f"прогоны пойдут через прокси ({STEAM_POOL.describe()})."
        )
    await update.message.reply_text(text)


async def setarb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setarb              — показать настройки арбитража
    /setarb <мин%>       — включить: слать лоты CSFloat, которые дешевле Steam на мин%
    /setarb off          — выключить
    """
    chat_id = update.effective_chat.id
    args = context.args
    settings = await get_arb_settings(chat_id)

    if not args:
        if settings["min_discount"] is None:
            await update.message.reply_text(
                "💱 Арбитраж CSFloat ↔ Steam выключен.\n\n"
                "Включить: /setarb <мин%>\nПример: /setarb 20 — слать лоты, которые "
                f"на CSFloat дешевле цены Steam минимум на 20%.\n"
                f"Проверка каждые {ARB_INTERVAL_MINUTES:g} мин, до "
                f"{ARB_TARGET_LISTINGS} лотов за прогон.\n\n"
                "Дополнительно:\n"
                "/setarbprice <мин$> <макс$> — ограничить диапазон цены\n"
                "/setarbvolume <шт> — только ликвидное (продаж на Steam за сутки)\n"
                "/setarbstickers <макс%> — ещё и лоты, где наклейки почти даром\n"
                "/arbnow — проверить прямо сейчас"
            )
        else:
            lines = [f"💱 Арбитраж включён: дешевле Steam минимум на {settings['min_discount']:g}%"]
            if settings["min_price"] is not None or settings["max_price"] is not None:
                lo = f"${settings['min_price']:.2f}" if settings["min_price"] is not None else "без границы"
                hi = f"${settings['max_price']:.2f}" if settings["max_price"] is not None else "без границы"
                lines.append(f"Цена лота: от {lo} до {hi}")
            if settings["min_volume"] is not None:
                lines.append(f"Ликвидность: от {settings['min_volume']} продаж на Steam")
            if settings["sticker_markup"] is not None:
                lines.append(f"Плюс лоты с наценкой за наклейки ≤{settings['sticker_markup']:g}%")
            lines.append(f"Проверка каждые {ARB_INTERVAL_MINUTES:g} мин. Выключить: /setarb off")
            await update.message.reply_text("\n".join(lines))
        return

    if args[0].lower() == "off":
        await set_arb_setting(chat_id, "min_discount", None)
        for job in context.application.job_queue.get_jobs_by_name(f"{ARB_JOB_PREFIX}{chat_id}"):
            job.schedule_removal()
        await update.message.reply_text("💱 Арбитраж выключен.")
        return

    if not csfloat_client.csfloat_enabled():
        await update.message.reply_text(
            "⚠️ Не задан CSFLOAT_API_KEY — без него CSFloat не отвечает.\n"
            "Ключ берётся в профиле csfloat.com на вкладке developer и прописывается "
            "переменной окружения CSFLOAT_API_KEY на Render."
        )
        return

    try:
        pct = float(args[0])
    except ValueError:
        await update.message.reply_text("Нужно число процентов. Пример: /setarb 20")
        return
    if pct <= 0:
        await update.message.reply_text("Процент должен быть больше нуля.")
        return

    await set_arb_setting(chat_id, "min_discount", pct)
    _schedule_arb_job(context.application.job_queue, chat_id, delay_minutes=0.2)
    await update.message.reply_text(
        f"💱 Арбитраж включён: ищу лоты CSFloat дешевле цены Steam минимум на {pct:g}%.\n"
        f"Проверка каждые {ARB_INTERVAL_MINUTES:g} мин, первый прогон — прямо сейчас.\n\n"
        f"⚠️ Учти: выручка от продажи в Steam попадает на кошелёк Steam и не выводится. "
        f"В сообщениях показываю «чистыми» с учётом комиссии ~{(1 - STEAM_FEE_MULTIPLIER) * 100:.0f}%, "
        f"чтобы процент не обманывал."
    )


async def setarbprice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/setarbprice <мин$> <макс$> — диапазон цены лота для арбитража; off — снять."""
    chat_id = update.effective_chat.id
    args = context.args

    if not args:
        s = await get_arb_settings(chat_id)
        lo = f"${s['min_price']:.2f}" if s["min_price"] is not None else "без границы"
        hi = f"${s['max_price']:.2f}" if s["max_price"] is not None else "без границы"
        await update.message.reply_text(
            f"Диапазон цены для арбитража: от {lo} до {hi}.\n"
            "Задать: /setarbprice <мин$> <макс$>, снять: /setarbprice off"
        )
        return

    if args[0].lower() == "off":
        await set_arb_setting(chat_id, "min_price", None)
        await set_arb_setting(chat_id, "max_price", None)
        await update.message.reply_text("Ограничение по цене снято.")
        return

    if len(args) < 2:
        await update.message.reply_text("Нужны оба значения: /setarbprice <мин$> <макс$>")
        return
    try:
        lo, hi = float(args[0]), float(args[1])
    except ValueError:
        await update.message.reply_text("Оба значения должны быть числами. Пример: /setarbprice 5 500")
        return
    if lo > hi:
        await update.message.reply_text("Минимум не может быть больше максимума.")
        return

    await set_arb_setting(chat_id, "min_price", lo)
    await set_arb_setting(chat_id, "max_price", hi)
    await update.message.reply_text(f"Арбитраж: смотрю лоты от ${lo:.2f} до ${hi:.2f}.")


async def setarbvolume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/setarbvolume <шт> — минимум продаж на Steam (отсекает неликвид); off — снять."""
    chat_id = update.effective_chat.id
    args = context.args

    if not args:
        s = await get_arb_settings(chat_id)
        cur = f"{s['min_volume']} продаж" if s["min_volume"] is not None else "не задан"
        await update.message.reply_text(
            f"Фильтр ликвидности: {cur}.\n\n"
            "Смысл: скидка на предмете, который на Steam почти не продаётся, обычно бумажная — "
            "выйти из него не получится.\n"
            "Задать: /setarbvolume <шт>, напр. /setarbvolume 5. Снять: /setarbvolume off"
        )
        return

    if args[0].lower() == "off":
        await set_arb_setting(chat_id, "min_volume", None)
        await update.message.reply_text("Фильтр ликвидности снят.")
        return

    try:
        vol = int(float(args[0]))
    except ValueError:
        await update.message.reply_text("Нужно целое число. Пример: /setarbvolume 5")
        return
    if vol < 0:
        await update.message.reply_text("Число не может быть отрицательным.")
        return

    await set_arb_setting(chat_id, "min_volume", vol)
    await update.message.reply_text(f"Арбитраж: только предметы с {vol}+ продаж на Steam.")


async def setarbstickers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/setarbstickers <макс%> — ловить лоты, где наклейки достаются почти даром; off — снять."""
    chat_id = update.effective_chat.id
    args = context.args

    if not args:
        s = await get_arb_settings(chat_id)
        cur = f"≤{s['sticker_markup']:g}%" if s["sticker_markup"] is not None else "не задан"
        await update.message.reply_text(
            f"Наценка за наклейки в арбитраже: {cur}.\n\n"
            "Это та же логика, что у обычного вотчлиста, но применённая к лотам CSFloat: "
            "сколько сверх голой цены Steam просят за набор наклеек относительно их реальной "
            "стоимости. 0% — наклейки достались даром.\n"
            "Ловит случаи, когда сам скин не дешевле рынка, а наклейки фактически бесплатны.\n"
            "Задать: /setarbstickers <макс%>, напр. /setarbstickers 10. Снять: /setarbstickers off"
        )
        return

    if args[0].lower() == "off":
        await set_arb_setting(chat_id, "sticker_markup", None)
        await update.message.reply_text("Отбор по наклейкам в арбитраже выключен.")
        return

    try:
        pct = float(args[0])
    except ValueError:
        await update.message.reply_text("Нужно число процентов. Пример: /setarbstickers 10")
        return

    await set_arb_setting(chat_id, "sticker_markup", pct)
    await update.message.reply_text(
        f"Арбитраж: теперь показываю ещё и лоты, где наценка за наклейки ≤{pct:g}%."
    )


async def arbnow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/arbnow — проверить арбитраж прямо сейчас, не дожидаясь расписания."""
    chat_id = update.effective_chat.id
    settings = await get_arb_settings(chat_id)
    if settings["min_discount"] is None:
        await update.message.reply_text("Арбитраж выключен. Включить: /setarb <мин%>, напр. /setarb 20")
        return
    if chat_id in _arb_running:
        await update.message.reply_text("Проверка уже идёт, дождись окончания.")
        return

    cooldown = csfloat_client.cooldown_remaining()
    if cooldown > 0:
        await update.message.reply_text(
            f"CSFloat на кулдауне после 429 — ещё {cooldown / 60:.0f} мин.\n"
            "Сбросить и попробовать сразу: /arbreset"
        )
        return

    await update.message.reply_text("Смотрю рынок CSFloat…")
    try:
        sent = await _run_arb_scan(context.bot, chat_id)
    except CSFloatRateLimited as e:
        if e.is_ip_block:
            await update.message.reply_text(
                f"⏸ {e}\nПричина — CSFloat блокирует IP сервера как VPN "
                "(«disable your VPN or try a different network»). Это не квота, "
                "ожиданием не лечится."
            )
        else:
            await update.message.reply_text(f"⏸ {e}")
        return
    except CSFloatError as e:
        await update.message.reply_text(f"⚠️ {e}")
        return

    if not sent:
        await update.message.reply_text("Готово, ничего подходящего не нашлось.")


async def setinterval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setinterval <минут> — пауза между прогонами автоскана.

    Это главный рычаг против 429, и он важнее паузы между отдельными запросами.
    Steam смотрит не на промежуток между двумя запросами, а на сколько их
    приходит суммарно. При 110 предметах прогон занимает около 7 минут, и с
    паузой в 2 минуты цикл выходит 9 — то есть больше семисот запросов в час с
    одного адреса. Столько Steam не прощает.

    Показывает расчётную нагрузку прямо в ответе: иначе цифра в минутах ничего
    не говорит, а последствия видны только через полчаса бана.
    """
    chat_id = update.effective_chat.id
    sticker_items, _ = _drop_stattrak(await get_watchlist(chat_id))
    float_items, _ = _drop_stattrak(await get_float_watchlist(chat_id))
    items = len(set(sticker_items) | set(float_items)) or 1
    scan_minutes = items * MIN_REQUEST_INTERVAL / 60

    def load_line(gap: float) -> str:
        cycle = scan_minutes + gap
        return f"{items * 60 / cycle:.0f} запросов в час (цикл {cycle:.0f} мин)"

    current = await _get_watch_interval(chat_id)

    if not context.args:
        await update.message.reply_text(
            "<b>Пауза между прогонами автоскана</b>\n"
            f"сейчас: {current:g} мин → {load_line(current)}\n\n"
            f"Предметов в сканах: {items}, один прогон ≈ {scan_minutes:.0f} мин.\n\n"
            "<code>/setinterval 25</code> — поменять\n\n"
            "<i>Ориентир: держать нагрузку в пределах 200-250 запросов в час "
            "с одного адреса. Больше — и Steam начинает отвечать 429, а это "
            "бан адреса на 30 минут и дольше.</i>",
            parse_mode="HTML",
        )
        return

    try:
        minutes = float(context.args[0].replace(",", "."))
    except ValueError:
        await update.message.reply_text(f"{context.args[0]!r} — не число. Пример: /setinterval 25")
        return
    if minutes < 0:
        await update.message.reply_text("Пауза не может быть отрицательной.")
        return

    await set_watch_gap(chat_id, minutes)

    text = f"✅ Пауза между прогонами: {minutes:g} мин\nНагрузка: {load_line(minutes)}"
    rate = items * 60 / (scan_minutes + minutes)
    if rate > 250:
        text += (
            f"\n\n⚠️ Это много для одного адреса — Steam при такой нагрузке "
            f"отвечает 429, а это бан на 30 минут и дольше. "
            f"Спокойное значение: /setinterval {max(1, round(items * 60 / 200 - scan_minutes))}"
        )
    # Джобу пересоздаём сразу, иначе новая пауза вступит в силу только после
    # следующего прогона — то есть через старый, слишком короткий интервал.
    if not await get_watch_paused(chat_id):
        _schedule_watchlist_job(context.application.job_queue, chat_id, minutes)
        text += "\n\nРасписание обновлено."
    await update.message.reply_text(text)


async def setratio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setratio <во сколько раз> — наклейки должны стоить дороже голого скина.

    2 означает «набор наклеек вдвое дороже самого скина», 1.3 — «на 30%
    дороже». off — фильтр выключить.

    Отвечает на другой вопрос, чем /setdefaults с его наценкой. Наценка — это
    «сколько сверху просят за наклейки», и на дешёвом скине она бывает
    отличной, хотя набор стоит копейки и возиться не с чем. Здесь же порог
    весомости: лот интересен именно как набор, а не как скин.
    """
    chat_id = update.effective_chat.id
    current = await get_sticker_ratio(chat_id)

    if not context.args:
        now = (
            f"сейчас: наклейки должны быть дороже скина в {current:g} раз"
            if current is not None
            else "сейчас: фильтр выключен"
        )
        await update.message.reply_text(
            "<b>Вес наклеек относительно скина</b>\n"
            f"{now}\n\n"
            "<code>/setratio 2</code> — набор вдвое дороже самого скина\n"
            "<code>/setratio 1.3</code> — на 30% дороже\n"
            "<code>/setratio off</code> — выключить\n\n"
            "<i>Считается от цены голого скина (самый дешёвый лот предмета), "
            "а не от цены лота — в неё наклейки уже включены.</i>",
            parse_mode="HTML",
        )
        return

    raw = context.args[0].lower().replace(",", ".").replace("x", "").replace("х", "")
    if raw in ("off", "выкл", "0"):
        await set_sticker_ratio(chat_id, None)
        await update.message.reply_text("✅ Фильтр по весу наклеек выключен.")
        return

    try:
        ratio = float(raw)
    except ValueError:
        await update.message.reply_text(
            f"{context.args[0]!r} — не число. Пример: /setratio 2 или /setratio 1.3"
        )
        return
    if ratio <= 0:
        await update.message.reply_text("Множитель должен быть больше нуля.")
        return

    await set_sticker_ratio(chat_id, ratio)
    example_skin = 10.0
    await update.message.reply_text(
        f"✅ Наклейки должны быть дороже скина в {ratio:g} раз.\n\n"
        f"Например: скин за ${example_skin:.0f} пройдёт, только если наклеек на нём "
        f"минимум на ${example_skin * ratio:.0f}.\n\n"
        "Проверить: /scanall"
    )


async def proxyadd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /proxyadd <адреса> — добавить прокси на ходу, без передеплоя.

    Принимает сколько угодно адресов через запятую, пробел или перевод строки —
    ровно в том виде, в каком их отдают провайдеры списком. Сохраняются в
    хранилище, поэтому переживают редеплой; список из переменной окружения при
    этом остаётся, пул складывает оба.
    """
    if not context.args:
        stored = await get_extra_proxies()
        await update.message.reply_text(
            "Добавить прокси: пришли их после команды, через запятую, пробел "
            "или с новой строки.\n\n"
            "<code>/proxyadd http://логин:пароль@хост:порт</code>\n\n"
            f"Сейчас добавлено через бота: {len(stored)}\n"
            f"Всего в пуле: {len(csfloat_client.CSFLOAT_POOL)}\n\n"
            "Убрать все добавленные: /proxyclear",
            parse_mode="HTML",
        )
        return

    raw = " ".join(context.args)
    added_cs, rejected = csfloat_client.CSFLOAT_POOL.add(raw)
    added_steam, _ = STEAM_POOL.add(raw)

    if added_cs or added_steam:
        # Сохраняем то, что реально приняли пулом, — так в хранилище не попадёт
        # мусор, который всё равно был бы отброшен при следующем старте.
        stored = await get_extra_proxies()
        known = set(stored)
        for proxy in csfloat_client.CSFLOAT_POOL.proxies:
            if proxy not in known:
                stored.append(proxy)
                known.add(proxy)
        await save_extra_proxies(stored)

    lines = [f"✅ Добавлено адресов: {added_cs}"]
    if rejected:
        lines.append(f"\n⚠️ Не приняты ({len(rejected)}):")
        for masked, problem in rejected[:5]:
            lines.append(f"  {masked} — {problem}")
    lines.append(f"\nВсего в пуле: {len(csfloat_client.CSFLOAT_POOL)}")
    lines.append("Проверить их: /proxycheck")
    await update.message.reply_text("\n".join(lines))


async def proxyclear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/proxyclear — забыть прокси, добавленные через бота (из переменной окружения останутся)."""
    stored = await get_extra_proxies()
    await save_extra_proxies([])
    await update.message.reply_text(
        f"Забыл {len(stored)} адрес(ов), добавленных через бота.\n"
        "Адреса из переменной окружения останутся — они подхватятся при перезапуске."
    )


async def proxycheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /proxycheck — какой исходящий адрес даёт каждый прокси на самом деле.

    Зачем понадобилось: в логе все семь «разных» прокси оказались одним хостом
    и портом (proxy.flameproxies.com:8989), различаясь только логином. Это
    шлюз с ротацией, и сколько за ним настоящих адресов — по конфигурации не
    видно вовсе.

    А вопрос принципиальный. Лимиты и баны и у Steam, и у CSFloat считаются по
    исходящему IP. Если за семью логинами стоит один адрес, то пул — иллюзия:
    бот думает, что у него семь независимых бюджетов, а на деле долбит один и
    тот же адрес всемером и сам себя банит. Ровно на это похожи логи, где все
    восемь запросов получили 429 в течение четырёх секунд.

    Проверяется единственным способом — спросить у внешнего сервиса, каким
    адресом мы к нему пришли.
    """
    pool = STEAM_POOL if STEAM_POOL.enabled() else csfloat_client.CSFLOAT_POOL
    if not pool.enabled():
        await update.message.reply_text("Прокси не заданы — проверять нечего.")
        return

    await update.message.reply_text(f"Проверяю {len(pool)} прокси…")

    async def ask_ip(session, proxy: str):
        async with session.get(
            "https://api.ipify.org?format=json",
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            data = await resp.json(content_type=None)
            return data.get("ip")

    async def one(proxy: str):
        # Спрашиваем ДВАЖДЫ. Один замер говорит только «адрес такой-то», а нам
        # нужно знать другое: закреплён он за логином или меняется на каждый
        # запрос. От этого зависит вся стратегия кулдаунов — при ротации
        # откладывать логин после 429 бессмысленно, потому что банится адрес, а
        # в следующий раз за тем же логином будет уже другой.
        try:
            async with aiohttp.ClientSession() as session:
                first = await ask_ip(session, proxy)
                second = await ask_ip(session, proxy)
                return proxy, first, second, None
        except Exception as e:
            return proxy, None, None, f"{type(e).__name__}: {e}"

    results = await asyncio.gather(*(one(p) for p in pool.proxies))

    ips: dict[str, int] = {}
    rotating = 0
    lines = ["<b>Исходящие адреса прокси</b>"]
    for proxy, first, second, error in results:
        if first:
            ips[first] = ips.get(first, 0) + 1
            if second and second != first:
                rotating += 1
                lines.append(
                    f"  {proxy_pool.mask(proxy)} → <code>{first}</code>, "
                    f"потом <code>{second}</code> ⟳"
                )
            else:
                lines.append(f"  {proxy_pool.mask(proxy)} → <code>{first}</code> (держится)")
        else:
            lines.append(f"  {proxy_pool.mask(proxy)} → ⚠️ {html_module.escape(error or 'нет ответа')}")

    working = len(ips)
    failed = len(results) - sum(1 for _, first, _, _ in results if first)
    on_cooldown = sum(1 for p in pool.proxies if pool.cooldown_remaining(p) > 0)

    lines.append("")
    lines.append(
        f"<b>Итого:</b> работают {len(results) - failed} из {len(results)}"
        + (f", не отвечают {failed}" if failed else "")
        + (f", на кулдауне {on_cooldown}" if on_cooldown else "")
    )

    # Отметки живости держим в пуле: /status и решения о маршруте должны знать
    # про мёртвые адреса, а не только тот, кто запустил проверку.
    for proxy, first, _second, _err in results:
        if first:
            pool.mark_alive(proxy)
        else:
            pool.mark_dead(proxy, "не ответил при проверке")

    if rotating:
        lines.append("")
        lines.append(
            f"⟳ <b>У {rotating} из {len(pool)} адрес меняется между запросами.</b>\n"
            "Значит откладывать прокси после 429 незачем — банится адрес, а "
            "следующий запрос уйдёт уже с другого. Кулдаун таким прокси только "
            "мешает, и он снижен до нескольких секунд."
        )

    unique = len(ips)
    lines.append("")
    if unique == 0:
        lines.append("⚠️ Ни один прокси не ответил — проверь доступы и остаток трафика.")
    elif unique == 1 and len(pool) > 1:
        lines.append(
            f"⚠️ <b>Все {len(pool)} прокси выходят с ОДНОГО адреса.</b>\n"
            "Значит пул не даёт ничего: лимиты и баны считаются по IP, и бот "
            "долбит один адрес всеми полосами сразу, сам себя загоняя в 429.\n\n"
            "Нужны sticky-сессии с разными исходящими адресами — у провайдера "
            "это обычно отдельные порты или логины вида user-session-1."
        )
    else:
        lines.append(f"✅ Разных адресов: {unique} из {len(pool)}.")
        duplicates = {ip: n for ip, n in ips.items() if n > 1}
        if duplicates:
            lines.append(
                "Повторяются: "
                + ", ".join(f"<code>{ip}</code> ×{n}" for ip, n in duplicates.items())
            )

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def setmarkets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setmarkets — пороги для сравнения с площадками, по-человечески.

    Без аргументов показывает текущие значения. Дальше — пара «что» и
    «сколько», например:
        /setmarkets спред 30
        /setmarkets продажи 20
        /setmarkets прибыль 5
        /setmarkets потолок 60
        /setmarkets цена 10
    """
    chat_id = update.effective_chat.id
    current = await get_market_settings(chat_id)

    def value_of(key, default):
        return current[key] if current[key] is not None else default

    if not context.args:
        await update.message.reply_text(
            "<b>Пороги для /markets</b>\n"
            f"  спред: от {value_of('min_discount', MARKETS_DEFAULT_DISCOUNT):g}%\n"
            f"  потолок спреда: {value_of('max_discount', market_prices.MAX_SANE_DISCOUNT_PCT):g}% "
            f"<i>(выше — почти всегда дефект данных)</i>\n"
            f"  продажи в Steam за сутки: от {value_of('min_volume', MARKETS_MIN_VOLUME)}\n"
            f"  прибыль: от ${value_of('min_profit', market_prices.MIN_NET_PROFIT):g}\n"
            f"  цена предмета: от ${value_of('min_price', market_prices.MIN_MARKET_PRICE):g}\n\n"
            "Поменять: <code>/setmarkets спред 30</code>\n"
            "Что можно: спред, потолок, продажи, прибыль, цена",
            parse_mode="HTML",
        )
        return

    aliases = {
        "спред": ("min_discount", "%"), "скидка": ("min_discount", "%"),
        "потолок": ("max_discount", "%"),
        "продажи": ("min_volume", "шт"), "объём": ("min_volume", "шт"),
        "прибыль": ("min_profit", "$"),
        "цена": ("min_price", "$"),
    }
    name = context.args[0].lower()
    if name not in aliases:
        await update.message.reply_text(
            f"Не знаю настройку {name!r}. Доступны: {', '.join(sorted(set(aliases)))}"
        )
        return
    if len(context.args) < 2:
        await update.message.reply_text("Не хватает значения. Пример: /setmarkets спред 30")
        return

    key, unit = aliases[name]
    try:
        raw = context.args[1].replace("%", "").replace("$", "").replace(",", ".")
        value = int(float(raw)) if key == "min_volume" else float(raw)
    except ValueError:
        await update.message.reply_text(f"{context.args[1]!r} — не число.")
        return
    if value < 0:
        await update.message.reply_text("Отрицательное значение не имеет смысла.")
        return

    await set_market_setting(chat_id, key, value)
    await update.message.reply_text(
        f"✅ {name}: {value:g} {unit}\n\nПроверить прямо сейчас: /markets"
    )


async def _verify_markets_against_steam(offers, min_discount_pct: float, min_volume: int):
    """
    Проверить находки с площадок живой ценой Steam и отсеять неликвид.

    Две задачи разом, и обе не решаются прайс-листом.

    Первая — цена. В прайс-листе это оценка, и на редких позициях она врёт
    особенно грубо: при пороге 20% верхушка выдачи состояла из скидок 90-95%,
    которых не бывает. priceoverview даёт lowest_price — то, что реально
    заплатишь.

    Вторая — ликвидность. Объёма продаж в прайс-листе нет ВООБЩЕ, а без него
    «выгода» ничего не стоит: предмет, который в Steam не продаётся, нельзя
    перепродать ни за какую цену. priceoverview возвращает volume за сутки.
    """
    if not offers:
        return []
    if STEAM_POOL.enabled() and STEAM_POOL.all_exhausted():
        log.warning("markets: все адреса пула на кулдауне после 429 — проверять нечем")
        return [], 0, len(offers)
    if not STEAM_POOL.enabled() and steam_cooldown_remaining(scope="pricing") > 0:
        log.warning("markets: Steam на кулдауне и прокси нет — проверить цены нечем")
        return [], 0, len(offers)

    # Сначала кэш — он снимает основную массу запросов, потому что кандидаты
    # от прогона к прогону повторяются.
    cached = await get_steam_prices_batch([o.market_hash_name for o in offers])
    misses = [o for o in offers if o.market_hash_name not in cached]
    fresh_budget = misses[:STEAM_LIVE_BUDGET]

    lanes = max(1, len(STEAM_POOL))
    semaphore = asyncio.Semaphore(lanes)
    log.info(
        "markets: %d кандидат(ов): в кэше %d, спрошу у Steam %d (потолок %d), полос %d",
        len(offers), len(cached), len(fresh_budget), STEAM_LIVE_BUDGET, lanes,
    )

    class Cached:
        __slots__ = ("lowest", "volume", "median")

        def __init__(self, entry):
            self.lowest, self.volume, self.median = entry["price"], entry.get("volume"), None

    async def check(offer, session):
        entry = cached.get(offer.market_hash_name)
        if entry:
            return offer, Cached(entry)
        if offer not in fresh_budget:
            return offer, None
        async with semaphore:
            try:
                live = await get_steam_market_price_retrying(session, offer.market_hash_name)
            except Exception:
                # Не .exception(): при выжженном пуле это десятки одинаковых
                # трейсбеков подряд, из-за которых настоящие ошибки не найти.
                log.info("markets: %s — Steam не ответил", offer.market_hash_name)
                return offer, None
            if live and live.lowest:
                await set_steam_price(offer.market_hash_name, live.lowest, live.volume)
            return offer, live

    verified = []
    unavailable = 0   # не смогли проверить: Steam не ответил
    rejected = 0      # проверили и отбраковали — это разные вещи

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        for offer, live in await asyncio.gather(*(check(o, session) for o in offers)):
            if live is None or not live.lowest:
                unavailable += 1
                continue

            was = offer.steam_price
            offer.apply_live_steam(live.lowest, live.volume)

            if (live.volume or 0) < min_volume:
                log.info(
                    "markets: %s — продаж за сутки %s, меньше %d: перепродать будет некому",
                    offer.market_hash_name, live.volume or 0, min_volume,
                )
                rejected += 1
                continue
            if offer.discount_pct < min_discount_pct:
                log.info(
                    "markets: %s — оценка была $%.2f, Steam на самом деле $%.2f, "
                    "скидка %.1f%% ниже порога",
                    offer.market_hash_name, was, live.lowest, offer.discount_pct,
                )
                rejected += 1
                continue
            verified.append(offer)

    log.info(
        "markets: подтвердилось %d, отбраковано %d, НЕ УДАЛОСЬ проверить %d из %d",
        len(verified), rejected, unavailable, len(offers),
    )
    # Возвращаем и то, что не удалось проверить: смешивать «проверили и не
    # подошло» с «не смогли проверить» нельзя. На проде из-за этого бот заявил
    # «ни один не подтвердился, значит это расхождения в прайс-листах» при том,
    # что все 60 запросов упали на кулдауне Steam и не проверялся никто.
    return verified, rejected, unavailable


async def markets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /markets [%] — сравнить Steam с площадками из прайс-листов csgotrader.

    Отдельный от CSFloat канал арбитража, и главное его свойство — отсутствие
    ограничений. Это статические файлы на CDN: ни ключа, ни квоты, ни банов по
    IP. Поэтому сравнивается ВЕСЬ каталог целиком, а не выборка лотов, и
    вопрос «почему только тысяча предметов» тут просто не возникает.

    Цена — агрегат по предмету, а не конкретный лот: ни флоата, ни наклеек, ни
    ссылки на лот. Для сигнала «предмет дешевле на площадке» этого хватает,
    дальше человек открывает площадку сам.
    """
    chat_id = update.effective_chat.id
    saved = await get_market_settings(chat_id)

    def setting(key, default):
        return saved[key] if saved[key] is not None else default

    # Аргумент команды перебивает сохранённый порог на один раз — удобно
    # быстро пощупать другое значение, не меняя настройку.
    try:
        threshold = (
            float(context.args[0].replace("%", "").replace(",", "."))
            if context.args
            else setting("min_discount", MARKETS_DEFAULT_DISCOUNT)
        )
    except ValueError:
        await update.message.reply_text("Порог не разобрал. Пример: /markets 25")
        return

    min_volume = int(setting("min_volume", MARKETS_MIN_VOLUME))
    max_discount = setting("max_discount", market_prices.MAX_SANE_DISCOUNT_PCT)
    min_profit = setting("min_profit", market_prices.MIN_NET_PROFIT)
    min_item_price = setting("min_price", market_prices.MIN_MARKET_PRICE)

    await update.message.reply_text(
        f"Сравниваю Steam с площадками.\n"
        f"Спред от {threshold:g}% до {max_discount:g}%, прибыль от ${min_profit:g}, "
        f"продаж в Steam от {min_volume} за сутки.\n"
        f"Пороги: /setmarkets"
    )

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        steam_prices = await get_csgotrader_prices(session)
        if not steam_prices:
            await update.message.reply_text("⚠️ Прайс-лист Steam не скачался — сравнивать не с чем.")
            return

        by_market = await market_prices.load_markets(session)
        if not by_market:
            await update.message.reply_text(
                "⚠️ Ни один файл площадок не открылся. Похоже, состав файлов на "
                "prices.csgotrader.app изменился — надо смотреть, какие есть сейчас."
            )
            return

        available = list(by_market)
        found: list = []
        for market, prices in by_market.items():
            found.extend(
                market_prices.compare(
                    steam_prices, prices, market,
                    min_discount_pct=threshold,
                    max_discount_pct=max_discount,
                    min_price=min_item_price,
                    min_net_profit=min_profit,
                    fee_multiplier=STEAM_FEE_MULTIPLIER,
                )
            )

    if not found:
        await update.message.reply_text(
            f"Проверил площадки ({', '.join(available)}) — дешевле Steam на {threshold:g}% "
            "ничего нет. Попробуй порог ниже."
        )
        return

    found.sort(key=lambda o: o.discount_pct, reverse=True)

    # Проверка у Steam. Прайс-лист даёт ОЦЕНКУ, и на копеечных и редких
    # позициях она врёт особенно сильно — на проде при пороге 20% верхушка
    # списка была со скидками 90-95%, которых не существует. Заодно только так
    # и узнаётся ликвидность: объёма продаж в прайс-листе нет вовсе.
    before = len(found)
    found, rejected, unavailable = await _verify_markets_against_steam(
        found[:MARKETS_VERIFY_LIMIT], threshold, min_volume
    )
    if not found:
        if unavailable and not rejected:
            # Ничего не проверено — значит и сказать про находки нечего.
            # Заявлять "это были расхождения в прайс-листах" тут нельзя: мы их
            # не опровергли, мы до них не дошли.
            await update.message.reply_text(
                f"Кандидатов {before}, но проверить цены у Steam не удалось "
                f"({unavailable} из {min(before, MARKETS_VERIFY_LIMIT)} запросов не прошли — "
                "Steam ограничил доступ). Это НЕ значит, что находок нет: их просто "
                "не с чем было сверить.\n\nПопробуй через несколько минут — "
                "адреса освободятся."
            )
        else:
            await update.message.reply_text(
                f"Кандидатов было {before}. Проверено у Steam: {rejected} отбраковано"
                + (f", {unavailable} не удалось проверить" if unavailable else "")
                + ".\nНи одна находка не подтвердилась живой ценой — значит это были "
                "расхождения в прайс-листах, а не выгода."
            )
        return
    found.sort(key=lambda o: o.discount_pct, reverse=True)
    lines = [
        f"🏪 Дешевле Steam — найдено {len(found)}\n"
        f"<i>Цены из прайс-листов, обновляются примерно раз в час. Это средняя цена "
        f"по предмету, а не конкретный лот: проверяй на площадке перед покупкой. "
        f"«Чистыми» — за вычетом комиссии Steam ~{(1 - STEAM_FEE_MULTIPLIER) * 100:.0f}%.</i>"
    ]
    for o in found[:15]:
        net = o.net_after_fee(STEAM_FEE_MULTIPLIER)
        lines.append(
            f"<code>{html_module.escape(o.market_hash_name)}</code>\n"
            f"  {html_module.escape(o.market)} ${o.market_price:.2f} | "
            f"Steam ${o.steam_price:.2f} | дешевле на {o.discount_pct:.1f}%\n"
            f"  чистыми при перепродаже: {'+' if net >= 0 else '-'}${abs(net):.2f}"
            f" | продаж в Steam за сутки: {o.steam_volume if o.steam_volume is not None else '?'}\n"
            f'  <a href="{o.steam_url}">Проверить в Steam</a>'
        )

    for chunk in _chunk_lines(lines, sep="\n\n"):
        await update.message.reply_text(chunk, parse_mode="HTML", disable_web_page_preview=True)


async def arbreset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /arbreset — снять кулдаун CSFloat вручную и показать, с чем мы к нему ходим.

    Кулдаун при блокировке по IP длинный (3 ч) и переживает передеплой, поэтому
    без ручного сброса нельзя проверить, помогла ли правка запроса: ждёшь не
    результат правки, а истечение таймера, который к ней отношения не имеет.
    """
    cooldown = csfloat_client.cooldown_remaining()
    await csfloat_client.reset_cooldown()
    was = f"был {cooldown / 60:.0f} мин" if cooldown > 0 else "кулдауна и не было"
    lines = [
        f"✅ Кулдаун CSFloat сброшен ({was}).",
        f"Ключ: {csfloat_client.key_fingerprint()}",
        f"User-Agent: {csfloat_client.CSFLOAT_USER_AGENT}",
        f"Маршрут: {csfloat_client.route_description()}",
    ]
    # Сброс кулдауна не добавляет квоты. Если в прошлый раз остаток был нулевой,
    # а окно ещё не сбросилось, то /arbnow сразу упрётся в тот же 429 — лучше
    # сказать это здесь, чем дать потыкать вслепую.
    budget = csfloat_client.budget_description()
    if budget:
        lines.append(f"Квота в прошлый замер: {budget}")
    lines.append("")
    lines.append("Проверить прямо сейчас: /arbnow")
    await update.message.reply_text("\n".join(lines))


async def scanall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/scanall — сканировать весь вотчлист прямо сейчас, не дожидаясь расписания."""
    chat_id = update.effective_chat.id
    sticker_items, st_a = _drop_stattrak(await get_watchlist(chat_id))
    float_items, st_b = _drop_stattrak(await get_float_watchlist(chat_id))
    items = set(sticker_items) | set(float_items)
    if not items and (st_a or st_b):
        await update.message.reply_text(
            f"В списках только StatTrak-предметы ({st_a + st_b} шт.), а они сейчас "
            "исключены из сканов. Сканировать нечего."
        )
        return
    if not items:
        await update.message.reply_text(
            "Оба списка пусты. Добавь предметы: /watchadd <предмет1>, <предмет2>, ... "
            "(охота по стикерам) или /floatadd <предмет> (охота за флоатом)."
        )
        return
    if chat_id in _watchlist_running:
        await update.message.reply_text("Скан вотчлиста уже идёт, дождись его окончания.")
        return
    cooldown = blocking_cooldown()
    if cooldown > 0:
        # Без этого лога отказ был неотличим в Render от штатного sendMessage —
        # ровно то, что сбивало с толку при разборе "почему /scanall ничего не
        # делает": в логе не было ни слова "Steam", ни слова "кулдаун".
        log.info(
            "scanall: chat_id=%s отказ — кулдаун Steam (listings) ещё %.0f мин",
            chat_id, cooldown / 60,
        )
        await update.message.reply_text(
            f"Steam на кулдауне после 429 (это временный бан IP, который продлевается от новых "
            f"попыток) — ещё {cooldown / 60:.0f} мин. Попробуй после этого."
        )
        return

    log.info("scanall: chat_id=%s начинаю скан %s предмет(ов)", chat_id, len(items))
    await update.message.reply_text(f"Начинаю скан {len(items)} предмет(ов) из вотчлиста…")
    # Ручной запуск: пауза короче фоновой — человек ждёт ответа, а разовый
    # всплеск на пару минут Steam переносит (в отличие от круглосуточного
    # потока, см. MANUAL_REQUEST_INTERVAL).
    found_any = await _run_watchlist_scan(
        context.bot, chat_id, request_interval=MANUAL_REQUEST_INTERVAL
    )
    await update.message.reply_text(
        "Готово." if found_any else "Готово, но ничего подходящего не нашлось ни по одному предмету."
    )


async def pricefile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    _pricefile_mode.add(chat_id)
    await update.message.reply_text(
        "Ок, жду прайс-лист стикеров — присылай JSON-файл(ы) с "
        "market/search/render (категория стикеров). Каждый файл сразу сольётся "
        "в общий прайс-лист.\n"
        f"Сейчас в прайс-листе {manual_prices_count()} цен.\n"
        "Когда закончишь — пришли /scan или /scanfile, чтобы вернуться к обычному сканированию."
    )


async def clearprices(update: Update, context: ContextTypes.DEFAULT_TYPE):
    count = clear_manual_prices()
    await update.message.reply_text(f"Прайс-лист стикеров очищен (было {count} записей). Можно загружать заново через /pricefile.")


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    document = update.message.document
    filename = (document.file_name or "").lower()
    try:
        tg_file = await context.bot.get_file(document.file_id)
        raw = bytes(await tg_file.download_as_bytearray())

        # PDF (частый случай на мобильных: "Печать -> Сохранить как PDF"
        # вместо обычного скачивания файла) — вытаскиваем текст через pdftotext
        # и склеиваем строки, разорванные печатным макетом, перед парсингом JSON.
        is_pdf = raw[:4] == b"%PDF" or filename.endswith(".pdf")
        if is_pdf:
            with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp_pdf:
                tmp_pdf.write(raw)
                tmp_pdf.flush()
                proc = subprocess.run(
                    ["pdftotext", "-raw", tmp_pdf.name, "-"],
                    capture_output=True,
                )
            if proc.returncode != 0:
                raise ValueError(f"не удалось извлечь текст из PDF: {proc.stderr.decode(errors='replace')}")
            text = proc.stdout.decode("utf-8", errors="replace")
            # pdftotext переносит длинные строки — склеиваем всё в одну строку,
            # JSON без переносов внутри строковых значений парсится нормально
            text = "".join(line.strip() for line in text.splitlines())
            # при печати страницы с встроенным JSON-viewer'ом браузера (Chrome/Firefox)
            # плавающая панель инструментов ("Автоформатировать", "Копировать" и т.п.)
            # попадает в текстовый слой PDF прямо посреди JSON-строк, ломая парсинг —
            # вычищаем известные подписи этой панели
            for artifact in (
                "Автоформатировать", "Свернуть все", "Развернуть все",
                "Развернуть всё", "Свернуть всё", "Необработанные данные",
                "Отфильтровать JSON", "Скопировать", "Копировать", "Сохранить как",
                "Заголовки", "Pretty Print", "Raw Data", "Save As", "Copy",
                "Collapse All", "Expand All", "Filter JSON",
            ):
                text = text.replace(artifact, "")
        else:
            text = None
            for enc in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
                try:
                    text = raw.decode(enc)
                    break
                except UnicodeDecodeError:
                    continue
            if text is None:
                raise ValueError("не удалось определить кодировку файла")
            text = text.strip()

        # если файл (HTML-страница целиком или PDF с шапкой браузера) содержит
        # что-то до/после JSON — вытаскиваем именно JSON-объект
        if not text.startswith("{"):
            start = text.find("{")
            end = text.rfind("}")
            if start == -1 or end == -1 or end <= start:
                raise ValueError("в файле не найден JSON-объект (похоже, это не тот файл)")
            text = text[start:end + 1]

        data = json.loads(text)
    except Exception as e:
        await update.message.reply_text(
            f"Не смог прочитать файл как JSON: {e}\n\n"
            f"Присылай либо сохранённый .json (Ctrl+S -> Текстовый файл), либо "
            f"PDF, сохранённый через «Печать -> Сохранить как PDF» с той же страницы."
        )
        return

    # Режим "жду прайс-лист стикеров" включается командой /pricefile —
    # больше не гадаем по форме JSON, пользователь явно говорит, что сейчас пришлёт.
    if chat_id in _pricefile_mode:
        results_list = data.get("results")
        if not (isinstance(results_list, list) and results_list and isinstance(results_list[0], dict)):
            await update.message.reply_text(
                f"Включён режим /pricefile, но в файле нет списка \"results\" с записями. "
                f"Ключи верхнего уровня в файле: {list(data.keys())}\n"
                f"Убедись, что это именно market/search/render (категория стикеров), не листинги."
            )
            return

        sample = results_list[0]
        log.info("handle_document(pricefile): поля первого элемента: %s", list(sample.keys()))

        items = {}
        skipped = 0
        for r in results_list:
            name = (
                r.get("hash_name")
                or r.get("market_hash_name")
                or (r.get("asset_description") or {}).get("market_hash_name")
                or r.get("name")
            )
            price = None
            if r.get("sell_price") is not None:
                price = r["sell_price"] / 100.0
            elif r.get("sell_price_text"):
                digits = "".join(c for c in r["sell_price_text"] if c.isdigit() or c == ".")
                try:
                    price = float(digits)
                except ValueError:
                    price = None

            if name and price is not None:
                items[html_module.unescape(name)] = price
            else:
                skipped += 1

        if not items:
            await update.message.reply_text(
                f"Не смог вытащить имя/цену ни из одной из {len(results_list)} записей. "
                f"Поля первого элемента: {list(sample.keys())}\n"
                f"Перешли это разработчику для донастройки парсинга."
            )
            return

        merged = ingest_manual_prices(items)
        total_count = data.get("total_count", len(results_list))
        start = data.get("start", 0)
        msg = (
            f"Прайс-лист стикеров: сохранено {len(items)} цен ({merged} новых/изменённых), "
            f"всего в прайс-листе теперь {manual_prices_count()}. "
            f"Это позиции {start}-{start + len(results_list)} из {total_count} по этому запросу.\n"
            f"Пришли следующую страницу (start={start + len(results_list)}) — режим /pricefile всё ещё активен.\n"
            f"Закончил — пришли /scan или /scanfile, чтобы вернуться к обычному сканированию."
        )
        if skipped:
            msg += f"\n({skipped} записей пропущено — не нашлось имени/цены)"
        await update.message.reply_text(msg)
        return

    total_count = data.get("total_count", 0)
    html = data.get("results_html", "")
    # используем start/pagesize, которые прислал сам Steam в файле — так можно
    # стартовать с любой страницы, а не только с первой (start=0)
    page_start = data.get("start", 0)
    page_size = data.get("pagesize", RENDER_COUNT)
    new_listings = _parse_listings_html(html)

    session = _file_sessions.get(chat_id)
    if session is None:
        # файл прислан без /scanfile — заводим сессию сама на лету, доставая
        # название предмета прямо из присланного HTML (нужно только для
        # ссылки на следующую страницу; параметры фильтра — по умолчанию)
        name_m = re.search(r'market_listing_item_name"[^>]*>([^<]+)<', html)
        market_hash_name = html_module.unescape(name_m.group(1)).strip() if name_m else None
        def_min, def_max = await _get_defaults(chat_id)
        session = {
            "market_hash_name": market_hash_name,
            "min_value": def_min,
            "max_markup": def_max,
            "listings": [],
            "next_start": 0,
            "total_count": None,
        }
        _file_sessions[chat_id] = session
        await update.message.reply_text(
            f"Файл принят без команды, начинаю сбор «{market_hash_name or 'предмет'}» "
            f"с параметрами по умолчанию (мин$ стикеров={def_min:.0f}, макс наценка={def_max:.0f}%).\n"
            f"Сменить дефолты: /setdefaults <мин$> <макс%>. "
            f"Или задать разово: /scanfile <ссылка> [мин$] [макс%]."
        )

    session["listings"].extend(new_listings)
    session["total_count"] = total_count
    session["next_start"] = page_start + page_size

    got = len(session["listings"])

    # парсим сразу же, не дожидаясь остальных страниц — с каждым новым файлом
    # список офферов пересчитывается заново на всех собранных к этому моменту лотах
    await _run_analysis(
        update, session["listings"], session["min_value"], session["max_markup"], session["market_hash_name"]
    )

    if session["next_start"] >= total_count:
        await update.message.reply_text(f"Все лоты собраны ({got} из {total_count}).")
        del _file_sessions[chat_id]
    elif session["market_hash_name"]:
        next_url = render_url(session["market_hash_name"], session["next_start"])
        second_start = session["next_start"] + RENDER_COUNT
        lines = [f"Собрано {got} из {total_count}. Когда будет время — вот следующая страница:\n{next_url}"]
        if second_start < total_count:
            second_url = render_url(session["market_hash_name"], second_start)
            lines.append(f"\nИ ещё одна следом, чтобы не бегать по одной:\n{second_url}")
        lines.append("\n(необязательно сразу — можно прислать в любой момент).")
        await update.message.reply_text("".join(lines))
    else:
        await update.message.reply_text(
            f"Собрано {got} из {total_count}, но не смог понять название предмета из файла, "
            f"чтобы дать ссылку на следующую страницу. Начни через /scanfile <ссылка>."
        )


def _fmt_mins(seconds: float) -> str:
    """Человеческая длительность: секунды не нужны, часы читаются лучше минут."""
    if seconds <= 0:
        return "сейчас"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.0f} мин"
    return f"{minutes / 60:.1f} ч"


def _next_run_in(job_queue, name: str) -> float | None:
    """Сколько секунд до следующего запуска джобы, либо None если её нет."""
    jobs = job_queue.get_jobs_by_name(name) if job_queue else []
    if not jobs:
        return None
    try:
        return max(0.0, (jobs[0].next_t - dt.datetime.now(dt.timezone.utc)).total_seconds())
    except Exception:
        return None


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /status — одна сводка: что запущено, что на паузе, что на кулдауне, какие
    пороги отбора действуют.

    Появилась потому, что почти все вопросы при отладке были вида "почему
    ничего не приходит", а ответ приходилось собирать из логов Render: списки —
    в одной команде, пауза — во второй, кулдауны — вообще нигде. Здесь всё
    сразу и в одном месте.
    """
    chat_id = update.effective_chat.id
    jq = context.job_queue

    sticker_items = await get_watchlist(chat_id)
    float_items = await get_float_watchlist(chat_id)
    paused = await get_watch_paused(chat_id)
    interval = await _get_watch_interval(chat_id)
    arb = await get_arb_settings(chat_id)
    def_min, def_max = await _get_defaults(chat_id)
    streak = await get_streak_markup(chat_id)
    p_lo, p_hi = await get_price_filter(chat_id)
    f_lo, f_hi = await get_float_filter(chat_id)
    f_markup = await get_float_markup(chat_id)

    lines = ["📊 <b>Состояние</b>", ""]

    # --- Списки и автоскан -------------------------------------------------
    lines.append("<b>Списки</b>")
    lines.append(f"  Вотчлист (стикеры): {len(sticker_items)}")
    lines.append(f"  Охота за флоатом: {len(float_items)}")
    if not sticker_items and not float_items:
        lines.append("  ⚠️ оба пусты — сканировать нечего (/watchadd, /floatadd)")

    lines.append("")
    lines.append("<b>Автоскан Steam</b>")
    if paused:
        lines.append("  ⏸ на паузе — /watchresume чтобы включить")
    else:
        nxt = _next_run_in(jq, f"{WATCHLIST_JOB_PREFIX}{chat_id}")
        when = f"следующий прогон через {_fmt_mins(nxt)}" if nxt is not None else "прогон не запланирован"
        lines.append(f"  ▶️ включён, {when} (интервал {interval:g} мин)")

    # --- Арбитраж ----------------------------------------------------------
    lines.append("")
    lines.append("<b>Арбитраж CSFloat</b>")
    if arb["min_discount"] is None:
        lines.append("  ⏹ выключен — /setarb 5 чтобы включить")
    else:
        nxt = _next_run_in(jq, f"{ARB_JOB_PREFIX}{chat_id}")
        when = f"проверка через {_fmt_mins(nxt)}" if nxt is not None else "проверка не запланирована"
        lines.append(f"  ▶️ включён: дешевле Steam на {arb['min_discount']:g}%+, {when}")
        if arb["sticker_markup"] is not None:
            lines.append(f"  наклейки: наценка ≤{arb['sticker_markup']:g}%")
        if arb["min_price"] is not None or arb["max_price"] is not None:
            lo = f"${arb['min_price']:.2f}" if arb["min_price"] is not None else "—"
            hi = f"${arb['max_price']:.2f}" if arb["max_price"] is not None else "—"
            lines.append(f"  цена лота: {lo} … {hi}")
        if arb["min_volume"] is not None:
            lines.append(f"  ликвидность: от {arb['min_volume']} продаж на Steam")

    # --- Доступ к площадкам ------------------------------------------------
    # Ровно то, чего не хватало: "почему /scanall отказывает" и "почему
    # арбитраж молчит" — оба ответа тут.
    lines.append("")
    lines.append("<b>Доступ к площадкам</b>")
    for scope, label in (("listings", "Steam, листинги"), ("pricing", "Steam, цены стикеров")):
        cd = steam_cooldown_remaining(scope)
        lines.append(f"  {label}: " + (f"⏸ кулдаун ещё {_fmt_mins(cd)}" if cd > 0 else "✅ свободен"))
    cf = csfloat_client.cooldown_remaining()
    if not csfloat_client.csfloat_enabled():
        lines.append("  CSFloat: ⚠️ нет ключа (CSFLOAT_API_KEY)")
    else:
        lines.append("  CSFloat: " + (f"⏸ кулдаун ещё {_fmt_mins(cf)} (/arbreset)" if cf > 0 else "✅ свободен"))
        lines.append(f"  маршрут CSFloat: {csfloat_client.route_description()}")
        proxy_problem = csfloat_client.http_proxy_problem()
        if proxy_problem:
            lines.append(f"  ⚠️ прокси настроен неверно: {proxy_problem}")
        if STEAM_POOL.enabled():
            lines.append(f"  прокси для Steam: {STEAM_POOL.describe()}")
        # Остаток квоты — то самое число, которое отличает «порог слишком
        # строгий» от «скан не дошёл до данных». Пока его не было видно, эти
        # два случая выглядели в чате одинаково: бот просто молчал.
        budget = csfloat_client.budget_description()
        if budget:
            lines.append(f"  квота CSFloat: {budget}")

    # --- Пороги отбора -----------------------------------------------------
    lines.append("")
    lines.append("<b>Пороги отбора</b>")
    lines.append(f"  Стикеры: от ${def_min:.0f}, наценка ≤{def_max:g}%")
    if streak is not None:
        lines.append(f"  Стрик ({STREAK_THRESHOLD}+ подряд): наценка ≤{streak:g}%")
    ratio = await get_sticker_ratio(chat_id)
    if ratio is not None:
        lines.append(f"  Вес наклеек: дороже скина в {ratio:g} раз")
    if p_lo is not None or p_hi is not None:
        lo = f"${p_lo:.2f}" if p_lo is not None else "—"
        hi = f"${p_hi:.2f}" if p_hi is not None else "—"
        lines.append(f"  Цена лота: {lo} … {hi}")
    if f_lo is None or f_hi is None:
        lines.append("  Флоат: ⚠️ порог не задан — охота за флоатом не идёт (/setfloatfilter 0.01 0.99)")
    else:
        extra = f", наценка ≤{f_markup:g}%" if f_markup is not None else ""
        lines.append(f"  Флоат: ≤{f_lo:g} или ≥{f_hi:g}{extra}")

    for chunk in _chunk_lines(lines):
        await update.message.reply_text(chunk, parse_mode="HTML")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /start — короткая карточка на один экран. Полный справочник вынесен в
    /help: раньше всё жило в одном сообщении, и оно разрослось так, что
    прочитать его целиком было невозможно, а /scanall терялся в середине.
    """
    await update.message.reply_text(
        "Привет! Я слежу за скинами CS2 и присылаю выгодные лоты.\n\n"
        "▶️ <b>Начать</b>\n"
        "/status — что сейчас происходит: списки, автоскан, кулдауны, пороги\n"
        "/scanall — проверить оба списка прямо сейчас\n"
        "/scan [предмет] — разовая проверка одного предмета\n\n"
        "📋 <b>Списки</b> (по ним идёт автоскан)\n"
        "/watchadd, /watchdel, /watchlist — охота по стикерам\n"
        "/floatadd, /floatdel, /floatlist — охота за редким флоатом\n"
        "/watchpause, /watchresume — пауза автоскана\n\n"
        "💱 <b>Арбитраж CSFloat ↔ Steam</b>\n"
        "/setarb [мин%] — включить, /arbnow — проверить сейчас\n\n"
        "⚙️ <b>Пороги</b>: /setdefaults, /setfloatfilter и другие — см. /help\n\n"
        "📖 <b>/help</b> — полный справочник со всеми командами и пояснениями",
        parse_mode="HTML",
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/help — полный справочник. Длинный намеренно: это место, куда идут за деталями."""
    chat_id = update.effective_chat.id
    def_min, def_max = await _get_defaults(chat_id)
    text = (
        "Привет! Пришли:\n"
        "/scan <ссылка или название предмета> [мин$ стикеров] [макс наценка%]\n"
        "Название — на английском, с | или без: /scan AK-47 | Slate или /scan AK-47 Slate\n"
        "Бот сам сходит в Steam за листингами и посчитает офферы.\n\n"
        "/scanfile — резервный ручной путь, если автоматический запрос не удался "
        "(например, IP временно на кулдауне после 429 у Steam): бот пришлёт ссылку "
        "на JSON, открой её в браузере, сохрани (Ctrl+S) и пришли файл сюда — можно "
        "и без команды, просто скинуть файл.\n\n"
        "/pricefile — загрузить прайс-лист цен на стикеры вручную (Steam market/search JSON), "
        "/clearprices — очистить его перед обновлением.\n\n"
        "/watchadd <предмет1>, <предмет2>, ... — добавить предметы в вотчлист на автоскан\n"
        "/watchdel <номер или название> — убрать предмет из вотчлиста\n"
        "/watchclear — полностью очистить вотчлист\n"
        f"/watchlist — показать вотчлист; следующий прогон стартует через {WATCH_GAP_MINUTES:g} мин "
        "после конца предыдущего (сам прогон списка уже безопасно троттлится по времени, так что "
        "отдельно ждать дольше для больших списков не нужно); бот пришлёт сообщение, только если "
        "найдёт новые офферы (один и тот же лот повторно не пришлёт в течение 5 часов).\n"
        "/scanall — сканировать оба списка прямо сейчас, не дожидаясь расписания.\n"
        "/watchpause — остановить автоскан (список сохраняется), /watchresume — возобновить.\n\n"
        "🔍 Охота за редким флоатом идёт по ОТДЕЛЬНОМУ списку — флоат имеет смысл искать на "
        "конкретных скинах, а не по всему вотчлисту:\n"
        "/floatadd <предмет1>, <предмет2>, ... — добавить предметы в охоту за флоатом\n"
        "/floatdel <номер или название> — убрать, /floatclear — очистить весь список\n"
        "/floatlist — показать список и текущее условие отбора\n"
        "(предмет может быть в обоих списках — тогда за прогон он тянется из Steam один раз "
        "и проверяется сразу по обоим критериям)\n\n"
        f"/setdefaults <мин$> <макс%> — поменять значения по умолчанию "
        f"(сейчас: {def_min:.0f}$ / {def_max:.0f}%).\n"
        f"/setstreakmarkup <%> — отдельный порог наценки для лотов с {STREAK_THRESHOLD}+ "
        f"подряд одинаковыми стикерами (\"стрик\").\n"
        f"/setpricefilter <мин$> <макс$> — показывать только лоты в этом диапазоне цены "
        f"(со стикерами); /setpricefilter off — убрать фильтр.\n"
        f"/setfloatfilter <низкий> <высокий> — искать лоты с редким флоатом (близко к 0 — топ "
        f"для Factory New, близко к 1 — топ для Battle-Scarred), не связано со стикерами; "
        f"/setfloatfilter off — убрать фильтр.\n"
        f"/setfloatmarkup <макс%> — показывать флоат-находку, только если её цена не больше "
        f"чем на макс% выше самого дешёвого лота этого предмета (иначе продавец уже в курсе "
        f"и заложил редкость в цену); /setfloatmarkup off — без ограничения по цене.\n\n"
        "💱 Арбитраж CSFloat ↔ Steam (по умолчанию выключен, вотчлист не нужен — "
        "сканируется весь рынок CSFloat):\n"
        "/setarb <мин%> — включить: слать лоты, которые на CSFloat дешевле цены Steam "
        "минимум на мин%; /setarb off — выключить\n"
        "/setarbprice <мин$> <макс$> — ограничить диапазон цены лота\n"
        "/setarbvolume <шт> — только ликвидное (сколько продаётся на Steam)\n"
        "/setarbstickers <макс%> — ещё и лоты, где наклейки достаются почти даром\n"
        "/arbnow — проверить прямо сейчас\n"
        "/arbreset — снять кулдаун CSFloat и показать, чем мы к нему стучимся\n"
        f"Учти: выручка от продажи в Steam приходит на кошелёк и не выводится, "
        f"поэтому в сообщениях показываю «чистыми» с учётом комиссии "
        f"~{(1 - STEAM_FEE_MULTIPLIER) * 100:.0f}%."
    )
    # Справочник давно перерос лимит Telegram в 4096 символов — режем на части
    # тем же helper'ом, что и списки (см. _chunk_lines).
    for chunk in _chunk_lines(text.split("\n\n"), sep="\n\n"):
        await update.message.reply_text(chunk)


# Меню команд Telegram (то, что видно при вводе "/"). Здесь НЕ все команды:
# их 27, и списком в четыре экрана пользоваться невозможно. Оставлены те, что
# нужны регулярно, в порядке реального сценария — сначала "посмотреть и
# проверить", потом списки, потом настройка. Остальные продолжают работать и
# перечислены в /help: команда, которой нет в меню, не перестаёт существовать.
BOT_COMMANDS = [
    BotCommand("status", "Что сейчас происходит: списки, автоскан, кулдауны"),
    BotCommand("scanall", "Проверить оба списка прямо сейчас"),
    BotCommand("scan", "Проверить один предмет"),
    BotCommand("watchlist", "Показать вотчлист (стикеры)"),
    BotCommand("watchadd", "Добавить предмет(ы) в вотчлист"),
    BotCommand("watchdel", "Убрать предмет из вотчлиста"),
    BotCommand("floatlist", "Показать список охоты за флоатом"),
    BotCommand("floatadd", "Добавить предмет в охоту за флоатом"),
    BotCommand("floatdel", "Убрать предмет из охоты за флоатом"),
    BotCommand("watchpause", "Остановить автоскан"),
    BotCommand("watchresume", "Возобновить автоскан"),
    BotCommand("arbnow", "Проверить арбитраж CSFloat прямо сейчас"),
    BotCommand("markets", "Сравнить Steam с другими площадками (весь каталог)"),
    BotCommand("setmarkets", "Пороги для /markets: спред, продажи, прибыль"),
    BotCommand("proxycheck", "Проверить прокси: сколько работают, сколько отвалились"),
    BotCommand("setinterval", "Пауза между прогонами автоскана"),
    BotCommand("setratio", "Наклейки дороже скина во сколько раз"),
    BotCommand("proxyadd", "Добавить прокси прямо из чата"),
    BotCommand("setarb", "Арбитраж: CSFloat дешевле Steam на N%"),
    BotCommand("help", "Полный справочник по всем командам"),
]

# Команды, которых нет в меню, но которые работают. Держим списком имён, чтобы
# /help мог свериться и не забыть про них при добавлении новых.
_UNLISTED_COMMANDS = (
    "start", "help", "scanfile", "watchclear", "floatclear",
    "arbreset", "setarbprice", "setarbvolume", "setarbstickers",
    "setdefaults", "setstreakmarkup", "setpricefilter",
    "setfloatfilter", "setfloatmarkup", "pricefile", "clearprices",
)


async def _on_startup(app: Application):
    await app.bot.set_my_commands(BOT_COMMANDS)

    # ДО всего остального, что может тронуть Steam (prewarm, автоскан) —
    # восстанавливаем кулдаун после 429, если рестарт застал его активным.
    # Без этого бот "забывал" бы про ещё не снятый бан на каждом рестарте
    # процесса и тут же пробовал снова, продлевая реальный бан.
    await load_persisted_cooldown()
    await csfloat_client.load_persisted_cooldown()
    # Прокси, добавленные через /proxyadd, живут в хранилище — без этого они
    # пропадали бы при каждом редеплое, а Render передеплоивает часто.
    try:
        stored_proxies = await get_extra_proxies()
    except Exception:
        log.exception("не смог прочитать сохранённые прокси")
        stored_proxies = []
    if stored_proxies:
        raw = " ".join(stored_proxies)
        added_cs, _ = csfloat_client.CSFLOAT_POOL.add(raw)
        added_steam, _ = STEAM_POOL.add(raw)
        log.info(
            "прокси: из хранилища добавлено %d в пул CSFloat и %d в пул Steam",
            added_cs, added_steam,
        )

    if csfloat_client.csfloat_enabled():
        log.info("csfloat: маршрут — %s", csfloat_client.route_description())
        _warn_if_over_budget()
        proxy_problem = csfloat_client.http_proxy_problem()
        if proxy_problem:
            log.error("csfloat: CSFLOAT_HTTP_PROXY задан неверно — %s", proxy_problem)

    asyncio.create_task(prewarm_loop())

    # Восстанавливаем джобы автоскана вотчлиста после рестарта/редеплоя —
    # без этого расписание жило бы только в памяти процесса и слетало каждый раз.
    chat_ids = await all_watchlist_chat_ids()
    restored = 0
    for chat_id in chat_ids:
        # джоба нужна, если непуст хоть один из списков — обычный или флоатовый
        if not await get_watchlist(chat_id) and not await get_float_watchlist(chat_id):
            continue
        if await get_watch_paused(chat_id):
            # /watchpause переживает редеплой: не воскрешаем джобу для чата,
            # который сам её остановил.
            log.info("watchlist: chat_id=%s на паузе, джобу не восстанавливаю", chat_id)
            continue
        interval = await _get_watch_interval(chat_id)
        _schedule_watchlist_job(app.job_queue, chat_id, interval)
        restored += 1
    log.info("watchlist: восстановлены джобы автоскана для %d чат(ов)", restored)

    # Джобы арбитража живут отдельно от вотчлиста: он не привязан к списку
    # предметов, поэтому и чаты берём по наличию настроек, а не по вотчлисту.
    arb_restored = 0
    for chat_id in await all_chat_ids_with_settings():
        settings = await get_arb_settings(chat_id)
        if settings["min_discount"] is None:
            continue
        _schedule_arb_job(app.job_queue, chat_id)
        arb_restored += 1
    if arb_restored:
        log.info("arb: восстановлены джобы арбитража для %d чат(ов)", arb_restored)


# ---------------------------------------------------------------------------
# Приём апдейтов: webhook вместо long-polling
# ---------------------------------------------------------------------------
# Зачем: Render делает zero-downtime деплой — поднимает новый контейнер и лишь
# потом гасит старый. Telegram же допускает только ОДНОГО читателя getUpdates
# на токен, поэтому в окне пересечения оба контейнера дерутся за апдейты, и
# проигравший падает с "Conflict: terminated by other getUpdates request".
# В логах это повторялось на каждом релизе. Webhook снимает проблему на корню:
# инициатива у Telegram, драться не за что.
#
# Почему не Application.run_webhook: его tornado-сервер регистрирует ровно один
# обработчик — POST на путь вебхука. Любой GET (в т.ч. "/") получит 404, то
# есть health-check, которым UptimeRobot не даёт бесплатному сервису заснуть,
# просто исчезнет. Поэтому апдейты принимает наш собственный HTTP-сервер,
# который и так слушает $PORT ради health-check.
#
# WEBHOOK_BASE_URL пуст (по умолчанию) — работаем на long-polling, как раньше.
# Это и способ отката: снял переменную, передеплоил, вернулся к polling —
# PTB при старте polling всегда сам зовёт delete_webhook, так что подвисшая
# подписка не помешает.
# Render сам сообщает адрес сервиса в RENDER_EXTERNAL_URL. Используем его как
# значение по умолчанию — и как проверку того, что задано вручную.
#
# Зачем. При переезде на новый сервис адрес меняется, а WEBHOOK_BASE_URL
# остаётся от старого. Бот при этом стартует нормально, setWebhook проходит
# успешно — и молча просит Telegram слать апдейты на ЧУЖОЙ хост. Снаружи это
# выглядит как "бот не работает" без единой ошибки в логе; ровно так и вышло
# 2026-08-22, хотя предупреждение об этом было написано в render.yaml.
#
# Полагаться на то, что человек заметит расхождение двух URL в логе, оказалось
# наивно. Теперь при пустой переменной адрес берётся автоматически, а при
# заданном и не совпадающем — громкая ошибка в логе с готовым решением.
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
WEBHOOK_BASE_URL = os.environ.get("WEBHOOK_BASE_URL", "").rstrip("/") or RENDER_EXTERNAL_URL


def _warn_if_webhook_url_foreign() -> None:
    """Сказать вслух, если webhook настроен на адрес не этого сервиса."""
    if not WEBHOOK_BASE_URL or not RENDER_EXTERNAL_URL:
        return
    if WEBHOOK_BASE_URL == RENDER_EXTERNAL_URL:
        return
    log.error(
        "WEBHOOK_BASE_URL указывает на ЧУЖОЙ адрес: задано %s, а этот сервис живёт на %s. "
        "Telegram будет слать апдейты туда, и бот не получит НИ ОДНОГО сообщения. "
        "Исправление: убрать переменную WEBHOOK_BASE_URL совсем (адрес подставится сам) "
        "или вписать в неё %s",
        WEBHOOK_BASE_URL, RENDER_EXTERNAL_URL, RENDER_EXTERNAL_URL,
    )
# Секрет в заголовке X-Telegram-Bot-Api-Secret-Token: Telegram шлёт его с
# каждым апдейтом, и это единственный способ отличить настоящий апдейт от
# чужого POST'а на наш URL. Необязателен, но без него мы верим кому угодно.
TG_WEBHOOK_SECRET = os.environ.get("TG_WEBHOOK_SECRET", "").strip()

# Путь вебхука не угадать со стороны, но и токен в URL не светим (он попадал бы
# в логи Render и в заголовки Referer): берём необратимый хэш от токена.
def _webhook_path(token: str) -> str:
    return "tg/" + hashlib.sha256(token.encode()).hexdigest()[:32]


# Заполняются перед стартом приёма апдейтов; до этого момента webhook отвечает
# 503 — сервер слушает порт раньше, чем поднимается Application.
_tg_application = None
_tg_loop: asyncio.AbstractEventLoop | None = None
_tg_webhook_path = ""


class _HealthHandler(BaseHTTPRequestHandler):
    """
    Health-check (GET/HEAD на что угодно) плюс приём апдейтов Telegram
    (POST на _tg_webhook_path), если включён режим webhook.
    """

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def do_HEAD(self):
        # Без этого HEAD-запросы (от UptimeRobot/прокси Render) ловят 501
        # Not Implemented из BaseHTTPRequestHandler по умолчанию, хотя бот жив.
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        if not _tg_webhook_path or self.path.lstrip("/") != _tg_webhook_path:
            self._reply(404, b"not found")
            return
        if TG_WEBHOOK_SECRET and self.headers.get(
            "X-Telegram-Bot-Api-Secret-Token"
        ) != TG_WEBHOOK_SECRET:
            log.warning("webhook: POST с неверным секретом, игнорирую")
            self._reply(403, b"forbidden")
            return
        if _tg_application is None or _tg_loop is None:
            self._reply(503, b"not ready")
            return

        try:
            length = int(self.headers.get("Content-Length") or 0)
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            update = Update.de_json(payload, _tg_application.bot)
        except Exception:
            log.exception("webhook: не смог разобрать апдейт")
            self._reply(400, b"bad request")
            return

        # HTTP-сервер живёт в отдельном потоке, а очередь апдейтов — в
        # asyncio-цикле бота, поэтому кладём через run_coroutine_threadsafe.
        # Отвечаем Telegram сразу, не дожидаясь обработки: он ретраит по
        # таймауту, а обработка предмета может занять минуты.
        try:
            asyncio.run_coroutine_threadsafe(
                _tg_application.update_queue.put(update), _tg_loop
            )
        except Exception:
            log.exception("webhook: не смог поставить апдейт в очередь")
            self._reply(500, b"error")
            return
        self._reply(200, b"ok")

    def _reply(self, code: int, body: bytes):
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # не засоряем логи health-чеками от UptimeRobot


def _start_health_server():
    """
    Render (Web Service) ждёт, что процесс слушает $PORT — без этого он решит,
    что деплой не удался, и будет перезапускать контейнер. Плюс сюда же будет
    стучаться UptimeRobot, чтобы бесплатный сервис не засыпал по бездействию,
    и (в режиме webhook) сам Telegram с апдейтами.
    Работает в отдельном потоке, чтобы не мешать asyncio-циклу python-telegram-bot.

    ThreadingHTTPServer, а не HTTPServer: обычный обрабатывает запросы строго
    по одному, и тогда health-check от UptimeRobot вставал бы в очередь за
    апдейтами Telegram (и наоборот). Для health-check это означало бы ложное
    "сервис не отвечает".
    """
    port = int(os.environ.get("PORT", 8080))
    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("health-check сервер слушает порт %d", port)


# Telegram принимает секрет только из этих символов, 1-256 длиной. Несоблюдение
# он ловит на set_webhook ошибкой "Secret token contains unallowed characters" —
# и раньше это роняло весь процесс, а Render уходил в цикл перезапусков. Лучше
# проверить самим и сказать понятным текстом, что именно не так.
_WEBHOOK_SECRET_RE = re.compile(r"^[A-Za-z0-9_-]{1,256}$")


def _webhook_secret_problem() -> str | None:
    """Что не так с TG_WEBHOOK_SECRET, либо None если всё в порядке (в т.ч. если он не задан)."""
    if not TG_WEBHOOK_SECRET:
        return None
    if _WEBHOOK_SECRET_RE.match(TG_WEBHOOK_SECRET):
        return None

    # Разбираем причину отдельно: при слишком длинном, но корректном по символам
    # секрете список "плохих символов" пуст, и сообщение про символы сбивало бы с толку.
    bad = sorted({c for c in TG_WEBHOOK_SECRET if not re.match(r"[A-Za-z0-9_-]", c)})
    if bad:
        return (
            f"TG_WEBHOOK_SECRET содержит символы, которых Telegram не принимает: {bad}. "
            f"Разрешены только латинские буквы, цифры, _ и - (длина 1-256)."
        )
    return (
        f"TG_WEBHOOK_SECRET длиной {len(TG_WEBHOOK_SECRET)} — Telegram принимает от 1 до 256 символов."
    )


async def _run_with_webhook(app, token: str) -> bool:
    """
    Приём апдейтов через webhook: Telegram сам шлёт их на наш HTTPS-адрес.
    Application ведём вручную (initialize/start), потому что апдейты в очередь
    кладёт наш HTTP-сервер, а не Updater — см. комментарий у WEBHOOK_BASE_URL.

    Возвращает False, если поднять webhook не удалось — тогда main() уходит на
    long-polling. Падать нельзя: Render перезапускает процесс на любой выход, и
    ошибка конфигурации превращается в бесконечный цикл перезапусков вместо
    работающего (пусть и на polling) бота.
    """
    global _tg_application, _tg_loop, _tg_webhook_path

    _warn_if_webhook_url_foreign()
    url = f"{WEBHOOK_BASE_URL}/{_webhook_path(token)}"
    await app.initialize()

    # post_init ПРИХОДИТСЯ звать руками — и это не перестраховка.
    #
    # Application.initialize() его НЕ вызывает; в PTB это делают только
    # run_polling() и run_webhook(), что прямо написано в докстринге метода:
    # "Does *not* call post_init - that is only done by run_polling and
    # run_webhook". Мы ведём Application вручную, потому что апдейты в очередь
    # кладёт наш HTTP-сервер, а не Updater, — значит и post_init на нас.
    #
    # Здесь раньше стоял комментарий "здесь же отрабатывает post_init", и он
    # был просто неверен. Ценой этого в webhook-режиме молча не работало ВСЁ
    # содержимое _on_startup: не восстанавливались кулдауны Steam и CSFloat
    # после рестарта (бот заново долбился в забаненный адрес), не поднимался
    # prewarm, не поднималось меню команд и, заметнее всего, не восстанавливались
    # джобы автоскана — и вотчлиста, и арбитража. Ручной /setarb ставил джобу в
    # памяти процесса, но любой редеплой её терял, а Render передеплоивает часто,
    # так что автоскан фактически не работал никогда.
    #
    # Ошибка в восстановлении не должна ронять процесс: Render перезапускает
    # бота на любой выход, и одна неудачная джоба превратилась бы в цикл
    # перезапусков вместо работающего бота.
    if app.post_init:
        try:
            await app.post_init(app)
        except Exception:
            log.exception("webhook: post_init упал — бот поднимется без восстановленного состояния")

    try:
        await app.bot.set_webhook(
            url=url,
            secret_token=TG_WEBHOOK_SECRET or None,
            allowed_updates=Update.ALL_TYPES,
        )
    except Exception:
        log.exception("webhook: set_webhook не удался, откатываюсь на long-polling")
        await app.shutdown()
        return False

    await app.start()

    # Публикуем состояние для HTTP-потока только после старта Application:
    # до этого POST'ы должны получать 503, а не падать на полуготовом объекте.
    _tg_application, _tg_loop = app, asyncio.get_running_loop()
    _tg_webhook_path = _webhook_path(token)

    log.info(
        "Режим webhook: апдейты принимаются на %s (секрет %s)",
        url, "задан" if TG_WEBHOOK_SECRET else "НЕ задан — стоит задать TG_WEBHOOK_SECRET",
    )

    try:
        await asyncio.Event().wait()  # работаем, пока процесс не погасят
    finally:
        _tg_application = None
        await app.stop()
        await app.shutdown()
    return True


def _build_application(token: str):
    """
    Собрать Application со всеми обработчиками. Отдельной функцией, потому что
    при откате с webhook на polling объект приходится строить заново: он уже
    прошёл initialize()/shutdown(), а run_polling() зовёт initialize() сам, и
    дважды инициализировать один и тот же Application нельзя.
    """
    app = Application.builder().token(token).post_init(_on_startup).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("scan", scan))
    app.add_handler(CommandHandler("scanfile", scanfile))
    app.add_handler(CommandHandler("setdefaults", setdefaults))
    app.add_handler(CommandHandler("setstreakmarkup", setstreakmarkup))
    app.add_handler(CommandHandler("setpricefilter", setpricefilter))
    app.add_handler(CommandHandler("setfloatfilter", setfloatfilter))
    app.add_handler(CommandHandler("setfloatmarkup", setfloatmarkup))
    app.add_handler(CommandHandler("pricefile", pricefile))
    app.add_handler(CommandHandler("clearprices", clearprices))
    app.add_handler(CommandHandler("watchadd", watchadd))
    app.add_handler(CommandHandler("watchdel", watchdel))
    app.add_handler(CommandHandler("watchclear", watchclear))
    app.add_handler(CommandHandler("floatadd", floatadd))
    app.add_handler(CommandHandler("floatdel", floatdel))
    app.add_handler(CommandHandler("floatlist", floatlist_cmd))
    app.add_handler(CommandHandler("floatclear", floatclear))
    app.add_handler(CommandHandler("watchlist", watchlist_cmd))
    app.add_handler(CommandHandler("watchpause", watchpause))
    app.add_handler(CommandHandler("watchresume", watchresume))
    app.add_handler(CommandHandler("scanall", scanall))
    app.add_handler(CommandHandler("setarb", setarb))
    app.add_handler(CommandHandler("setarbprice", setarbprice))
    app.add_handler(CommandHandler("setarbvolume", setarbvolume))
    app.add_handler(CommandHandler("setarbstickers", setarbstickers))
    app.add_handler(CommandHandler("arbnow", arbnow))
    app.add_handler(CommandHandler("markets", markets))
    app.add_handler(CommandHandler("setmarkets", setmarkets))
    app.add_handler(CommandHandler("proxycheck", proxycheck))
    app.add_handler(CommandHandler("setinterval", setinterval))
    app.add_handler(CommandHandler("setratio", setratio))
    app.add_handler(CommandHandler("proxyadd", proxyadd))
    app.add_handler(CommandHandler("proxyclear", proxyclear))
    app.add_handler(CommandHandler("arbreset", arbreset))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_selection))
    return app


def main():
    token = os.environ["TG_BOT_TOKEN"]
    _start_health_server()
    app = _build_application(token)

    use_webhook = bool(WEBHOOK_BASE_URL)
    if use_webhook:
        problem = _webhook_secret_problem()
        if problem:
            # Не падаем: Render перезапускает процесс на любой выход, и опечатка
            # в переменной окружения превратилась бы в цикл перезапусков вместо
            # работающего бота. Работаем на polling и громко говорим, что чинить.
            log.error("%s Webhook не включён, работаю на long-polling.", problem)
            use_webhook = False

    if use_webhook:
        use_webhook = asyncio.run(_run_with_webhook(app, token))
        if not use_webhook:
            # set_webhook не прошёл — Application уже выключен, строим заново:
            # run_polling() сам зовёт initialize(), а дважды инициализировать
            # один и тот же объект нельзя.
            app = _build_application(token)

    if not use_webhook:
        # Long-polling — прежнее поведение и путь отката. PTB при старте
        # polling сам зовёт delete_webhook, так что снять WEBHOOK_BASE_URL и
        # передеплоить достаточно, чтобы вернуться сюда без ручной уборки.
        log.info("Режим long-polling")
        app.run_polling()


if __name__ == "__main__":
    main()
