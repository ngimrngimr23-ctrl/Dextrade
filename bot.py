"""
Telegram-бот.

Поверхность команд устроена так: ДЕЙСТВИЯ — командами, ПОРОГИ — кнопками.

    /start     меню: сканы, списки, состояние, пороги, прокси
    /scanall   прогнать оба списка прямо сейчас
    /scan      разовая проверка одного предмета
    /watch     вотчлист по стикерам: показать, добавить, убрать, пауза
    /float     охота за флоатом: список + «/float чек» по конкретному скину
    /setarb    арбитраж CSFloat: порог, интервал, сброс кулдауна
    /arbnow    проверить арбитраж немедленно
    /markets   сравнить Steam со сторонними площадками
    /inv       инвентарь: оценить и следить за ростом цен
    /proxyadd  добавить прокси без передеплоя
    /help      справочник (собирается из реестра COMMANDS, не пишется руками)

Разделение не косметическое. Действие ты знаешь заранее, и напечатать его
быстрее, чем открыть меню; порог трогают раз в месяц, у него числовой
параметр, и синтаксис к следующему разу забывается. Живой пример: /setdefaults
5 7 задавала «минимум наклеек $5» и «доплата не выше 7% их стоимости», причём
второе число регулярно читали как «лот дороже голого скина на 7%» — разные
вещи, разница в деньгах кратная. Кнопка с подписью и примером в долларах эту
ошибку делает невозможной.

Старые имена (watchadd, floatlist, setdefaults, arbreset и ещё два десятка)
работают как прежде — они перечислены в COMMANDS и просто убраны из меню
Telegram.

Резервный ручной путь, если Steam не отвечает (например, IP на кулдауне
после 429): /scanfile <ссылка> — бот пришлёт ссылку на JSON, его надо
сохранить в браузере (Ctrl+S) и прислать файлом. Файл можно слать и без
команды.

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
import statistics
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import NamedTuple
from urllib.parse import quote

import aiohttp
from telegram import BotCommand, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

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
    take_throttle_wait,
    reset_cooldown as steam_reset_cooldown,
)
from csgo_api import search_items as search_csgo_items
from http_session import close_session as close_http_session
from inventory import InventoryError, fetch_inventory, resolve_steamid
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
    filter_new_offers,
    mark_offers_sent,
    get_all_chat_settings,
    SENT_OFFER_TTL_SECONDS,
    get_market_settings,
    set_market_setting,
    get_steam_prices_batch,
    set_steam_price,
    get_extra_proxies,
    save_extra_proxies,
    get_inventory_steamid,
    set_inventory_steamid,
    get_inventory_growth,
    set_inventory_growth,
    get_inventory_baseline,
    save_inventory_baseline,
    mark_offer_sent,
    get_arb_settings,
    set_arb_setting,
    all_chat_ids_with_settings,
)
import csfloat_client
import market_prices
import menu
import pricing
import proxy_pool
import sih_client
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

# Сколько предметов вотчлиста обрабатывать одновременно.
#
# Это НЕ разрешение слать в Steam чаще: темп запросов держит троттлинг в
# steam_client, и он один на весь процесс. Смысл в другом — убрать простой.
# Раньше цикл был строго последовательным: пауза 4 с, запрос, ответ, разбор,
# шесть походов в Upstash, и только потом следующий предмет. Обязательными
# были только 4 секунды, остальные 2-3 добавлялись сверху. Теперь пока по
# одному предмету идёт разбор, запрос по следующему уже отправлен.
#
# Четырёх полос хватает с запасом: на паузе в 2 секунды и ~3 секундах работы
# на предмет очередь заполняется двумя-тремя, четвёртая — про запас на случай
# медленного ответа Steam.
SCAN_CONCURRENCY = int(os.environ.get("SCAN_CONCURRENCY", "4"))

# /proxycheck: сколько адресов проверять одновременно и до скольких печатать
# построчный список. Ограничения нужны только для больших пулов — само
# количество прокси ничем не ограничено.
PROXYCHECK_CONCURRENCY = int(os.environ.get("PROXYCHECK_CONCURRENCY", "10"))
PROXYCHECK_DETAIL_LIMIT = int(os.environ.get("PROXYCHECK_DETAIL_LIMIT", "20"))

# /floatcheck: с какой разницы медиан считать, что за флоат реально доплачивают.
# Ниже этого — шум выборки: на проде AWP | Black Nile (FN) с флоатом 0.00585
# стоил на 0.8% дороже обычного, и называть это наценкой было бы враньём.
FLOATCHECK_MEANINGFUL_PREMIUM_PCT = float(os.environ.get("FLOATCHECK_MEANINGFUL_PREMIUM_PCT", "10"))

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


class ScanSettings(NamedTuple):
    """
    Все настройки чата, которые нужны при отборе офферов, одним объектом.

    Зачем: раньше _compute_offers дёргала пять отдельных геттеров
    (get_streak_markup, get_sticker_ratio, get_price_filter, get_float_filter,
    get_float_markup), и каждый уходил в Upstash отдельным HTTP-запросом за
    ОДНИМ И ТЕМ ЖЕ JSON-блоком настроек. На прогоне вотчлиста в 110 предметов
    это 550 запросов на ровном месте, причём выстроенных в цепочку: пока идёт
    один, скан стоит.

    Настройки за время прогона не меняются, поэтому читаются один раз в начале
    (_load_scan_settings) и передаются вниз. Кто настройки не передал — прочтёт
    их сам, но всё равно одним запросом вместо пяти.
    """

    min_value: float
    max_markup: float
    streak_markup: float | None
    sticker_ratio: float | None
    min_price: float | None
    max_price: float | None
    float_low: float | None
    float_high: float | None
    float_markup: float | None

    @classmethod
    def from_raw(cls, raw: dict) -> "ScanSettings":
        return cls(
            min_value=raw.get("min_value", DEFAULT_MIN_VALUE),
            max_markup=raw.get("max_markup", DEFAULT_MAX_MARKUP),
            streak_markup=raw.get("streak_markup"),
            sticker_ratio=raw.get("sticker_ratio"),
            min_price=raw.get("min_price"),
            max_price=raw.get("max_price"),
            float_low=raw.get("float_low_max"),
            float_high=raw.get("float_high_min"),
            float_markup=raw.get("float_markup_pct"),
        )


async def _load_scan_settings(chat_id: int) -> ScanSettings:
    """Один поход в хранилище за всем блоком настроек чата."""
    return ScanSettings.from_raw(await get_all_chat_settings(chat_id))


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
    """
    Свободный текст: либо значение порога, которого ждёт меню, либо номер
    варианта после неоднозначного поиска по названию.

    Порядок важен. Ожидание порога проверяется первым: если чат ждёт число
    для настройки, трактовать это сообщение как номер предмета нельзя — оба
    ожидания выглядят одинаково («пришли число»), и перепутать их значит
    молча применить ввод не туда.
    """
    if await _handle_pending_setting(update, context):
        return

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

    # Разбор ПО ИМЕНИ режима, без ветки "всё остальное".
    #
    # Раньше здесь стояло `if mode == "scan" ... else scanfile`, и это была
    # мина: любой новый режим молча уезжал в ручной сбор файлов. На ней и
    # подорвался /floatcheck — пользователь выбирал номер варианта и получал
    # инструкцию «сохрани страницу как .json и пришли файл», не имевшую к
    # его команде никакого отношения.
    mode = pending["mode"]
    if mode == "scan":
        await _proceed_scan(update, market_hash_name, min_value, max_markup)
    elif mode == "scanfile":
        await _proceed_scanfile(update, market_hash_name, min_value, max_markup)
    elif mode == "floatcheck":
        await _proceed_floatcheck(update, market_hash_name, pending.get("float"))
    else:
        log.error("handle_text_selection: неизвестный режим %r — обработчик не найден", mode)
        await update.message.reply_text(
            f"Внутренняя ошибка: не знаю, что делать с режимом {mode!r}. "
            "Повтори команду заново."
        )


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
    settings: ScanSettings | None = None,
):
    """
    Общая логика для /scan, /scanfile и автоскана вотчлиста: цены стикеров -> офферы.

    check_stickers / check_floats — что именно искать в этом предмете. Автоскан
    держит два независимых списка (обычный вотчлист /watchadd и список под охоту
    за флоатом /floatadd), поэтому для конкретного предмета может быть нужно
    только одно из двух. Ручные /scan и /scanfile считают всё — там пользователь
    сам назвал предмет, значит интересно и то, и другое.

    settings — уже прочитанные настройки чата. Прогон вотчлиста читает их один
    раз на весь список и передаёт сюда; кто не передал, прочтёт сам (см.
    ScanSettings — там же, зачем это вообще понадобилось).
    """
    if settings is None:
        settings = await _load_scan_settings(chat_id)
    streak_markup = settings.streak_markup
    sticker_ratio = settings.sticker_ratio
    min_price, max_price = settings.min_price, settings.max_price
    float_low, float_high = settings.float_low, settings.float_high
    float_markup = settings.float_markup

    all_sticker_keys = {s for l in listings for s in l.stickers}
    sticker_prices = await get_sticker_prices(all_sticker_keys) if all_sticker_keys else {}

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
            "Формат: /watch <предмет1>, <предмет2>, ...\n"
            "Можно ссылку или название (на английском), через запятую для нескольких сразу.\n"
            "Пример: /watch AK-47 | Slate (Field-Tested), M4A4 | Asiimov (Field-Tested)\n\n"
            "Степень износа можно не расписывать на каждый предмет, а указать один раз "
            "последним элементом — подойдёт и сокращение (FN/MW/FT/WW/BS):\n"
            "/watch AK-47 | Redline, AWP | Redline, Field-Tested"
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
            "Формат: /watch убрать <номер из /watch или точное название>\n"
            "Пример: /watch убрать 2, или короче /watch -2"
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
            await update.message.reply_text(f"«{arg}» не найден в списке. Точное название смотри в /watch.")
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
            "Вотчлист пуст. Добавь предметы: /watch <предмет1>, <предмет2>, ...\n"
            f"Пауза между прогонами: {interval:g} мин после конца предыдущего."
        )
        return

    lines = [f"📋 Вотчлист ({len(items)}), следующий прогон — через {interval:g} мин после конца текущего/предыдущего:"]
    for i, name in enumerate(items, start=1):
        lines.append(f"{i}. {name}")
    for chunk in _chunk_lines(lines):
        await update.message.reply_text(chunk)


# ---------------------------------------------------------------------------
# Подкоманды: /watch убрать 3 вместо /watchdel 3
#
# Команд накопилось 39, и половина из них была одним и тем же действием над
# разными списками: watchadd/floatadd, watchdel/floatdel, watchlist/floatlist.
# Имена приходилось держать в голове словарём, потому что по самому имени
# нельзя было понять, к какому списку оно относится.
#
# Теперь у каждого списка одна команда, а действие — первым словом. Старые
# имена остались рабочими алиасами (см. _build_application): они ничего не
# стоят, а мышечная память у них уже есть.
# ---------------------------------------------------------------------------

class _SubCtx:
    """
    Тот же context, но с подменёнными args.

    Нужен, чтобы подкоманда делегировала работу в уже написанный и
    оттестированный обработчик, а не дублировала его логику: /watch убрать 3
    попадает в watchdel ровно тем же путём, что и /watchdel 3. Всё, кроме
    args, отдаём настоящему context — job_queue, bot, application.
    """

    def __init__(self, base, args: list[str]):
        self._base = base
        self.args = args

    def __getattr__(self, name):
        return getattr(self._base, name)


def _subcommand(args: list[str], table: dict[str, tuple]) -> tuple | None:
    """
    Разобрать первое слово как действие. Возвращает (обработчик, остаток
    аргументов) либо None, если слово действием не является.
    """
    if not args:
        return None
    head = args[0].lower().strip()
    if head in table:
        return table[head], args[1:]
    return None


_WATCH_ACTIONS = {
    "список": "list", "list": "list", "покажи": "list",
    "убрать": "del", "удалить": "del", "del": "del", "-": "del",
    "очистить": "clear", "очисти": "clear", "clear": "clear",
    "стоп": "pause", "пауза": "pause", "стой": "pause", "pause": "pause", "stop": "pause",
    "старт": "resume", "пуск": "resume", "resume": "resume", "start": "resume", "вкл": "resume",
}


async def watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /watch                     — показать вотчлист и состояние автоскана
    /watch <предмет1>, <...>   — добавить предметы
    /watch -3                  — убрать третий (можно «/watch убрать 3»)
    /watch очистить            — очистить список
    /watch стоп | старт        — пауза автоскана и обратно

    Всё, что не опознано как действие, считается названием предмета: имена
    скинов бывают какими угодно, а список действий короткий и закрытый, так
    что неоднозначность возможна только если скин назвали словом «очистить».
    """
    args = list(context.args)

    if not args:
        return await watchlist_cmd(update, context)

    head = args[0].lower().strip()

    # «-3» и «- 3»: минус вплотную к номеру — самая короткая запись удаления,
    # и она не может быть началом названия скина.
    if head.startswith("-") and head[1:].strip().isdigit():
        return await watchdel(update, _SubCtx(context, [head[1:].strip()]))

    action = _WATCH_ACTIONS.get(head)
    if action == "list":
        return await watchlist_cmd(update, _SubCtx(context, args[1:]))
    if action == "del":
        return await watchdel(update, _SubCtx(context, args[1:]))
    if action == "clear":
        return await watchclear(update, _SubCtx(context, args[1:]))
    if action == "pause":
        return await watchpause(update, _SubCtx(context, args[1:]))
    if action == "resume":
        return await watchresume(update, _SubCtx(context, args[1:]))

    return await watchadd(update, context)


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
            "Задай: /start → Пороги → Флоат"
            if low is None or high is None else ""
        )
        await update.message.reply_text(
            "Формат: /float <предмет1>, <предмет2>, ...\n"
            "Можно ссылку или название (на английском), через запятую для нескольких сразу.\n"
            "Пример: /float AK-47 | Redline (Field-Tested)\n\n"
            "Степень износа можно указать один раз последним элементом (FN/MW/FT/WW/BS):\n"
            "/float AK-47 | Redline, AWP | Asiimov, Factory New\n\n"
            "Это ОТДЕЛЬНЫЙ список от /watch — флоат ищется только по нему." + hint
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
        lines.append("⚠️ Порог флоата не задан — охота не пойдёт. Задай: /start → Пороги → Флоат")
    lines.append(f"Всего в списке флоата: {len(current)}.")
    await update.message.reply_text("\n\n".join(lines))


async def floatdel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/floatdel <номер из /floatlist или точное название> — убрать предмет из списка флоата."""
    chat_id = update.effective_chat.id
    current = await get_float_watchlist(chat_id)

    if not context.args:
        await update.message.reply_text(
            "Формат: /float убрать <номер из /float или точное название>\nПример: /float убрать 2, или короче /float -2"
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
            await update.message.reply_text(f"«{arg}» не найден. Точное название смотри в /float.")
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
            "Добавить: /float <предмет>"
        )
        return

    if low is None or high is None:
        threshold = "⚠️ порог не задан (/start → Пороги → Флоат) — охота не идёт"
    else:
        threshold = f"флоат ≤{low:g} или ≥{high:g}"
        if markup is not None:
            threshold += f", наценка ≤{markup:g}%"

    lines = [f"🔍 Охота за флоатом ({len(items)}), условие: {threshold}"]
    for i, name in enumerate(items, start=1):
        lines.append(f"{i}. {name}")
    for chunk in _chunk_lines(lines):
        await update.message.reply_text(chunk)


_FLOAT_ACTIONS = {
    "список": "list", "list": "list", "покажи": "list",
    "убрать": "del", "удалить": "del", "del": "del", "-": "del",
    "очистить": "clear", "очисти": "clear", "clear": "clear",
    "чек": "check", "check": "check", "проверь": "check", "проверить": "check",
}


async def float_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /float                        — список охоты за флоатом и условие отбора
    /float <предмет1>, <...>      — добавить предметы
    /float -2                     — убрать второй (можно «/float убрать 2»)
    /float очистить               — очистить список
    /float чек <предмет> [флоат]  — платят ли за низкий флоат на этом скине

    «чек» — единственное тут, что не про список: это разовый разбор одного
    предмета (бывшая /floatcheck). Раньше он делил префикс со списком,
    ничего с ним не разделяя, и это была прямая ловушка — /floatlist и
    /floatcheck выглядели родственниками, будучи разными вещами. Внутри одной
    команды родство хотя бы честное: и то, и другое про флоат.
    """
    args = list(context.args)

    if not args:
        return await floatlist_cmd(update, context)

    head = args[0].lower().strip()

    if head.startswith("-") and head[1:].strip().isdigit():
        return await floatdel(update, _SubCtx(context, [head[1:].strip()]))

    action = _FLOAT_ACTIONS.get(head)
    if action == "list":
        return await floatlist_cmd(update, _SubCtx(context, args[1:]))
    if action == "del":
        return await floatdel(update, _SubCtx(context, args[1:]))
    if action == "clear":
        return await floatclear(update, _SubCtx(context, args[1:]))
    if action == "check":
        return await floatcheck(update, _SubCtx(context, args[1:]))

    return await floatadd(update, context)


def _offer_key(market_hash_name: str, offer: Offer) -> str:
    """
    Стабильный ключ конкретного лота — по inspect-ссылке (уникальна для
    каждого экземпляра предмета в Steam), либо, если её нет, по сочетанию
    название+цена+стикеры. Нужен, чтобы не слать один и тот же оффер
    повторно в течение SENT_OFFER_TTL_SECONDS (см. storage.py).
    """
    basis = offer.inspect_link or f"{market_hash_name}|{offer.price}|{','.join(offer.stickers)}"
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()


class _ScanStats:
    """
    Разбивка времени прогона по фазам — чтобы оптимизировать по замеру, а не
    по ощущениям. Суммы считаются по всем предметам, поэтому при параллельной
    обработке они БОЛЬШЕ реального времени прогона: это нормально и как раз
    показывает, сколько удалось наложить друг на друга.
    """

    __slots__ = ("steam", "compute", "send", "items", "failed")

    def __init__(self) -> None:
        self.steam = 0.0    # запрос листингов: пауза троттлинга + сеть
        self.compute = 0.0  # цены стикеров, отбор офферов, дедуп (Upstash + CPU)
        self.send = 0.0     # отправка сообщений в Telegram
        self.items = 0
        self.failed = 0


async def _watchlist_scan_item(
    bot, chat_id: int, market_hash_name: str, min_value: float, max_markup: float,
    *, check_stickers: bool = True, check_floats: bool = True,
    request_interval: float | None = None,
    settings: ScanSettings | None = None,
    stats: "_ScanStats | None" = None,
) -> bool:
    """
    Возвращает True, если нашлись НОВЫЕ офферы (не присылавшиеся этому чату
    за последние 5 часов) и сообщение реально ушло в чат.
    check_stickers/check_floats — что искать: предмет мог попасть в прогон из
    обычного вотчлиста, из списка охоты за флоатом, или сразу из обоих.
    SteamRateLimited пробрасывается наверх — прогон должен остановиться целиком,
    а не продолжать долбить Steam остальными предметами во время бана.

    request_interval — пауза между запросами к Steam (у ручного скана она
    короче фоновой, см. MANUAL_REQUEST_INTERVAL).
    """
    t0 = time.perf_counter()
    try:
        listings = await fetch_all_listings(market_hash_name, request_interval=request_interval)
    except SteamRateLimited:
        raise
    except Exception as e:
        log.warning("watchlist: %s (chat_id=%s): %s", market_hash_name, chat_id, e)
        if stats is not None:
            stats.steam += time.perf_counter() - t0
            stats.failed += 1
        return False

    t1 = time.perf_counter()
    offers, sticker_prices = await _compute_offers(
        chat_id, listings, min_value, max_markup,
        check_stickers=check_stickers, check_floats=check_floats,
        settings=settings,
    )

    new_offers: list = []
    if offers:
        keys = [_offer_key(market_hash_name, o) for o in offers]
        # Одним запросом на весь лот, а не по запросу на каждый оффер.
        is_new = await filter_new_offers(chat_id, keys)
        new_offers = [o for o, fresh in zip(offers, is_new) if fresh]

    t2 = time.perf_counter()
    sent = False
    if new_offers:
        chunks = _format_offers_chunks(new_offers, sticker_prices, market_hash_name)
        chunks[0] = f"🔔 {html_module.escape(market_hash_name)}\n\n{chunks[0]}"
        for chunk in chunks:
            await bot.send_message(chat_id=chat_id, text=chunk, parse_mode="HTML", disable_web_page_preview=True)
        await mark_offers_sent(chat_id, [_offer_key(market_hash_name, o) for o in new_offers])
        sent = True

    if stats is not None:
        stats.steam += t1 - t0
        stats.compute += t2 - t1
        stats.send += time.perf_counter() - t2
        stats.items += 1

    # Пусто — молчим: автоскан идёт постоянно, и сообщение «ничего не нашлось»
    # на каждый предмет превратило бы его в спам.
    return sent


class WatchlistScanReport(NamedTuple):
    """
    Итог прогона — и /scanall, и автоскан шлют по нему один и тот же отчёт
    "Готово", чтобы поведение не расходилось между ручным и фоновым запуском
    (раньше автоскан на пустом результате молчал вообще, и по логам нельзя
    было отличить "ничего не нашёл" от "не запустился").
    """

    found_any: bool
    items: int


async def _run_watchlist_scan(
    bot, chat_id: int, request_interval: float | None = None
) -> WatchlistScanReport | None:
    """
    Прогоняет весь вотчлист чата разом — общая логика для джобы по расписанию
    и команды /scanall. Возвращает WatchlistScanReport, либо None, если прогон
    не запустился вообще (пустой список / уже идёт другой прогон) — в этом
    случае отчёт "Готово" слать нечего, само событие уже понятно из другого
    сообщения (или его специально не шлют, как при паузе автоскана).
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
        # Настройки чата — ОДИН раз на весь прогон, а не по пять запросов в
        # Upstash на каждый предмет (см. ScanSettings).
        settings = await _load_scan_settings(chat_id)
        min_value, max_markup = settings.min_value, settings.max_markup

        stats = _ScanStats()
        started = time.perf_counter()
        take_throttle_wait()  # обнуляем счётчик пауз перед прогоном

        queue: asyncio.Queue = asyncio.Queue()
        for entry in scan_plan:
            queue.put_nowait(entry)

        found_any = False
        # Первый пойманный рейт-лимит: (название предмета, исключение).
        rate_limit_hit: list[tuple[str, SteamRateLimited]] = []

        async def worker() -> None:
            nonlocal found_any
            while not rate_limit_hit:
                try:
                    market_hash_name, check_stickers, check_floats = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    found = await _watchlist_scan_item(
                        bot, chat_id, market_hash_name, min_value, max_markup,
                        check_stickers=check_stickers, check_floats=check_floats,
                        request_interval=request_interval,
                        settings=settings, stats=stats,
                    )
                except SteamRateLimited as e:
                    # Влетели в рейт-лимит Steam. Останавливаем весь прогон:
                    # каждая следующая попытка во время бана продлевает его.
                    # Соседние воркеры увидят непустой rate_limit_hit и просто
                    # не возьмут следующий предмет — уже отправленные запросы
                    # отменой всё равно не вернуть, а новых не будет.
                    if not rate_limit_hit:
                        rate_limit_hit.append((market_hash_name, e))
                    return
                found_any = found_any or found

        # Предметы идут параллельно, но темп запросов к Steam держит общий
        # троттлинг (throttle_steam_request) — он отмеряет паузу от ОТПРАВКИ
        # предыдущего запроса. То есть Steam видит ровно тот же один запрос в
        # request_interval секунд, что и при последовательном обходе.
        # Параллельность нужна не чтобы просить чаще, а чтобы не простаивать:
        # пока по одному предмету идёт разбор ответа и походы в Upstash,
        # запрос по следующему уже в пути.
        workers = min(SCAN_CONCURRENCY, len(scan_plan))
        await asyncio.gather(*(worker() for _ in range(workers)))

        elapsed = time.perf_counter() - started
        throttle_wait = take_throttle_wait()
        log.info(
            "watchlist: chat_id=%s прогон за %.1f с — предметов %d (ошибок %d), полос %d, пауза %.1f с. "
            "Суммарно по фазам (идут внахлёст): Steam %.1f с (из них троттлинг %.1f с), "
            "отбор+Upstash %.1f с, Telegram %.1f с",
            chat_id, elapsed, stats.items, stats.failed, workers,
            request_interval if request_interval is not None else MIN_REQUEST_INTERVAL,
            stats.steam, throttle_wait, stats.compute, stats.send,
        )

        if rate_limit_hit:
            market_hash_name, e = rate_limit_hit[0]
            log.warning("watchlist: прогон chat_id=%s остановлен из-за рейт-лимита: %s", chat_id, e)
            await bot.send_message(
                chat_id=chat_id,
                text=f"⏸ Автоскан остановлен на «{market_hash_name}»: {e}",
            )
        return WatchlistScanReport(found_any=found_any, items=stats.items)
    finally:
        _watchlist_running.discard(chat_id)


def _format_scan_done(report: "WatchlistScanReport") -> str:
    """
    Единый текст итога и для /scanall, и для автоскана — специально ОДИН И ТОТ
    ЖЕ формат, чтобы пользователь видел одинаковое сообщение независимо от
    того, кто запустил прогон.
    """
    tail = "есть новые находки — см. выше." if report.found_any else "ничего подходящего не нашлось."
    return f"Готово: проверено {report.items} предмет(ов), {tail}"


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
        report = await _run_watchlist_scan(context.bot, chat_id)
        if report is not None:
            await context.bot.send_message(chat_id=chat_id, text=_format_scan_done(report))
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

def _arb_interval(settings: dict) -> float:
    """
    Интервал автоскана арбитража для чата: заданный командой /setarb, иначе
    общий из окружения. Отдельной функцией, потому что спрашивают его четыре
    разных места, и «забыл посмотреть в настройки чата» здесь означает тихий
    возврат к старому расписанию.
    """
    value = settings.get("interval")
    return float(value) if value else ARB_INTERVAL_MINUTES


# Часовая квота CSFloat на КЛЮЧ (x-ratelimit-limit). Не умножается числом
# прокси — проверено в проде: при семи адресах счётчик шёл одной цепочкой.
CSFLOAT_HOURLY_BUDGET = 200


def _arb_budget_problem(interval_minutes: float) -> str | None:
    """
    Влезает ли выбранный интервал в квоту CSFloat — и если нет, чем это
    кончится и что с этим делать.

    Считать надо явно и говорить вслух, потому что перебор проявляется не
    ошибкой, а тишиной: первые прогоны часа отрабатывают, дальше квота
    выбрана, всё падает на 429 и подборка приходит пустой. Со стороны это
    неотличимо от «ничего выгодного не нашлось».
    """
    if interval_minutes <= 0:
        return None
    per_scan = -(-ARB_TARGET_LISTINGS // csfloat_client.MAX_LIMIT)  # округление вверх
    scans_per_hour = 60 / interval_minutes
    needed = per_scan * scans_per_hour
    if needed <= CSFLOAT_HOURLY_BUDGET:
        return None

    safe_interval = 60 * per_scan / CSFLOAT_HOURLY_BUDGET
    safe_target = int(CSFLOAT_HOURLY_BUDGET / scans_per_hour) * csfloat_client.MAX_LIMIT
    return (
        f"⚠️ {ARB_TARGET_LISTINGS} лотов каждые {interval_minutes:g} мин = "
        f"{needed:.0f} запросов в час при доступных {CSFLOAT_HOURLY_BUDGET}.\n"
        f"Квота кончится к середине часа, дальше прогоны будут падать на 429 "
        f"и подборка придёт пустой.\n"
        f"Влезет: интервал от {safe_interval:.0f} мин, либо ARB_TARGET_LISTINGS="
        f"{safe_target} при нынешнем интервале."
    )


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

    # Полос столько, сколько выдержит priceoverview, а НЕ сколько адресов в
    # пуле: этот эндпоинт режется жёстче всех, и 46 прокси означали 46
    # одновременных полос и 53 запроса в минуту (диагностика 2026-08-27).
    lanes = pricing.PRICE_CONCURRENCY
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
            # Объём перезаписываем ТОЛЬКО когда он известен. Цена из листингов
            # (запасной путь при забаненном priceoverview) объёма не содержит, и
            # безусловная запись затирала бы признак ликвидности, добытый ранее
            # из окон прайс-листа и reference.quantity — см. _fill_steam_prices.
            if live.volume is not None:
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

    # Своей сессии тут не нужно: прайс-лист почти всегда отдаётся из памяти,
    # а если всё-таки скачивается — берётся общая сессия процесса.
    prices = await get_csgotrader_price_details()
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
            _schedule_arb_job(context.job_queue, chat_id, _arb_interval(settings))


# ---------------------------------------------------------------------------
# Слежение за инвентарём: сообщать, когда свои скины подорожали
#
# Дешевле всего остального в этом боте, и это стоит понимать. Инвентарь целиком
# приходит ОДНИМ запросом, а цены берутся из прайс-листа csgotrader, уже
# разобранного в памяти процесса, — то есть проверка инвентаря на пятьсот
# предметов стоит один сетевой запрос, а не пятьсот. Поэтому здесь нет ни пула
# прокси, ни бюджетов, ни хитрых пауз: их просто нечем тратить.
# ---------------------------------------------------------------------------

INVENTORY_JOB_PREFIX = "inventory_scan_"
# Раз в час. Чаще смысла нет: прайс-лист на стороне csgotrader обновляется
# примерно раз в час, и более частая проверка сравнивала бы одни и те же числа.
INVENTORY_INTERVAL_MINUTES = float(os.environ.get("INVENTORY_INTERVAL_MINUTES", "60"))
# Предметы дешевле этого в отчёт не идут: рост на 30% от десяти центов — это
# три цента, и такие строки только прячут настоящие движения.
INVENTORY_MIN_PRICE = float(os.environ.get("INVENTORY_MIN_PRICE", "0.50"))
# Какое окно прайс-листа считать текущей ценой. Сутки — самое свежее, что есть.
INVENTORY_PRICE_WINDOW = "last_24h"


def _schedule_inventory_job(job_queue, chat_id: int, delay_minutes: float | None = None) -> None:
    """Следующая проверка инвентаря — одноразовой джобой, как у вотчлиста и арбитража."""
    for job in job_queue.get_jobs_by_name(f"{INVENTORY_JOB_PREFIX}{chat_id}"):
        job.schedule_removal()
    job_queue.run_once(
        inventory_scan_job,
        when=(delay_minutes if delay_minutes is not None else INVENTORY_INTERVAL_MINUTES) * 60,
        data={"chat_id": chat_id},
        name=f"{INVENTORY_JOB_PREFIX}{chat_id}",
    )


def _inventory_price(prices: dict, market_hash_name: str) -> float | None:
    """
    Текущая цена предмета из прайс-листа.

    Берём сначала суточное окно, и только если его нет — недельное. Предметы,
    которые не продавались сутки, иначе выпали бы из слежения совсем, хотя для
    инвентаря они как раз обычное дело.
    """
    entry = prices.get(market_hash_name)
    if entry is None:
        return None
    for window in (INVENTORY_PRICE_WINDOW, "last_7d", "last_30d"):
        value = entry.windows.get(window)
        if value:
            return value
    return None


async def _run_inventory_scan(bot, chat_id: int, *, announce_baseline: bool = False):
    """
    Один прогон: прочитать инвентарь, сравнить с сохранённым снимком цен и
    сообщить о выросших.

    Возвращает (сколько предметов проверено, сколько выросло) либо None, если
    прогон не состоялся (нет привязанного аккаунта).

    Точка отсчёта сдвигается ТОЛЬКО у тех предметов, о росте которых мы
    сообщили. Иначе одно и то же подорожание всплывало бы в каждом прогоне —
    и наоборот, если сдвигать всё подряд, медленный рост по чуть-чуть за раз
    никогда не набрал бы порога.
    """
    steamid = await get_inventory_steamid(chat_id)
    if not steamid:
        return None

    threshold = await get_inventory_growth(chat_id)
    items = await fetch_inventory(steamid)
    if not items:
        return 0, 0

    prices = await get_csgotrader_price_details()
    if not prices:
        raise InventoryError("Прайс-лист не скачался — оценить инвентарь нечем.")

    baseline = await get_inventory_baseline(chat_id)
    new_baseline = dict(baseline)
    grown: list[tuple[str, int, float, float, float]] = []  # имя, шт, было, стало, %
    priced = 0
    first_seen = 0

    for item in items:
        price = _inventory_price(prices, item.market_hash_name)
        if price is None or price < INVENTORY_MIN_PRICE:
            continue
        priced += 1

        was = baseline.get(item.market_hash_name)
        if was is None:
            # Первая встреча — просто запоминаем, сравнивать пока не с чем.
            new_baseline[item.market_hash_name] = price
            first_seen += 1
            continue

        if threshold is None or was <= 0:
            continue
        growth_pct = (price - was) / was * 100
        if growth_pct >= threshold:
            grown.append((item.market_hash_name, item.count, was, price, growth_pct))
            new_baseline[item.market_hash_name] = price  # отсчёт от новой цены

    # Предметы, которых в инвентаре больше нет, из снимка убираем: держать их
    # вечно значит копить мусор и однажды отчитаться о росте того, что продано.
    present = {i.market_hash_name for i in items}
    new_baseline = {k: v for k, v in new_baseline.items() if k in present}

    if new_baseline != baseline:
        await save_inventory_baseline(chat_id, new_baseline)

    if grown:
        grown.sort(key=lambda row: row[4], reverse=True)
        total_gain = sum((now - was) * count for _, count, was, now, _ in grown)
        lines = [
            f"📈 Подорожало в инвентаре — {len(grown)} поз.\n"
            f"<i>Цены из прайс-листа csgotrader (обновляется примерно раз в час), "
            f"это средняя цена по предмету, а не текущий нижний лот. "
            f"Суммарно прибавка ≈ ${total_gain:.2f}.</i>"
        ]
        for name, count, was, now, pct in grown[:20]:
            amount = f" ×{count}" if count > 1 else ""
            lines.append(
                f"<code>{html_module.escape(name)}</code>{amount}\n"
                f"  ${was:.2f} → ${now:.2f} (+{pct:.1f}%)\n"
                f'  <a href="{_steam_market_url(name)}">Открыть в Steam</a>'
            )
        for chunk in _chunk_lines(lines, sep="\n\n"):
            await bot.send_message(chat_id=chat_id, text=chunk, parse_mode="HTML", disable_web_page_preview=True)
    elif announce_baseline and first_seen:
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f"Запомнил цены по {first_seen} предмет(ам) — это точка отсчёта. "
                f"Дальше буду сообщать, когда что-то из них подорожает."
            ),
        )

    log.info(
        "inventory: chat_id=%s проверено %d, впервые записано %d, выросло %d (порог %s)",
        chat_id, priced, first_seen, len(grown),
        f"{threshold:g}%" if threshold is not None else "не задан",
    )
    return priced, len(grown)


def _steam_market_url(market_hash_name: str) -> str:
    return f"https://steamcommunity.com/market/listings/730/{quote(market_hash_name, safe='')}"


async def inventory_scan_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data["chat_id"]
    try:
        await _run_inventory_scan(context.bot, chat_id)
    except SteamRateLimited as e:
        log.warning("inventory: chat_id=%s рейт-лимит: %s", chat_id, e)
    except InventoryError as e:
        log.warning("inventory: chat_id=%s не прочитать инвентарь: %s", chat_id, e)
        # Молча гасить нельзя: закрытый инвентарь означает, что слежение не
        # работает вообще, и пользователь должен об этом узнать. Но и в каждый
        # прогон повторять не будем — раз в SENT_OFFER_TTL_SECONDS.
        notice_key = "inventory_error_notice"
        if not await was_offer_sent_recently(chat_id, notice_key):
            await context.bot.send_message(chat_id=chat_id, text=f"⚠️ Слежение за инвентарём: {e}")
            await mark_offer_sent(chat_id, notice_key)
    except Exception:
        log.exception("inventory: непредвиденная ошибка в прогоне chat_id=%s", chat_id)
    finally:
        if await get_inventory_growth(chat_id) is not None:
            _schedule_inventory_job(context.job_queue, chat_id)


_INV_WATCH_WORDS = ("следить", "рост", "watch", "invwatch")


async def inv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /inv                  — показать привязанный аккаунт и оценку инвентаря
    /inv <ссылка|steamid> — привязать аккаунт
    /inv следить <%>      — сообщать, когда скин подорожал на N% (бывш. /invwatch)
    /inv следить off      — выключить слежение
    /inv off              — отвязать аккаунт и забыть снимок цен

    «следить» проверяется ДО разбора аргумента как ссылки на профиль: иначе
    слово уехало бы в resolve_steamid и вернулось ошибкой «не нашёл такой
    профиль», что к делу отношения не имеет.
    """
    chat_id = update.effective_chat.id

    if context.args and context.args[0].lower() in _INV_WATCH_WORDS:
        return await invwatch(update, _SubCtx(context, list(context.args[1:])))

    if context.args and context.args[0].lower() in ("off", "выкл"):
        await set_inventory_steamid(chat_id, None)
        await set_inventory_growth(chat_id, None)
        await save_inventory_baseline(chat_id, {})
        for job in context.application.job_queue.get_jobs_by_name(f"{INVENTORY_JOB_PREFIX}{chat_id}"):
            job.schedule_removal()
        await update.message.reply_text("Аккаунт отвязан, точка отсчёта забыта.")
        return

    if context.args:
        try:
            steamid = await resolve_steamid(" ".join(context.args))
        except (InventoryError, SteamRateLimited) as e:
            await update.message.reply_text(f"⚠️ {e}")
            return
        await set_inventory_steamid(chat_id, steamid)
        # Снимок от прошлого аккаунта к новому не относится.
        await save_inventory_baseline(chat_id, {})
        await update.message.reply_text(f"✅ Аккаунт привязан: {steamid}\nСчитаю инвентарь…")
    else:
        steamid = await get_inventory_steamid(chat_id)
        if not steamid:
            await update.message.reply_text(
                "Аккаунт не привязан.\n\n"
                "<code>/inv https://steamcommunity.com/id/твой_ник</code>\n"
                "или <code>/inv 7656119...</code>\n\n"
                "Инвентарь должен быть открыт: Steam → Профиль → Редактировать "
                "профиль → Настройки приватности → «Инвентарь» = Открытый.",
                parse_mode="HTML",
            )
            return

    try:
        items = await fetch_inventory(steamid)
    except (InventoryError, SteamRateLimited) as e:
        await update.message.reply_text(f"⚠️ {e}")
        return

    if not items:
        await update.message.reply_text("В инвентаре нет продаваемых предметов CS2.")
        return

    prices = await get_csgotrader_price_details()
    total = 0.0
    unpriced = 0
    valued: list[tuple[str, int, float]] = []
    for item in items:
        price = _inventory_price(prices, item.market_hash_name)
        if price is None:
            unpriced += 1
            continue
        total += price * item.count
        valued.append((item.market_hash_name, item.count, price))

    valued.sort(key=lambda row: row[2] * row[1], reverse=True)
    units = sum(i.count for i in items)
    lines = [
        f"🎒 Инвентарь: {units} предмет(ов), {len(items)} уникальных\n"
        f"Оценка: <b>${total:.2f}</b>"
        + (f"\n<i>Без цены в прайс-листе: {unpriced}</i>" if unpriced else "")
    ]
    for name, count, price in valued[:10]:
        amount = f" ×{count}" if count > 1 else ""
        lines.append(f"<code>{html_module.escape(name)}</code>{amount} — ${price * count:.2f}")
    if len(valued) > 10:
        lines.append(f"<i>…и ещё {len(valued) - 10} поз.</i>")

    growth = await get_inventory_growth(chat_id)
    lines.append(
        f"\nСлежение за ростом: {f'включено, порог {growth:g}%' if growth is not None else 'выключено'}\n"
        "Включить: /inv следить 15"
    )
    for chunk in _chunk_lines(lines, sep="\n"):
        await update.message.reply_text(chunk, parse_mode="HTML", disable_web_page_preview=True)


async def invwatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /invwatch <%>  — сообщать, когда предмет из инвентаря подорожал на N%
    /invwatch off  — выключить
    """
    chat_id = update.effective_chat.id
    steamid = await get_inventory_steamid(chat_id)

    if not context.args:
        growth = await get_inventory_growth(chat_id)
        baseline = await get_inventory_baseline(chat_id)
        if growth is None:
            await update.message.reply_text(
                "📈 Слежение за ростом инвентаря выключено.\n\n"
                "Включить: <code>/inv следить 15</code> — сообщу, когда предмет "
                "подорожает на 15% от цены, записанной при первом замере.\n\n"
                + ("" if steamid else "Сначала привяжи аккаунт: /inv <ссылка на профиль>"),
                parse_mode="HTML",
            )
        else:
            await update.message.reply_text(
                f"📈 Слежу за ростом: порог {growth:g}%\n"
                f"Точка отсчёта записана по {len(baseline)} предмет(ам).\n"
                f"Проверка раз в {INVENTORY_INTERVAL_MINUTES:g} мин.\n\n"
                "Поменять порог: /inv следить 25\nВыключить: /inv следить off"
            )
        return

    if context.args[0].lower() in ("off", "выкл"):
        await set_inventory_growth(chat_id, None)
        for job in context.application.job_queue.get_jobs_by_name(f"{INVENTORY_JOB_PREFIX}{chat_id}"):
            job.schedule_removal()
        await update.message.reply_text("📈 Слежение за ростом инвентаря выключено.")
        return

    if not steamid:
        await update.message.reply_text(
            "Сначала привяжи аккаунт: <code>/inv https://steamcommunity.com/id/твой_ник</code>",
            parse_mode="HTML",
        )
        return

    try:
        pct = float(context.args[0].replace("%", "").replace(",", "."))
    except ValueError:
        await update.message.reply_text("Нужно число процентов. Пример: /inv следить 15")
        return
    if pct <= 0:
        await update.message.reply_text("Процент должен быть больше нуля.")
        return

    await set_inventory_growth(chat_id, pct)
    await update.message.reply_text(
        f"📈 Слежу за инвентарём: сообщу, когда предмет подорожает на {pct:g}% "
        f"от записанной цены.\nПроверка раз в {INVENTORY_INTERVAL_MINUTES:g} мин, "
        f"первый замер — сейчас."
    )

    try:
        await _run_inventory_scan(context.bot, chat_id, announce_baseline=True)
    except (InventoryError, SteamRateLimited) as e:
        await update.message.reply_text(f"⚠️ {e}")
        return
    _schedule_inventory_job(context.application.job_queue, chat_id)


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
        f"({len(float_items)} шт.) сохранены — их можно смотреть /watch, /float и чистить "
        f"/watch -N, /float -N.\n"
        f"Возобновить оба сразу: /watch старт. Разовый скан вручную по-прежнему работает: /scanall."
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


_ARB_RESET_WORDS = ("сброс", "сбросить", "reset", "arbreset", "кулдаун")


async def setarb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setarb                  — показать настройки арбитража
    /setarb <мин%>           — включить: слать лоты CSFloat дешевле Steam на мин%
    /setarb <мин%> <минут>   — то же плюс пауза между автоматическими прогонами
    /setarb сброс            — снять кулдаун CSFloat (бывшая /arbreset)
    /setarb off              — выключить

    Интервал живёт ЗДЕСЬ, а не отдельной командой, потому что он не
    самостоятельная настройка: он осмысленен только вместе с процентом и
    упирается в ту же квоту CSFloat. Разнесённые по разным командам, процент и
    частота выглядели независимыми — а на деле частота решает, доживёт ли
    прогон до конца часа (см. _arb_budget_problem).
    """
    chat_id = update.effective_chat.id
    args = context.args
    settings = await get_arb_settings(chat_id)
    interval = _arb_interval(settings)

    if not args:
        if settings["min_discount"] is None:
            await update.message.reply_text(
                "💱 Арбитраж CSFloat ↔ Steam выключен.\n\n"
                "Включить: /setarb <мин%> [минут между прогонами]\n"
                "Пример: /setarb 20 — слать лоты, которые на CSFloat дешевле цены "
                "Steam минимум на 20%.\n"
                "Пример: /setarb 30 9 — то же с порогом 30% и проверкой каждые 9 мин.\n"
                f"Сейчас интервал {interval:g} мин, до {ARB_TARGET_LISTINGS} лотов за прогон.\n\n"
                "Дополнительно:\n"
                "/setarbprice <мин$> <макс$> — ограничить диапазон цены\n"
                "/setarbvolume <шт> — только ликвидное (продаж на Steam за сутки)\n"
                "/setarbstickers <макс%> — ещё и лоты, где наклейки почти даром\n"
                "/setarb сброс — снять кулдаун CSFloat\n"
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
            lines.append(f"Проверка каждые {interval:g} мин.")
            problem = _arb_budget_problem(interval)
            if problem:
                lines.append("")
                lines.append(problem)
            lines.append("")
            lines.append("Поменять: /setarb <мин%> [минут]. Выключить: /setarb off")
            await update.message.reply_text("\n".join(lines))
        return

    head = args[0].lower()

    if head in _ARB_RESET_WORDS:
        await _arb_reset(update)
        return

    if head == "off":
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
        pct = float(args[0].replace("%", "").replace(",", "."))
    except ValueError:
        await update.message.reply_text(
            f"{args[0]!r} — не число. Формат: /setarb <мин%> [минут между прогонами], "
            "например /setarb 30 9"
        )
        return
    if pct <= 0:
        await update.message.reply_text("Процент должен быть больше нуля.")
        return

    if len(args) >= 2:
        try:
            minutes = float(args[1].replace(",", "."))
        except ValueError:
            await update.message.reply_text(
                f"{args[1]!r} — не число минут. Формат: /setarb 30 9"
            )
            return
        if minutes <= 0:
            await update.message.reply_text("Интервал должен быть больше нуля.")
            return
        await set_arb_setting(chat_id, "interval", minutes)
        interval = minutes

    await set_arb_setting(chat_id, "min_discount", pct)
    _schedule_arb_job(context.application.job_queue, chat_id, delay_minutes=0.2)

    lines = [
        f"💱 Арбитраж включён: ищу лоты CSFloat дешевле цены Steam минимум на {pct:g}%.",
        f"Проверка каждые {interval:g} мин, первый прогон — прямо сейчас.",
    ]
    problem = _arb_budget_problem(interval)
    if problem:
        # Не запрещаем: интервал пользователь выбрал сам и вправе его оставить.
        # Но молчать нельзя — последствия видны только через полчаса тишины.
        lines.append("")
        lines.append(problem)
    lines.append("")
    lines.append(
        f"⚠️ Учти: выручка от продажи в Steam попадает на кошелёк Steam и не выводится. "
        f"В сообщениях показываю «чистыми» с учётом комиссии "
        f"~{(1 - STEAM_FEE_MULTIPLIER) * 100:.0f}%, чтобы процент не обманывал."
    )
    await update.message.reply_text("\n".join(lines))


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


async def floatcheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /floatcheck <предмет> [флоат] — платит ли рынок за низкий флоат ИМЕННО тут.

    Зачем команда. Общего ответа на «сколько стоит хороший флоат» не
    существует: на одном скине за 0.005 доплачивают кратно, на другом — ноль.
    Разбираться надо по конкретному предмету, и вот наглядный случай с прода
    2026-08-27: AWP | Black Nile (FN) с флоатом 0.00585 стоил на Steam $36.60
    при цене обычного экземпляра $36.30 — наценка 0.8%, то есть шум.

    Причина, по которой это вообще возможно: Steam флоат НЕ ПОКАЗЫВАЕТ — ни в
    поиске, ни в фильтрах. Покупателю пришлось бы открывать инспект-ссылку
    каждого лота вручную, поэтому низкий флоат там лежит по цене обычного. А
    CSFloat вырос из FloatDB, у него флоат — первоклассный признак с
    сортировкой и рангом. Разница между этими двумя площадками и есть весь
    смысл охоты за флоатом; команда отвечает, есть ли она на данном предмете.

    Как считается: два запроса к CSFloat — самые низкофлоатные лоты и самые
    высокофлоатные, в пределах ОДНОГО market_hash_name (а он включает износ,
    так что категория зафиксирована и сравниваются сопоставимые вещи).
    Сравниваем медианные цены двух групп.
    """
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text(
            "Проверить, платят ли за низкий флоат на конкретном скине.\n\n"
            "<code>/float чек AWP | Black Nile (Factory New)</code>\n"
            "<code>/float чек AWP | Black Nile (Factory New) 0.00585</code>\n\n"
            "Во втором виде скажу ещё и про твой флоат: насколько он низкий "
            "для этого предмета и попадает ли в ценимую зону.",
            parse_mode="HTML",
        )
        return
    if not csfloat_client.csfloat_enabled():
        await update.message.reply_text("Не задан CSFLOAT_API_KEY — спрашивать цены не у кого.")
        return

    # Последний аргумент может быть флоатом пользователя. Отделяем его от
    # названия так же, как это делает _split_args для мин$/макс% в /scan.
    args = list(context.args)
    my_float: float | None = None
    if len(args) > 1:
        try:
            candidate = float(args[-1].replace(",", "."))
        except ValueError:
            candidate = None
        if candidate is not None and 0 <= candidate <= 1:
            my_float = candidate
            args = args[:-1]

    raw = " ".join(args)
    market_hash_name = await _resolve_market_hash_name(
        update, raw, "floatcheck", DEFAULT_MIN_VALUE, DEFAULT_MAX_MARKUP
    )
    if market_hash_name is None:
        # Ждём выбора номера. Флоат пользователя надо пронести через это
        # ожидание, иначе после выбора он потеряется.
        pending = _pending_search.get(update.effective_chat.id)
        if pending is not None:
            pending["float"] = my_float
        return

    await _proceed_floatcheck(update, market_hash_name, my_float)


async def _proceed_floatcheck(update: Update, market_hash_name: str, my_float: float | None):
    """Собственно разбор предмета — отдельно, чтобы вызываться и после выбора номера."""
    await update.message.reply_text(f"Смотрю лоты «{market_hash_name}» на CSFloat…")
    proxy = csfloat_client.CSFLOAT_POOL.next() if csfloat_client.CSFLOAT_POOL.enabled() else None
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60), headers=csfloat_client._API_HEADERS,
        ) as session:
            # Два конца выборки: самые низкофлоатные лоты и самые высокофлоатные.
            # Оба в пределах одного market_hash_name, а он включает износ — то
            # есть категория зафиксирована и сравниваются сопоставимые вещи.
            low, _ = await csfloat_client.fetch_listings_page(
                session, sort_by="lowest_float",
                market_hash_name=market_hash_name, proxy=proxy,
            )
            high, _ = await csfloat_client.fetch_listings_page(
                session, sort_by="highest_float",
                market_hash_name=market_hash_name, proxy=proxy,
            )
    except CSFloatRateLimited as e:
        await update.message.reply_text(f"⏸ {e}")
        return
    except CSFloatError as e:
        await update.message.reply_text(f"⚠️ {e}")
        return

    priced = [l for l in low + high if l.float_value is not None and l.price > 0]
    if not priced:
        await update.message.reply_text(
            f"На CSFloat сейчас нет лотов «{market_hash_name}» с известным флоатом — "
            "сравнивать нечего."
        )
        return

    # Дубликаты: если лотов у предмета меньше сотни, обе выборки пересекаются.
    by_id = {l.listing_id: l for l in priced}
    lots = sorted(by_id.values(), key=lambda l: l.float_value)

    lines = [
        f"🔍 <b>{html_module.escape(market_hash_name)}</b>",
        f"Лотов на CSFloat в выборке: {len(lots)} "
        f"(флоат {lots[0].float_value:.5f} … {lots[-1].float_value:.5f})",
    ]

    if len(lots) < 4:
        # Двух-трёх лотов не хватает даже на грубое сравнение: одна случайная
        # цена сделает «наценку» любой. Честнее сказать, что вывода нет.
        lines.append(
            "\n⚠️ Лотов слишком мало для вывода — на такой выборке «наценка» "
            "будет случайной. Смотри руками."
        )
    else:
        half = len(lots) // 2
        low_group, high_group = lots[:half], lots[half:]
        low_med = statistics.median(l.price for l in low_group)
        high_med = statistics.median(l.price for l in high_group)
        premium = (low_med - high_med) / high_med * 100 if high_med > 0 else 0.0

        lines.append(
            f"\n<b>Нижняя половина по флоату</b> ({low_group[0].float_value:.5f}"
            f"–{low_group[-1].float_value:.5f})\n"
            f"  медиана цены ${low_med:.2f}"
        )
        lines.append(
            f"<b>Верхняя половина</b> ({high_group[0].float_value:.5f}"
            f"–{high_group[-1].float_value:.5f})\n"
            f"  медиана цены ${high_med:.2f}"
        )

        if premium >= FLOATCHECK_MEANINGFUL_PREMIUM_PCT:
            lines.append(
                f"\n✅ <b>За низкий флоат доплачивают: +{premium:.0f}%</b>\n"
                f"<i>Значит охота за флоатом на этом предмете имеет смысл.</i>"
            )
        elif premium <= -FLOATCHECK_MEANINGFUL_PREMIUM_PCT:
            lines.append(
                f"\n↕️ Низкофлоатные тут <b>дешевле</b> на {abs(premium):.0f}% — "
                "скорее всего цену определяет что-то другое (паттерн, наклейки), "
                "а не флоат."
            )
        else:
            lines.append(
                f"\n❌ <b>Наценки за флоат нет</b> (разница {premium:+.0f}%).\n"
                "<i>Покупать этот экземпляр ради флоата смысла нет: продать "
                "дороже обычного не выйдет.</i>"
            )

    if my_float is not None:
        lower = sum(1 for l in lots if l.float_value < my_float)
        cheapest = min(lots, key=lambda l: l.price)
        lines.append(
            f"\n<b>Твой флоат {my_float:.5f}</b>\n"
            f"  ниже него в выборке: {lower} из {len(lots)} лотов\n"
            f"  самый дешёвый лот на CSFloat: ${cheapest.price:.2f} "
            f"(флоат {cheapest.float_value:.5f})"
        )

    lines.append(
        "\n<i>Steam флоат не показывает вовсе — там низкий флоат лежит по цене "
        "обычного. CSFloat его ищет и ранжирует. В этом зазоре и есть смысл "
        "охоты; цифры выше говорят, есть ли он на этом предмете.</i>"
    )

    for chunk in _chunk_lines(lines):
        await update.message.reply_text(chunk, parse_mode="HTML", disable_web_page_preview=True)


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
            "Сбросить и попробовать сразу: /setarb сброс"
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

    # Берём ВЕСЬ текст сообщения, а не context.args. PTB режет аргументы по
    # пробелам, и при вставке длинного списка это лишний проход, который на
    # многострочном тексте легко теряет разделители. Пулу же всё равно — он
    # сам режет по запятым, пробелам и переводам строк.
    text = update.message.text or ""
    raw = text.split(None, 1)[1] if len(text.split(None, 1)) > 1 else ""

    result = csfloat_client.CSFLOAT_POOL.add(raw)
    STEAM_POOL.add(raw)
    added_cs, rejected = result.added, result.rejected

    if added_cs or result.duplicates:
        # Сохраняем ТОЛЬКО добавленное через бота (pool.extra()), а не весь пул.
        # Раньше цикл шёл по всем proxies подряд и утаскивал в хранилище заодно
        # адреса из переменной окружения. Последствие было обидное: /proxyclear
        # потом рапортовал «забыл 8», хотя руками добавляли один, — а по сути
        # чистил дубли env-адресов, которые всё равно вернулись бы при старте.
        await save_extra_proxies(csfloat_client.CSFLOAT_POOL.extra())

    # Отчитываемся по всем трём исходам. Сведённые в одно число «добавлено», они
    # выглядят как лимит, которого нет: вставил двадцать, семь новых — и кажется,
    # что бот больше семи не берёт.
    lines = [f"Разобрано адресов: {result.seen}"]
    lines.append(f"  ✅ добавлено: {result.added}")
    if result.duplicates:
        lines.append(f"  ↩️ уже были в пуле: {result.duplicates}")
    if rejected:
        lines.append(f"  ⚠️ не приняты: {len(rejected)}")
        for masked, problem in rejected[:5]:
            lines.append(f"      {masked} — {problem}")
        if len(rejected) > 5:
            lines.append(f"      …и ещё {len(rejected) - 5}")
    lines.append(f"\nВсего в пуле: {len(csfloat_client.CSFLOAT_POOL)} (предела нет)")
    lines.append(
        "Не влезло в одно сообщение — шли следующей командой, адреса складываются."
    )
    lines.append("Проверить их: /proxycheck")
    await update.message.reply_text("\n".join(lines))


async def proxyclear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /proxyclear      — забыть прокси, добавленные через бота
    /proxyclear all  — плюс отключить до рестарта адреса из переменной окружения

    Режим all нужен, когда провайдер отключил аккаунт целиком (403 на всех
    сессиях): ждать передеплоя ради того, чтобы бот перестал ходить на мёртвые
    адреса, незачем. Насовсем они убираются правкой CSFLOAT_HTTP_PROXY.
    """
    drop_env = bool(context.args) and context.args[0].lower() in ("all", "все", "всё")

    stored = await get_extra_proxies()
    await save_extra_proxies([])
    # Убираем из УЖЕ РАБОТАЮЩИХ пулов прямо сейчас, а не только из хранилища:
    # хранилище влияет лишь на следующий старт, а процесс продолжал бы таскать
    # эти адреса в памяти, пока Render сам не передеплоит бота.
    targets = list(stored)
    if drop_env:
        targets += csfloat_client.CSFLOAT_POOL.from_env() + STEAM_POOL.from_env()
    removed_cs = csfloat_client.CSFLOAT_POOL.remove(targets, include_env=drop_env)
    removed_steam = STEAM_POOL.remove(targets, include_env=drop_env)

    left_cs = len(csfloat_client.CSFLOAT_POOL)
    lines = [
        f"Убрано из пулов — CSFloat: -{removed_cs}, Steam: -{removed_steam}. "
        f"Рестарт не нужен."
    ]
    if stored:
        lines.append(f"Из хранилища забыто {len(stored)} адрес(ов), добавленных через бота.")
    else:
        lines.append("В хранилище добавленных через бота адресов не было.")

    if drop_env:
        lines.append(
            "\nАдреса из переменной окружения отключены ДО РЕСТАРТА — при следующем "
            "запуске они вернутся. Чтобы убрать насовсем, поправь CSFLOAT_HTTP_PROXY "
            "на Render."
        )
    elif left_cs:
        # Главное, чего не хватало раньше: если в пуле осталось что-то, надо
        # прямо сказать, откуда оно, — иначе «забыл 0» при семи живых адресах
        # выглядит как поломка команды, а не как отказ трогать чужое.
        lines.append(
            f"\nВ пуле осталось {left_cs} адрес(ов) — они из переменной окружения "
            f"CSFLOAT_HTTP_PROXY, и эта команда их не трогает.\n"
            f"Отключить их до рестарта: /proxyclear all\n"
            f"Убрать насовсем: поправь переменную на Render."
        )
    await update.message.reply_text("\n".join(lines))


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

    # Ограничиваем одновременность: на полусотне адресов безоглядный gather дал
    # бы сотню параллельных запросов к ipify, и он сам начал бы отвечать
    # отказом — проверка врала бы про «мёртвые» прокси, которые на деле живы.
    gate = asyncio.Semaphore(PROXYCHECK_CONCURRENCY)

    async def one(proxy: str):
        # Спрашиваем ДВАЖДЫ. Один замер говорит только «адрес такой-то», а нам
        # нужно знать другое: закреплён он за логином или меняется на каждый
        # запрос. От этого зависит вся стратегия кулдаунов — при ротации
        # откладывать логин после 429 бессмысленно, потому что банится адрес, а
        # в следующий раз за тем же логином будет уже другой.
        async with gate:
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
    # Построчный список адресов печатаем только у небольшого пула. На полусотне
    # он всё равно не влезет в сообщение Telegram, а главное — не нужен:
    # там важны итоги (сколько живых, сколько разных IP), а не портянка.
    detailed = len(results) <= PROXYCHECK_DETAIL_LIMIT
    lines = ["<b>Исходящие адреса прокси</b>"] if detailed else []
    for proxy, first, second, error in results:
        if first:
            ips[first] = ips.get(first, 0) + 1
            if second and second != first:
                rotating += 1
                if detailed:
                    lines.append(
                        f"  {proxy_pool.mask(proxy)} → <code>{first}</code>, "
                        f"потом <code>{second}</code> ⟳"
                    )
            elif detailed:
                lines.append(f"  {proxy_pool.mask(proxy)} → <code>{first}</code> (держится)")
        elif detailed:
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

    if not detailed:
        lines.insert(
            0,
            f"<b>Исходящие адреса прокси</b>\n<i>Адресов {len(results)} — построчный "
            f"список опущен, показываю итоги.</i>",
        )
    # Даже с опущенным списком сообщение может не влезть, если много разных IP.
    for chunk in _chunk_lines(lines):
        await update.message.reply_text(chunk, parse_mode="HTML")


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

    # Полос столько, сколько выдержит priceoverview, а НЕ сколько адресов в
    # пуле: этот эндпоинт режется жёстче всех, и 46 прокси означали 46
    # одновременных полос и 53 запроса в минуту (диагностика 2026-08-27).
    lanes = pricing.PRICE_CONCURRENCY
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

            # Неизвестный объём НЕ считаем нулевым. Цена, взятая из листингов
            # (запасной путь, когда priceoverview забанен), объёма не содержит
            # вовсе — и прежнее `(live.volume or 0)` молча отбраковало бы по
            # ликвидности вообще всё, что пришло этим путём. Тот же урок уже
            # выучен в analyzer.find_arbitrage_offers: «не знаем» и «не
            # продаётся» — разные вещи.
            if live.volume is not None and live.volume < min_volume:
                log.info(
                    "markets: %s — продаж за сутки %s, меньше %d: перепродать будет некому",
                    offer.market_hash_name, live.volume, min_volume,
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


class MarketsUnavailable(Exception):
    """Цены площадок не собрались. Текст уходит пользователю как есть."""


async def _collect_market_prices(session):
    """
    Цены площадок и Steam для сравнения: (источник, цены Steam, по площадкам, лотов).

    Основной источник — SIH: один запрос отдаёт весь каталог, причём цена
    покупки и цена Steam лежат в ОДНОЙ записи. Это не мелочь. Раньше два числа
    приходили из двух разных мест и сшивались у нас, и на проде они разошлись
    на 97% предметов с устойчивым отношением 2.3-2.7x — то есть кто-то из двоих
    считал в других единицах, а понять кто, имея только их спор, было нечем.
    Когда оба числа считает одна сторона, такого расхождения не может быть
    по построению.

    csgotrader остаётся запасным путём ровно на один случай — пока не задан
    SIH_API_KEY. Это не второй источник «для надёжности», а страховка от того,
    чтобы /markets не отвечал пустотой на боте без ключа.
    """
    if sih_client.enabled():
        items = await sih_client.fetch_items(session)
        steam_prices, by_market, counts = sih_client.split_by_market(items)
        if not steam_prices:
            raise MarketsUnavailable(
                f"SIH отдал {len(items)} предметов, но ни у одного нет цены Steam — "
                "сравнивать не с чем. Похоже, изменился формат ответа get-items."
            )
        log.info(
            "markets: источник SIH — %d предметов, с ценой Steam %d, площадок %d",
            len(items), len(steam_prices), len(by_market),
        )
        return "SIH", steam_prices, by_market, counts

    steam_prices = await get_csgotrader_prices(session)
    if not steam_prices:
        raise MarketsUnavailable(
            "Прайс-лист Steam не скачался — сравнивать не с чем.\n"
            "Основной источник цен теперь SIH: задай SIH_API_KEY в переменных "
            "окружения на Render."
        )
    by_market = await market_prices.load_markets(session)
    if not by_market:
        raise MarketsUnavailable(
            "Ни один файл площадок не открылся. Похоже, состав файлов на "
            "prices.csgotrader.app изменился.\n"
            "Основной источник цен теперь SIH: задай SIH_API_KEY на Render."
        )
    return "csgotrader", steam_prices, by_market, {}


async def markets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /markets [%] — сравнить Steam с площадками (источник — SIH).

    Отдельный от CSFloat канал арбитража, и главное его свойство — охват. Весь
    каталог приходит одним запросом, поэтому сравнивается он целиком, а не
    выборка лотов, и вопрос «почему только тысяча предметов» тут не возникает.

    Цена — лучшее предложение по предмету, а не конкретный лот: ни флоата, ни
    наклеек, ни прямой ссылки на лот. Зато есть count (сколько лотов) и market
    (на какой площадке) — этого раньше не было вовсе.
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
        f"Сравниваю Steam с площадками ({'SIH' if sih_client.enabled() else 'csgotrader'}).\n"
        f"Спред от {threshold:g}% до {max_discount:g}%, прибыль от ${min_profit:g}, "
        f"продаж в Steam от {min_volume} за сутки.\n"
        f"Пороги: /start → Пороги → Площадки"
    )

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        try:
            source, steam_prices, by_market, counts = await _collect_market_prices(session)
        except (sih_client.SihError, MarketsUnavailable) as e:
            await update.message.reply_text(f"⚠️ {e}")
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            # Сетевую ошибку ловим отдельно от отказа источника: молча упасть
            # в обработчик PTB значит оставить человека вообще без ответа.
            log.warning("markets: источник цен недоступен: %s", e)
            await update.message.reply_text(
                f"⚠️ Не достучался до источника цен ({type(e).__name__}). "
                "Попробуй ещё раз через минуту."
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
        for offer in found:
            offer.listing_count = counts.get(offer.market_hash_name)

    # Сколько лотов на площадке — это ликвидность на стороне ПОКУПКИ, и её
    # стоит показать до того, как человек пойдёт покупать: скидка на
    # единственном экземпляре живёт минуты.
    if counts:
        no_stock = sum(1 for o in found if o.listing_count == 0)
        if no_stock:
            found = [o for o in found if o.listing_count != 0]
            log.info("markets: отброшено %d находок без лотов в наличии", no_stock)

    if not found:
        # Площадок у SIH под три десятка — перечислять все значит утопить
        # смысл сообщения в списке названий.
        shown = ", ".join(sorted(available)[:8])
        if len(available) > 8:
            shown += f" и ещё {len(available) - 8}"
        await update.message.reply_text(
            f"Проверил площадки ({shown}) — дешевле Steam на {threshold:g}% "
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
            # Кто именно отказал — принципиально. Пул мёртвых прокси и
            # рейт-лимит Steam лечатся совершенно по-разному, а раньше оба
            # случая назывались «Steam ограничил доступ», и это отправляло
            # искать поломку не там.
            if STEAM_POOL.all_dead():
                reason = f"прокси не пропускают запросы: {STEAM_POOL.failure_hint()}"
                advice = "Пока прокси не заработают, сверять цены не с чем."
            else:
                reason = "Steam ограничил доступ"
                advice = "Попробуй через несколько минут — адреса освободятся."
            await update.message.reply_text(
                f"Кандидатов {before}, но проверить цены у Steam не удалось "
                f"({unavailable} из {min(before, MARKETS_VERIFY_LIMIT)} запросов не прошли — "
                f"{reason}). Это НЕ значит, что находок нет: их просто "
                f"не с чем было сверить.\n\n{advice}"
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
        f"<i>Источник: {source}, кэш обновляется раз в час. Это лучшее предложение "
        f"по предмету, а не конкретный лот: проверяй на площадке перед покупкой. "
        f"«Чистыми» — за вычетом комиссии Steam ~{(1 - STEAM_FEE_MULTIPLIER) * 100:.0f}%.</i>"
    ]
    for o in found[:15]:
        net = o.net_after_fee(STEAM_FEE_MULTIPLIER)
        # Число лотов знает только SIH. Пишем строку, лишь когда оно есть, —
        # «лотов: ?» рядом с реальными числами читается как сбой, а не как
        # «этот источник такого не отдаёт».
        stock = f" | лотов: {o.listing_count}" if o.listing_count else ""
        lines.append(
            f"<code>{html_module.escape(o.market_hash_name)}</code>\n"
            f"  {html_module.escape(o.market)} ${o.market_price:.2f} | "
            f"Steam ${o.steam_price:.2f} | дешевле на {o.discount_pct:.1f}%\n"
            f"  чистыми при перепродаже: {'+' if net >= 0 else '-'}${abs(net):.2f}"
            f" | продаж в Steam за сутки: {o.steam_volume if o.steam_volume is not None else '?'}"
            f"{stock}\n"
            f'  <a href="{o.steam_url}">Проверить в Steam</a>'
        )

    for chunk in _chunk_lines(lines, sep="\n\n"):
        await update.message.reply_text(chunk, parse_mode="HTML", disable_web_page_preview=True)


async def arbreset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/arbreset — старое имя, живёт как алиас. Смысл — в /setarb сброс."""
    await _arb_reset(update)


async def _arb_reset(update):
    """
    Снять кулдаун CSFloat вручную и показать, с чем мы к нему ходим.

    Кулдаун при блокировке по IP длинный (3 ч) и переживает передеплой, поэтому
    без ручного сброса нельзя проверить, помогла ли правка запроса: ждёшь не
    результат правки, а истечение таймера, который к ней отношения не имеет.

    Отдельной функцией от команды, потому что зовётся из трёх мест: старого
    /arbreset, подкоманды /setarb сброс и кнопки в разделе «Состояние».
    """
    cooldown = csfloat_client.cooldown_remaining()
    await csfloat_client.reset_cooldown()
    # Арбитраж упирается в ДВА независимых кулдауна: CSFloat (сбор лотов) и
    # Steam-область pricing (проверка цены живым priceoverview). Сбрасывать
    # только первый бессмысленно — прогон соберёт лоты и снова не подтвердит ни
    # одного кандидата, что 2026-08-27 и выглядело как «фикс не помог».
    price_cooldown = await steam_reset_cooldown("pricing")
    was = f"был {cooldown / 60:.0f} мин" if cooldown > 0 else "кулдауна и не было"
    lines = [
        f"✅ Кулдаун CSFloat сброшен ({was}).",
        f"✅ Кулдаун проверки цен Steam сброшен ("
        + (f"был {price_cooldown / 60:.0f} мин" if price_cooldown > 0 else "кулдауна и не было")
        + ").",
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
            "Оба списка пусты. Добавь предметы: /watch <предмет1>, <предмет2>, ... "
            "(охота по стикерам) или /float <предмет> (охота за флоатом)."
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
    report = await _run_watchlist_scan(
        context.bot, chat_id, request_interval=MANUAL_REQUEST_INTERVAL
    )
    # None сюда дойти не должен: списки непустые и "уже идёт" отсеяно выше,
    # но проверка дешёвая, а падать на отчёте о завершении не хочется.
    await update.message.reply_text(_format_scan_done(report) if report is not None else "Готово.")


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
            f"Сменить пороги: /start → Пороги → Стикеры. "
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
    """/status — старое имя, живёт как алиас. Смысл — в разделе «Состояние» меню /start."""
    for chunk in _chunk_lines(await _status_lines(update.effective_chat.id, context.job_queue)):
        await update.message.reply_text(chunk, parse_mode="HTML")


async def _status_lines(chat_id: int, jq) -> list[str]:
    """
    Одна сводка: что запущено, что на паузе, что на кулдауне, какие пороги
    отбора действуют.

    Появилась потому, что почти все вопросы при отладке были вида "почему
    ничего не приходит", а ответ приходилось собирать из логов Render: списки —
    в одной команде, пауза — во второй, кулдауны — вообще нигде. Здесь всё
    сразу и в одном месте.

    Возвращает строки, а не шлёт их: то же самое показывает раздел «Состояние»
    в меню, и рисовать его надо правкой существующего сообщения, а не новым.
    """
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
        lines.append("  ⚠️ оба пусты — сканировать нечего (/watch, /float)")

    lines.append("")
    lines.append("<b>Автоскан Steam</b>")
    if paused:
        lines.append("  ⏸ на паузе — /watch старт чтобы включить")
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
        lines.append("  CSFloat: " + (f"⏸ кулдаун ещё {_fmt_mins(cf)} (/setarb сброс)" if cf > 0 else "✅ свободен"))
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
        lines.append("  Флоат: ⚠️ порог не задан — охота за флоатом не идёт (/start → Пороги → Флоат)")
    else:
        extra = f", наценка ≤{f_markup:g}%" if f_markup is not None else ""
        lines.append(f"  Флоат: ≤{f_lo:g} или ≥{f_hi:g}{extra}")

    return lines


# ---------------------------------------------------------------------------
# Меню на кнопках
#
# Разделение простое: действия остались командами, пороги переехали сюда.
# Команду выигрывает частота — набрать /scanall быстрее, чем открыть меню и
# ткнуть. Кнопку выигрывает редкость плюс числовой параметр: пороги трогают
# раз в месяц, их восемнадцать, и синтаксис к следующему разу забывается.
# Подпись с примером в долларах прямо на экране объясняет порог так, как
# позиционные аргументы /setdefaults 5 7 не объяснят никогда.
#
# Старые команды никуда не делись и работают как раньше (см.
# _build_application) — меню их не заменяет, а даёт второй путь.
# ---------------------------------------------------------------------------

# Чат ждёт, что следующим сообщением придёт значение порога: chat_id -> key.
# Отдельно от _pending_search, потому что это разные ожидания и путать их
# нельзя: там ждут номер варианта предмета, здесь — число.
_pending_setting: dict[int, str] = {}


class _MenuMessage:
    """
    Подменяет update.message для команд, запущенных кнопкой.

    Обработчики написаны под update.message.reply_text, а у callback-запроса
    message — это сообщение с самим меню, отвечать в которое неправильно:
    ответ должен прийти новым сообщением, а меню остаться на месте.
    """

    def __init__(self, chat_id: int, bot):
        self.chat_id = chat_id
        self.text = ""
        self._bot = bot

    async def reply_text(self, text, **kwargs):
        return await self._bot.send_message(chat_id=self.chat_id, text=text, **kwargs)


class _MenuUpdate:
    """Переходник Update -> обработчик команды, когда команду запустили кнопкой."""

    def __init__(self, query, bot):
        self.callback_query = query
        self.effective_chat = query.message.chat
        self.effective_user = query.from_user
        self.message = _MenuMessage(query.message.chat.id, bot)


def _fmt_money(value) -> str:
    return "не задано" if value is None else f"${value:,.2f}".replace(",", " ")


def _fmt_pct(value) -> str:
    return "выключено" if value is None else f"{value:g}%"


async def _settings_snapshot(chat_id: int) -> dict[str, str]:
    """Текущее значение каждого порога человеческой строкой, по ключам menu.SETTINGS."""
    def_min, def_max = await _get_defaults(chat_id)
    streak = await get_streak_markup(chat_id)
    ratio = await get_sticker_ratio(chat_id)
    p_lo, p_hi = await get_price_filter(chat_id)
    f_lo, f_hi = await get_float_filter(chat_id)
    f_markup = await get_float_markup(chat_id)
    arb = await get_arb_settings(chat_id)
    mk = await get_market_settings(chat_id)
    watch_interval = await _get_watch_interval(chat_id)

    def pair(lo, hi, fmt=_fmt_money) -> str:
        if lo is None and hi is None:
            return "выключено"
        return f"{fmt(lo)} … {fmt(hi)}"

    def mk_value(key, default, fmt):
        value = mk[key] if mk[key] is not None else default
        return fmt(value)

    return {
        "st_min": _fmt_money(def_min),
        "st_markup": f"{def_max:g}% их цены",
        "st_streak": _fmt_pct(streak),
        "st_ratio": "выключено" if ratio is None else f"×{ratio:g}",
        "st_price": pair(p_lo, p_hi),
        "fl_range": (
            "выключено"
            if f_lo is None or f_hi is None
            else f"≤{f_lo:g} или ≥{f_hi:g}"
        ),
        "fl_markup": _fmt_pct(f_markup),
        "ar_disc": _fmt_pct(arb["min_discount"]),
        "ar_int": f"{_arb_interval(arb):g} мин",
        "ar_price": pair(arb["min_price"], arb["max_price"]),
        "ar_vol": (
            "выключено"
            if arb["min_volume"] is None
            else f"от {arb['min_volume']} продаж"
        ),
        "ar_stick": _fmt_pct(arb["sticker_markup"]),
        "mk_disc": mk_value("min_discount", MARKETS_DEFAULT_DISCOUNT, _fmt_pct),
        "mk_max": mk_value("max_discount", market_prices.MAX_SANE_DISCOUNT_PCT, _fmt_pct),
        "mk_vol": mk_value("min_volume", MARKETS_MIN_VOLUME, lambda v: f"от {v:g} продаж"),
        "mk_profit": mk_value("min_profit", market_prices.MIN_NET_PROFIT, _fmt_money),
        "mk_price": mk_value("min_price", market_prices.MIN_MARKET_PRICE, _fmt_money),
        "wt_int": f"{watch_interval:g} мин",
    }


def _parse_setting(setting: menu.Setting, raw: str):
    """
    Разобрать введённое значение. Бросает ValueError с человеческим текстом —
    он и уйдёт пользователю, поэтому формулировки тут не отладочные.

    Возвращает готовое к записи значение: число, пару чисел или None для
    «выключить».
    """
    text = raw.strip().lower().replace(",", ".").replace("$", "").replace("%", "")

    if text in ("off", "выкл", "выключить", "убрать", "-"):
        if not setting.can_off:
            raise ValueError(f"«{setting.label}» нельзя выключить — нужно число.")
        return None

    parts = text.split()

    if setting.kind in ("pair_money", "pair_float"):
        if len(parts) != 2:
            raise ValueError(f"Нужно два числа через пробел. Пример: {setting.example}")
        try:
            lo, hi = float(parts[0]), float(parts[1])
        except ValueError:
            raise ValueError(f"Оба значения должны быть числами. Пример: {setting.example}")
        if setting.kind == "pair_float":
            if not (0.0 <= lo <= 1.0 and 0.0 <= hi <= 1.0):
                raise ValueError("Флоат — число от 0 до 1.")
            if lo >= hi:
                raise ValueError("Первое число (низкий флоат) должно быть меньше второго.")
        elif lo > hi:
            raise ValueError("Минимум не может быть больше максимума.")
        return (lo, hi)

    if len(parts) != 1:
        raise ValueError(f"Нужно одно число. Пример: {setting.example}")
    try:
        value = float(parts[0])
    except ValueError:
        raise ValueError(f"{raw.strip()!r} — не число. Пример: {setting.example}")

    if setting.kind == "int":
        if value < 0:
            raise ValueError("Число не может быть отрицательным.")
        return int(value)
    if setting.kind in ("minutes", "ratio"):
        if value <= 0:
            raise ValueError("Значение должно быть больше нуля.")
        return value
    if value < 0:
        raise ValueError("Отрицательное значение не имеет смысла.")
    return value


async def _apply_setting(chat_id: int, key: str, value, context) -> str:
    """
    Записать порог и вернуть подтверждение.

    Здесь же — побочные эффекты расписания: интервалы и включение арбитража
    меняют джобы, и без пересоздания новое значение вступило бы в силу только
    после следующего прогона, то есть через старый интервал. На этом уже
    обжигались в /setinterval, поэтому повторяем то же и тут.
    """
    setting = menu.BY_KEY[key]
    jq = context.application.job_queue

    if key in ("st_min", "st_markup"):
        cur_min, cur_max = await _get_defaults(chat_id)
        if key == "st_min":
            await set_chat_defaults(chat_id, value, cur_max)
            return f"✅ Минимум наклеек на лоте: {_fmt_money(value)}"
        await set_chat_defaults(chat_id, cur_min, value)
        return (
            f"✅ Доплата за наклейки: не больше {value:g}% их цены.\n"
            f"Например: наклеек на $40 — берём лот, если он дороже голого скина "
            f"не больше чем на ${40 * value / 100:.2f}."
        )

    if key == "st_streak":
        await set_streak_markup(chat_id, value)
        return (
            "✅ Стрик-лоты снова по общему порогу."
            if value is None
            else f"✅ Доплата для стрик-лотов ({STREAK_THRESHOLD}+ подряд): ≤{value:g}%"
        )

    if key == "st_ratio":
        await set_sticker_ratio(chat_id, value)
        return (
            "✅ Фильтр по весу наклеек выключен."
            if value is None
            else f"✅ Наклейки должны быть дороже голого скина в {value:g} раз."
        )

    if key == "st_price":
        lo, hi = (None, None) if value is None else value
        await set_price_filter(chat_id, lo, hi)
        return (
            "✅ Фильтр цены лота убран."
            if value is None
            else f"✅ Цена лота: {_fmt_money(lo)} … {_fmt_money(hi)}"
        )

    if key == "fl_range":
        lo, hi = (None, None) if value is None else value
        await set_float_filter(chat_id, lo, hi)
        return (
            "✅ Охота за флоатом выключена."
            if value is None
            else f"✅ Ищу флоат ≤{lo:g} или ≥{hi:g}."
        )

    if key == "fl_markup":
        await set_float_markup(chat_id, value)
        return (
            "✅ Ограничение по цене для флоат-находок убрано."
            if value is None
            else f"✅ Флоат-находка проходит, если дороже дешёвого лота не более чем на {value:g}%."
        )

    if key == "ar_disc":
        await set_arb_setting(chat_id, "min_discount", value)
        if value is None:
            for job in jq.get_jobs_by_name(f"{ARB_JOB_PREFIX}{chat_id}"):
                job.schedule_removal()
            return "✅ Арбитраж выключен."
        _schedule_arb_job(jq, chat_id, delay_minutes=0.2)
        return f"✅ Арбитраж включён: дешевле Steam минимум на {value:g}%. Первый прогон — сейчас."

    if key == "ar_int":
        await set_arb_setting(chat_id, "interval", value)
        arb = await get_arb_settings(chat_id)
        text = f"✅ Интервал автоскана арбитража: {value:g} мин."
        if arb["min_discount"] is not None:
            _schedule_arb_job(jq, chat_id, value)
            text += "\nРасписание обновлено."
        problem = _arb_budget_problem(value)
        if problem:
            text += "\n\n" + problem
        return text

    if key == "ar_price":
        lo, hi = (None, None) if value is None else value
        await set_arb_setting(chat_id, "min_price", lo)
        await set_arb_setting(chat_id, "max_price", hi)
        return (
            "✅ Ограничение по цене в арбитраже снято."
            if value is None
            else f"✅ Арбитраж: лоты {_fmt_money(lo)} … {_fmt_money(hi)}"
        )

    if key == "ar_vol":
        await set_arb_setting(chat_id, "min_volume", value)
        return (
            "✅ Фильтр ликвидности в арбитраже снят."
            if value is None
            else f"✅ Арбитраж: только предметы с {value}+ продаж на Steam."
        )

    if key == "ar_stick":
        await set_arb_setting(chat_id, "sticker_markup", value)
        return (
            "✅ Отбор по наклейкам в арбитраже выключен."
            if value is None
            else f"✅ Арбитраж: плюс лоты с доплатой за наклейки ≤{value:g}%."
        )

    if key.startswith("mk_"):
        storage_key = {
            "mk_disc": "min_discount", "mk_max": "max_discount",
            "mk_vol": "min_volume", "mk_profit": "min_profit",
            "mk_price": "min_price",
        }[key]
        await set_market_setting(chat_id, storage_key, value)
        return f"✅ {setting.label}: {value:g}\n\nПроверить прямо сейчас: /markets"

    if key == "wt_int":
        await set_watch_gap(chat_id, value)
        text = f"✅ Пауза между прогонами автоскана: {value:g} мин."
        if not await get_watch_paused(chat_id):
            _schedule_watchlist_job(jq, chat_id, value)
            text += "\nРасписание обновлено."
        return text

    raise ValueError(f"Неизвестный порог {key!r}")


async def _menu_screen(chat_id: int, node: str, context) -> tuple[str, object]:
    """Текст и клавиатура для узла меню. Один вход — чтобы «назад» и «обновить» шли тем же путём."""
    if node == "root":
        return (
            "🤖 <b>Dextrade</b>\n"
            "Слежу за скинами CS2 и присылаю выгодные лоты.\n\n"
            "<i>Команды никуда не делись: /watch, /float, /scan, /setarb, "
            "/inv, /markets. Полный список — /help.</i>",
            menu.root(),
        )

    if node == "lists":
        sticker_items = await get_watchlist(chat_id)
        float_items = await get_float_watchlist(chat_id)
        paused = await get_watch_paused(chat_id)
        interval = await _get_watch_interval(chat_id)
        lines = [
            "📋 <b>Списки</b>",
            "",
            f"Вотчлист (стикеры): <b>{len(sticker_items)}</b> — /watch",
            f"Охота за флоатом: <b>{len(float_items)}</b> — /float",
            "",
            (
                "⏸ Автоскан на паузе."
                if paused
                else f"▶️ Автоскан включён, пауза между прогонами {interval:g} мин."
            ),
        ]
        if not sticker_items and not float_items:
            lines.append("")
            lines.append(
                "⚠️ Оба списка пусты — сканировать нечего.\n"
                "Добавить: <code>/watch AK-47 | Redline (Field-Tested)</code>"
            )
        return "\n".join(lines), menu.lists(paused)

    if node == "state":
        text = "\n".join(await _status_lines(chat_id, context.job_queue))
        show_reset = (
            csfloat_client.cooldown_remaining() > 0
            or steam_cooldown_remaining("pricing") > 0
        )
        return text, menu.state(show_reset)

    if node == "set":
        return (
            "⚙️ <b>Пороги</b>\n\n"
            "Что и когда бот считает находкой. Выбери раздел — внутри видно "
            "текущие значения и что каждое означает.",
            menu.sections(),
        )

    if node.startswith("set:"):
        section_key = node.split(":", 1)[1]
        section = menu.SECTION_BY_KEY.get(section_key)
        if section is None:
            return "Такого раздела нет.", menu.sections()
        values = await _settings_snapshot(chat_id)
        lines = [f"<b>{html_module.escape(section.title)}</b>", "", f"<i>{section.intro}</i>", ""]
        for setting in menu.BY_SECTION[section_key]:
            lines.append(f"<b>{html_module.escape(setting.label)}</b>: {values[setting.key]}")
        lines.append("")
        lines.append("<i>Нажми на порог, чтобы поменять.</i>")
        return "\n".join(lines), menu.section(section_key)

    if node == "proxy":
        stored = await get_extra_proxies()
        lines = [
            "🌐 <b>Прокси</b>",
            "",
            f"Всего в пуле CSFloat: <b>{len(csfloat_client.CSFLOAT_POOL)}</b>",
            f"Пул Steam: {STEAM_POOL.describe()}",
            f"Добавлено через бота: {len(stored)}",
            "",
            "Добавить: <code>/proxyadd http://логин:пароль@хост:порт</code>\n"
            "<i>Можно сколько угодно за раз — через запятую, пробел или с новой строки.</i>",
        ]
        return "\n".join(lines), menu.proxy()

    if node == "prices":
        return (
            "📄 <b>Прайс-лист стикеров</b>\n\n"
            f"Сейчас в нём <b>{manual_prices_count()}</b> цен.\n\n"
            "<i>Нужен, когда автоматические цены стикеров недоступны. "
            "Файл можно и просто прислать в чат — команда не обязательна.</i>",
            menu.prices(),
        )

    return "Не знаю такого экрана.", menu.root()


async def _show_menu(query, chat_id: int, node: str, context) -> None:
    """Перерисовать меню на месте. Одинаковый текст Telegram отвергает — это не ошибка."""
    text, keyboard = await _menu_screen(chat_id, node, context)
    try:
        await query.edit_message_text(
            text, parse_mode="HTML", reply_markup=keyboard, disable_web_page_preview=True
        )
    except Exception as e:
        if "not modified" not in str(e).lower():
            raise


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Единая точка разбора нажатий: навигация, правка порога, действие."""
    query = update.callback_query
    # Отвечаем сразу: пока answer() не пришёл, у пользователя крутится
    # ожидание на кнопке, а действия вроде /scanall идут минутами.
    await query.answer()

    chat_id = query.message.chat.id
    data = query.data or ""
    kind, _, payload = data.partition("|")

    if kind == menu.NAV:
        await _show_menu(query, chat_id, payload or "root", context)
        return

    if kind == menu.EDIT:
        setting = menu.BY_KEY.get(payload)
        if setting is None:
            await _show_menu(query, chat_id, "set", context)
            return
        _pending_setting[chat_id] = setting.key
        values = await _settings_snapshot(chat_id)
        text = (
            f"<b>{html_module.escape(setting.label)}</b>\n"
            f"сейчас: {values[setting.key]}\n\n"
            f"<i>{html_module.escape(setting.hint)}</i>\n\n"
            f"Пришли новое значение сообщением. Пример: <code>{setting.example}</code>"
        )
        try:
            await query.edit_message_text(
                text, parse_mode="HTML", reply_markup=menu.editing(setting)
            )
        except Exception:
            log.exception("menu: не смог показать экран правки %s", setting.key)
        return

    if kind != menu.ACT:
        return

    # --- Действия ----------------------------------------------------------
    shim = _MenuUpdate(query, context.bot)

    if payload.startswith("off:"):
        key = payload.split(":", 1)[1]
        setting = menu.BY_KEY.get(key)
        if setting is None or not setting.can_off:
            return
        _pending_setting.pop(chat_id, None)
        try:
            confirmation = await _apply_setting(chat_id, key, None, context)
        except ValueError as e:
            await shim.message.reply_text(str(e))
            return
        await shim.message.reply_text(confirmation)
        await _show_menu(query, chat_id, f"set:{setting.section}", context)
        return

    if payload == "scanall":
        await scanall(shim, context)
    elif payload == "arbnow":
        await arbnow(shim, context)
    elif payload == "markets":
        await markets(shim, _SubCtx(context, []))
    elif payload == "pause":
        await watchpause(shim, context)
        await _show_menu(query, chat_id, "lists", context)
    elif payload == "resume":
        await watchresume(shim, context)
        await _show_menu(query, chat_id, "lists", context)
    elif payload == "arbreset":
        await _arb_reset(shim)
        await _show_menu(query, chat_id, "state", context)
    elif payload == "proxycheck":
        await proxycheck(shim, context)
    elif payload == "proxyclear":
        await proxyclear(shim, _SubCtx(context, []))
        await _show_menu(query, chat_id, "proxy", context)
    elif payload == "pricefile":
        await pricefile(shim, context)
    elif payload == "clearprices":
        await clearprices(shim, context)
        await _show_menu(query, chat_id, "prices", context)
    else:
        log.warning("menu: неизвестное действие %r", payload)


async def _handle_pending_setting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Значение порога, присланное после нажатия кнопки. True — сообщение съедено.

    Вызывается ПЕРВЫМ в обработчике текста: если чат ждёт число для порога,
    трактовать это сообщение как что-то ещё нельзя.
    """
    chat_id = update.effective_chat.id
    key = _pending_setting.get(chat_id)
    if key is None:
        return False

    setting = menu.BY_KEY.get(key)
    if setting is None:
        del _pending_setting[chat_id]
        return False

    raw = (update.message.text or "").strip()
    try:
        value = _parse_setting(setting, raw)
    except ValueError as e:
        # Ожидание НЕ снимаем: человек ошибся в формате, а не передумал.
        await update.message.reply_text(f"{e}\n\nПришли значение ещё раз или /start, чтобы выйти.")
        return True

    del _pending_setting[chat_id]
    try:
        confirmation = await _apply_setting(chat_id, key, value, context)
    except ValueError as e:
        await update.message.reply_text(str(e))
        return True

    await update.message.reply_text(confirmation)
    return True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/start — главное меню. Всё остальное достижимо отсюда в один-два нажатия."""
    _pending_setting.pop(update.effective_chat.id, None)
    text, keyboard = await _menu_screen(update.effective_chat.id, "root", context)
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=keyboard)


# Порядок групп в /help — по сценарию, а не по алфавиту: сначала то, с чего
# начинают, в конце служебное.
_HELP_GROUPS = (
    "Начать",
    "Списки",
    "Арбитраж CSFloat ↔ Steam",
    "Площадки",
    "Инвентарь",
    "Служебное",
)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/help — справочник, собранный из реестра команд, а не написанный руками."""
    blocks = [
        "🤖 <b>Dextrade</b> — слежу за скинами CS2 и присылаю выгодные лоты.\n"
        "Пороги отбора живут в меню: /start → Пороги."
    ]

    for group in _HELP_GROUPS:
        entries = [c for c in COMMANDS if c.group == group and c.help]
        if not entries:
            continue
        blocks.append(f"<b>{group}</b>")
        blocks.extend(html_module.escape(c.help) for c in entries)

    aliases = [c.name for c in COMMANDS if c.group == "Старые имена"]
    blocks.append(
        "<b>Старые имена</b>\n"
        "Работают по-прежнему, просто убраны из меню:\n"
        + ", ".join(f"/{name}" for name in aliases)
    )

    blocks.append(
        f"<i>Выручка от продажи в Steam приходит на кошелёк и не выводится, "
        f"поэтому в сообщениях показываю «чистыми» с учётом комиссии "
        f"~{(1 - STEAM_FEE_MULTIPLIER) * 100:.0f}%.</i>"
    )

    for chunk in _chunk_lines(blocks, sep="\n\n"):
        await update.message.reply_text(chunk, parse_mode="HTML")


# ---------------------------------------------------------------------------
# Реестр команд: одно место, из которого берутся и меню Telegram, и /help,
# и регистрация обработчиков.
#
# Раньше это были три независимых списка, и они разъехались: в /help,
# заявленном как «полный справочник», не хватало тринадцати команд из
# тридцати девяти — целых функций вроде инвентаря, площадок и прокси. Причём
# заметить это было нельзя: справочник был литеральной строкой, и добавить
# команду, не тронув её, ничего не мешало.
#
# Теперь источник один. Забыть описать команду больше не выйдет: она просто
# не зарегистрируется, потому что _build_application ходит по этому же списку.
# ---------------------------------------------------------------------------

class Command(NamedTuple):
    name: str
    handler: object
    group: str
    # Подпись в меню Telegram (то, что видно при вводе "/"). None — команда
    # работает, но в меню не показывается: список в тридцать строк
    # бесполезен, туда идут только регулярно нужные.
    menu: str | None
    # Строка в /help. None — это старое имя-алиас, оно попадёт в общий
    # список в конце справочника, а не отдельным абзацем.
    help: str | None = None


COMMANDS: tuple[Command, ...] = (
    # --- С чего начать -----------------------------------------------------
    Command(
        "start", start, "Начать",
        "Меню: сканы, списки, состояние, пороги",
        "/start — меню на кнопках. Оттуда доступны все пороги отбора, "
        "состояние бота и прокси — синтаксис помнить не нужно.",
    ),
    Command(
        "scanall", scanall, "Начать",
        "Проверить оба списка прямо сейчас",
        "/scanall — прогнать вотчлист и охоту за флоатом немедленно, "
        "не дожидаясь расписания.",
    ),
    Command(
        "scan", scan, "Начать",
        "Проверить один предмет",
        "/scan <ссылка или название> [мин$ стикеров] [макс наценка%] — разовая "
        "проверка одного предмета. Название на английском, с | или без: "
        "/scan AK-47 | Slate или /scan AK-47 Slate.",
    ),
    # --- Списки ------------------------------------------------------------
    Command(
        "watch", watch, "Списки",
        "Вотчлист: показать, добавить, убрать",
        "/watch — показать вотчлист (охота по стикерам)\n"
        "/watch <предмет1>, <предмет2> — добавить; степень износа можно указать "
        "один раз последним элементом (FN/MW/FT/WW/BS)\n"
        "/watch -3 — убрать третий (или /watch убрать 3)\n"
        "/watch очистить — очистить список\n"
        "/watch стоп | /watch старт — пауза автоскана и обратно",
    ),
    Command(
        "float", float_cmd, "Списки",
        "Флоат: список и проверка скина",
        "/float — список охоты за редким флоатом и условие отбора\n"
        "/float <предмет1>, <предмет2> — добавить\n"
        "/float -2 — убрать второй, /float очистить — очистить\n"
        "/float чек <предмет> [флоат] — платят ли за низкий флоат именно на "
        "этом скине\n"
        "Список отдельный от /watch: флоат имеет смысл искать на конкретных "
        "скинах. Предмет может быть в обоих — тогда за прогон он тянется из "
        "Steam один раз и проверяется по обоим критериям.",
    ),
    # --- Арбитраж ----------------------------------------------------------
    Command(
        "arbnow", arbnow, "Арбитраж CSFloat ↔ Steam",
        "Проверить арбитраж прямо сейчас",
        "/arbnow — проверить рынок CSFloat немедленно.",
    ),
    Command(
        "setarb", setarb, "Арбитраж CSFloat ↔ Steam",
        "Арбитраж: порог, интервал, сброс",
        "/setarb — показать настройки\n"
        "/setarb <мин%> — включить: слать лоты CSFloat дешевле цены Steam\n"
        "/setarb <мин%> <минут> — то же плюс пауза между прогонами, "
        "например /setarb 30 9\n"
        "/setarb сброс — снять кулдаун CSFloat\n"
        "/setarb off — выключить\n"
        "Остальные пороги арбитража — /start → Пороги → Арбитраж.",
    ),
    # --- Площадки ----------------------------------------------------------
    Command(
        "markets", markets, "Площадки",
        "Сравнить Steam с другими площадками",
        "/markets [мин%] — сравнить цены Steam со сторонними площадками по "
        "всему каталогу. Пороги — /start → Пороги → Площадки.",
    ),
    # --- Инвентарь ---------------------------------------------------------
    Command(
        "inv", inv, "Инвентарь",
        "Инвентарь Steam: оценить и следить",
        "/inv — оценка привязанного инвентаря\n"
        "/inv <ссылка на профиль> — привязать аккаунт (инвентарь должен быть "
        "открыт)\n"
        "/inv следить 15 — сообщать, когда предмет подорожал на 15%\n"
        "/inv следить off — выключить слежение, /inv off — отвязать аккаунт",
    ),
    # --- Служебное ---------------------------------------------------------
    Command(
        "proxyadd", proxyadd, "Служебное",
        "Добавить прокси прямо из чата",
        "/proxyadd <адреса> — добавить прокси без передеплоя. Сколько угодно "
        "за раз: через запятую, пробел или с новой строки. Проверить и "
        "почистить — /start → Прокси.",
    ),
    Command(
        "scanfile", scanfile, "Служебное", None,
        "/scanfile <ссылка> — резервный ручной путь, когда автоматический "
        "запрос не удался (например, IP на кулдауне после 429). Бот пришлёт "
        "ссылку на JSON: открой в браузере, сохрани Ctrl+S и пришли файл сюда. "
        "Файл можно слать и без команды.",
    ),
    Command(
        "help", help_cmd, "Служебное",
        "Справочник по всем командам",
        "/help — то, что ты сейчас читаешь.",
    ),

    # --- Старые имена ------------------------------------------------------
    # Ничего не стоят, а мышечная память на них уже есть. Из меню Telegram
    # убраны, в справочнике перечислены одной строкой.
    Command("watchadd", watchadd, "Старые имена", None),
    Command("watchdel", watchdel, "Старые имена", None),
    Command("watchlist", watchlist_cmd, "Старые имена", None),
    Command("watchclear", watchclear, "Старые имена", None),
    Command("watchpause", watchpause, "Старые имена", None),
    Command("watchresume", watchresume, "Старые имена", None),
    Command("floatadd", floatadd, "Старые имена", None),
    Command("floatdel", floatdel, "Старые имена", None),
    Command("floatlist", floatlist_cmd, "Старые имена", None),
    Command("floatclear", floatclear, "Старые имена", None),
    Command("floatcheck", floatcheck, "Старые имена", None),
    Command("invwatch", invwatch, "Старые имена", None),
    Command("arbreset", arbreset, "Старые имена", None),
    Command("status", status, "Старые имена", None),
    Command("setdefaults", setdefaults, "Старые имена", None),
    Command("setstreakmarkup", setstreakmarkup, "Старые имена", None),
    Command("setpricefilter", setpricefilter, "Старые имена", None),
    Command("setfloatfilter", setfloatfilter, "Старые имена", None),
    Command("setfloatmarkup", setfloatmarkup, "Старые имена", None),
    Command("setratio", setratio, "Старые имена", None),
    Command("setinterval", setinterval, "Старые имена", None),
    Command("setmarkets", setmarkets, "Старые имена", None),
    Command("setarbprice", setarbprice, "Старые имена", None),
    Command("setarbvolume", setarbvolume, "Старые имена", None),
    Command("setarbstickers", setarbstickers, "Старые имена", None),
    Command("proxycheck", proxycheck, "Старые имена", None),
    Command("proxyclear", proxyclear, "Старые имена", None),
    Command("pricefile", pricefile, "Старые имена", None),
    Command("clearprices", clearprices, "Старые имена", None),
)


BOT_COMMANDS = [
    BotCommand(cmd.name, cmd.menu) for cmd in COMMANDS if cmd.menu
]


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
        cs_result = csfloat_client.CSFLOAT_POOL.add(raw)
        steam_result = STEAM_POOL.add(raw)
        log.info(
            "прокси: из хранилища добавлено %d в пул CSFloat и %d в пул Steam "
            "(разобрано %d, дубликатов %d)",
            cs_result.added, steam_result.added, cs_result.seen, cs_result.duplicates,
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
        _schedule_arb_job(app.job_queue, chat_id, _arb_interval(settings))
        arb_restored += 1
    if arb_restored:
        log.info("arb: восстановлены джобы арбитража для %d чат(ов)", arb_restored)

    # Слежение за инвентарём — так же по настройкам, а не по вотчлисту. Без
    # восстановления оно молча умирало бы на каждом передеплое Render, а
    # передеплоивает он часто: пользователь включил /invwatch и потом неделю
    # ждал бы уведомлений, которых никто не собирался слать.
    inv_restored = 0
    for chat_id in await all_chat_ids_with_settings():
        if await get_inventory_growth(chat_id) is None:
            continue
        if not await get_inventory_steamid(chat_id):
            continue
        _schedule_inventory_job(app.job_queue, chat_id)
        inv_restored += 1
    if inv_restored:
        log.info("inventory: восстановлены джобы слежения для %d чат(ов)", inv_restored)


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
        await close_http_session()
    return True


def _build_application(token: str):
    """
    Собрать Application со всеми обработчиками. Отдельной функцией, потому что
    при откате с webhook на polling объект приходится строить заново: он уже
    прошёл initialize()/shutdown(), а run_polling() зовёт initialize() сам, и
    дважды инициализировать один и тот же Application нельзя.
    """
    app = Application.builder().token(token).post_init(_on_startup).build()
    # Из одного реестра — и обработчики, и меню Telegram, и /help. Раньше это
    # были три списка, и справочник от них отставал на треть команд.
    for cmd in COMMANDS:
        app.add_handler(CommandHandler(cmd.name, cmd.handler))
    app.add_handler(CallbackQueryHandler(menu_callback))
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
