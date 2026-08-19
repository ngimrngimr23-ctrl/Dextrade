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
import hashlib
import html as html_module
import json
import logging
import os
import re
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from telegram import BotCommand, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from steam_client import (
    fetch_all_listings,
    market_hash_name_from_url,
    render_url,
    _parse_listings_html,
    RENDER_COUNT,
    STEAM_PROXY_URL,
    SteamRateLimited,
    steam_cooldown_remaining,
    load_persisted_cooldown,
)
from csgo_api import search_items as search_csgo_items
from pricing import get_sticker_prices, ingest_manual_prices, clear_manual_prices, manual_prices_count
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
    mark_offer_sent,
    get_arb_settings,
    set_arb_setting,
    all_chat_ids_with_settings,
)
import csfloat_client
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
ARB_INTERVAL_MINUTES = 5.0
ARB_PAGES_PER_SCAN = 4  # 4 страницы по 50 = до 200 лотов за прогон, самых дешёвых относительно Steam
_arb_running: set[int] = set()

# chat_id -> идёт прогон вотчлиста прямо сейчас — защита от наложения тиков,
# если сканирование всех предметов не укладывается в заданный интервал.
_watchlist_running: set[int] = set()


async def _get_watch_interval(chat_id: int) -> float:
    return WATCH_GAP_MINUTES


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


async def _run_watchlist_scan(bot, chat_id: int) -> bool | None:
    """
    Прогоняет весь вотчлист чата разом — общая логика для джобы по расписанию
    и команды /scanall. Возвращает True/False (нашлось ли хоть что-то) или
    None, если прогон не запустился (пустой список / уже идёт другой прогон).
    """
    if chat_id in _watchlist_running:
        log.info("watchlist: прогон для chat_id=%s уже идёт, пропускаю повторный запуск", chat_id)
        return None

    sticker_items = await get_watchlist(chat_id)
    float_items = await get_float_watchlist(chat_id)
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
        cooldown = steam_cooldown_remaining()
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
        f"<i>«Чистыми» = сколько останется, если перепродать в Steam по текущей цене, "
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
        block = (
            f"<b>{html_module.escape(o.market_hash_name)}</b>\n"
            f"  CSFloat ${o.csfloat_price:.2f} | Steam ${o.steam_price:.2f} | {price_cmp}\n"
            f"  чистыми при перепродаже: {net}"
        )
        if o.float_value is not None:
            block += f"\n  флоат {o.float_value:.5f}"
        if o.steam_volume is not None:
            block += f" | продаж на Steam: {o.steam_volume}"
        if o.stickers:
            block += f"\n  <code>{html_module.escape(', '.join(o.stickers))}</code>"
            if o.stickers_value > 0:
                block += f"\n  наклейки ≈${o.stickers_value:.2f}"
                if o.sticker_markup_pct is not None:
                    block += f", наценка за них {o.sticker_markup_pct:.1f}%"
            if o.stickers_unpriced:
                block += f" (у {o.stickers_unpriced} цена неизвестна)"
        block += f'\n  <a href="{o.url}">Открыть на CSFloat</a>'
        lines.append(block)

    return _chunk_lines(lines, sep="\n\n")


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
        listings = await csfloat_client.fetch_market(
            pages=ARB_PAGES_PER_SCAN,
            sort_by="highest_discount",
            min_price=settings["min_price"],
            max_price=settings["max_price"],
        )
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
        if not offers:
            return 0

        # Тот же дедуп, что у вотчлиста: один и тот же лот не присылаем повторно
        new_offers = []
        for o in offers:
            key = f"arb:{o.listing_id}"
            if not await was_offer_sent_recently(chat_id, key):
                new_offers.append(o)
        if not new_offers:
            return 0

        for chunk in _format_arb_chunks(new_offers):
            await bot.send_message(
                chat_id=chat_id, text=chunk, parse_mode="HTML", disable_web_page_preview=True
            )
        for o in new_offers:
            await mark_offer_sent(chat_id, f"arb:{o.listing_id}")
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
    sticker_items = await get_watchlist(chat_id)
    float_items = await get_float_watchlist(chat_id)
    text = (
        f"▶️ Автоскан возобновлён: вотчлист ({len(sticker_items)} шт.) + охота за флоатом "
        f"({len(float_items)} шт.), пауза {interval:g} мин между прогонами."
    )
    cooldown = steam_cooldown_remaining()
    if cooldown > 0:
        text += f"\n\n⚠️ Но Steam сейчас на кулдауне после 429 — первые {cooldown / 60:.0f} мин прогоны будут пропускаться."
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
                f"{ARB_PAGES_PER_SCAN * 50} свежих лотов за прогон.\n\n"
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
    await update.message.reply_text(
        f"✅ Кулдаун CSFloat сброшен ({was}).\n"
        f"Ключ: {csfloat_client.key_fingerprint()}\n"
        f"User-Agent: {csfloat_client.CSFLOAT_USER_AGENT}\n"
        f"Маршрут: {csfloat_client.route_description()}\n\n"
        "Проверить прямо сейчас: /arbnow"
    )


async def scanall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/scanall — сканировать весь вотчлист прямо сейчас, не дожидаясь расписания."""
    chat_id = update.effective_chat.id
    sticker_items = await get_watchlist(chat_id)
    float_items = await get_float_watchlist(chat_id)
    items = set(sticker_items) | set(float_items)
    if not items:
        await update.message.reply_text(
            "Оба списка пусты. Добавь предметы: /watchadd <предмет1>, <предмет2>, ... "
            "(охота по стикерам) или /floatadd <предмет> (охота за флоатом)."
        )
        return
    if chat_id in _watchlist_running:
        await update.message.reply_text("Скан вотчлиста уже идёт, дождись его окончания.")
        return
    cooldown = steam_cooldown_remaining()
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
    found_any = await _run_watchlist_scan(context.bot, chat_id)
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    def_min, def_max = await _get_defaults(chat_id)
    await update.message.reply_text(
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


BOT_COMMANDS = [
    BotCommand("start", "Помощь и список команд"),
    BotCommand("scan", "Проверить предмет (автоматический запрос к Steam)"),
    BotCommand("scanfile", "Проверить предмет вручную (резерв, если /scan не удался)"),
    BotCommand("watchadd", "Добавить предмет(ы) в вотчлист"),
    BotCommand("watchdel", "Убрать предмет из вотчлиста"),
    BotCommand("watchclear", "Полностью очистить вотчлист"),
    BotCommand("floatadd", "Добавить предмет в охоту за флоатом"),
    BotCommand("floatlist", "Показать список охоты за флоатом"),
    BotCommand("floatdel", "Убрать предмет из охоты за флоатом"),
    BotCommand("floatclear", "Очистить список охоты за флоатом"),
    BotCommand("watchlist", "Показать вотчлист и интервал автоскана"),
    BotCommand("watchpause", "Остановить автоскан вотчлиста"),
    BotCommand("watchresume", "Возобновить автоскан вотчлиста"),
    BotCommand("scanall", "Сканировать весь вотчлист прямо сейчас"),
    BotCommand("setarb", "Арбитраж: CSFloat дешевле Steam на N%"),
    BotCommand("arbnow", "Проверить арбитраж прямо сейчас"),
    BotCommand("arbreset", "Снять кулдаун CSFloat"),
    BotCommand("setarbprice", "Арбитраж: диапазон цены лота"),
    BotCommand("setarbvolume", "Арбитраж: фильтр ликвидности"),
    BotCommand("setarbstickers", "Арбитраж: наклейки почти даром"),
    BotCommand("setdefaults", "Настроить мин. стоимость стикеров и наценку"),
    BotCommand("setstreakmarkup", "Наценка для стрик-лотов (4-5 одинаковых стикеров)"),
    BotCommand("setpricefilter", "Фильтр по итоговой цене лота"),
    BotCommand("setfloatfilter", "Искать лоты с редким флоатом"),
    BotCommand("setfloatmarkup", "Макс. наценка для флоат-находок"),
    BotCommand("pricefile", "Загрузить свои цены на стикеры файлом"),
    BotCommand("clearprices", "Очистить загруженные цены на стикеры"),
]


async def _on_startup(app: Application):
    await app.bot.set_my_commands(BOT_COMMANDS)

    # ДО всего остального, что может тронуть Steam (prewarm, автоскан) —
    # восстанавливаем кулдаун после 429, если рестарт застал его активным.
    # Без этого бот "забывал" бы про ещё не снятый бан на каждом рестарте
    # процесса и тут же пробовал снова, продлевая реальный бан.
    await load_persisted_cooldown()
    await csfloat_client.load_persisted_cooldown()

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


class _HealthHandler(BaseHTTPRequestHandler):
    """Отвечает 200 OK на любой GET — этого достаточно UptimeRobot'у, чтобы считать сервис живым."""

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def do_HEAD(self):
        # Без этого HEAD-запросы (от UptimeRobot/прокси Render) ловят 501
        # Not Implemented из BaseHTTPRequestHandler по умолчанию, хотя бот жив.
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass  # не засоряем логи health-чеками от UptimeRobot


def _start_health_server():
    """
    Render (Web Service) ждёт, что процесс слушает $PORT — без этого он решит,
    что деплой не удался, и будет перезапускать контейнер. Плюс сюда же будет
    стучаться UptimeRobot, чтобы бесплатный сервис не засыпал по бездействию.
    Работает в отдельном потоке, чтобы не мешать asyncio-циклу python-telegram-bot.
    """
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("health-check сервер слушает порт %d", port)


def main():
    token = os.environ["TG_BOT_TOKEN"]
    _start_health_server()
    app = Application.builder().token(token).post_init(_on_startup).build()
    app.add_handler(CommandHandler("start", start))
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
    app.add_handler(CommandHandler("arbreset", arbreset))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_selection))
    app.run_polling()


if __name__ == "__main__":
    main()
