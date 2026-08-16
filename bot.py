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
from analyzer import find_offers, find_float_offers, Offer, STREAK_THRESHOLD
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
    get_watchlist,
    set_watchlist,
    get_watch_paused,
    set_watch_paused,
    all_watchlist_chat_ids,
    was_offer_sent_recently,
    mark_offer_sent,
)

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

# /setfloatfilter: сколько самых дешёвых лотов на предмет проверяем на флоат.
# Steam всегда отдаёт лоты отсортированными от дешёвых к дорогим, так что
# "недооценённый редкий флоат" (продавец не в курсе, что он особенный) скорее
# всего среди дешёвых — проверять все 100 лотов на каждый предмет ради этого
# смысла нет, а вот стоимость (лишние запросы) была бы ощутимой.
FLOAT_CHECK_TOP_N = 25

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
    sample_legacy = None
    for listing in to_check:
        info, reason = decode_inspect_link(listing.inspect_link)
        reasons[reason] = reasons.get(reason, 0) + 1
        if info is not None:
            result[listing.inspect_link] = info["floatvalue"]
        elif reason == "legacy_link" and sample_legacy is None:
            sample_legacy = listing.inspect_link[:120]

    log.info(
        "cs_inspect: раскодировано %d из %d inspect-ссылок (по причинам: %s)",
        len(result), len(to_check), reasons,
    )
    if sample_legacy and not result:
        # ни одной не раскодировали — показываем образец, чтобы в следующем
        # прогоне сразу было видно, в каком виде реально приходят ссылки
        log.info("cs_inspect: пример нераспознанной ссылки: %s", sample_legacy)
    return result


async def _compute_offers(chat_id: int, listings, min_value: float, max_markup: float):
    """Общая логика для /scan, /scanfile и автоскана вотчлиста: цены стикеров -> офферы."""
    all_sticker_keys = {s for l in listings for s in l.stickers}
    sticker_prices = await get_sticker_prices(all_sticker_keys) if all_sticker_keys else {}
    streak_markup = await get_streak_markup(chat_id)
    min_price, max_price = await get_price_filter(chat_id)
    float_low, float_high = await get_float_filter(chat_id)

    # Флоат для ОХОТЫ (фильтр) считаем только когда фильтр задан — там нужны
    # первые FLOAT_CHECK_TOP_N самых дешёвых лотов. А вот показать флоат на
    # уже отобранных по стикерам офферах можно всегда: декодирование локальное,
    # сетевых запросов не делает, и офферов обычно единицы.
    float_offers = []
    if float_low is not None and float_high is not None and listings:
        top_floats = _decode_floats(listings, limit=FLOAT_CHECK_TOP_N)
        if top_floats:
            float_offers = find_float_offers(listings, top_floats, float_low, float_high)

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
            # находка чисто по флоату — стикерная наценка тут не считалась вообще
            block = f"${o.price:.2f} | 🔍 редкий флоат {o.float_value:.5f}"
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
                f"Проверяются первые {FLOAT_CHECK_TOP_N} самых дешёвых лотов на предмет — там, где "
                "недооценённый редкий флоат вероятнее всего.\n"
                "/setfloatfilter off — убрать фильтр"
            )
        else:
            await update.message.reply_text(
                f"Текущий фильтр флоата: ≤{low:g} (топ для FN) или ≥{high:g} (топ для BS), "
                f"среди первых {FLOAT_CHECK_TOP_N} самых дешёвых лотов на предмет."
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
        f"Ок, теперь ищу лоты с флоатом ≤{low:g} или ≥{high:g} "
        f"среди первых {FLOAT_CHECK_TOP_N} самых дешёвых лотов на предмет."
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
    await update.message.reply_text("\n".join(lines))


def _offer_key(market_hash_name: str, offer: Offer) -> str:
    """
    Стабильный ключ конкретного лота — по inspect-ссылке (уникальна для
    каждого экземпляра предмета в Steam), либо, если её нет, по сочетанию
    название+цена+стикеры. Нужен, чтобы не слать один и тот же оффер
    повторно в течение SENT_OFFER_TTL_SECONDS (см. storage.py).
    """
    basis = offer.inspect_link or f"{market_hash_name}|{offer.price}|{','.join(offer.stickers)}"
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()


async def _watchlist_scan_item(bot, chat_id: int, market_hash_name: str, min_value: float, max_markup: float) -> bool:
    """
    Возвращает True, если нашлись НОВЫЕ офферы (не присылавшиеся этому чату
    за последние 5 часов) и сообщение реально ушло в чат.
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

    offers, sticker_prices = await _compute_offers(chat_id, listings, min_value, max_markup)
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

    items = await get_watchlist(chat_id)
    if not items:
        return None

    _watchlist_running.add(chat_id)
    try:
        min_value, max_markup = await _get_defaults(chat_id)
        found_any = False
        for market_hash_name in items:
            try:
                found = await _watchlist_scan_item(bot, chat_id, market_hash_name, min_value, max_markup)
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


async def watchpause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/watchpause — остановить автоскан вотчлиста (список при этом сохраняется)."""
    chat_id = update.effective_chat.id
    await set_watch_paused(chat_id, True)
    for job in context.application.job_queue.get_jobs_by_name(f"{WATCHLIST_JOB_PREFIX}{chat_id}"):
        job.schedule_removal()
    items = await get_watchlist(chat_id)
    await update.message.reply_text(
        f"⏸ Автоскан вотчлиста остановлен. Список ({len(items)} шт.) сохранён — "
        f"его можно смотреть /watchlist и чистить /watchdel.\n"
        f"Возобновить: /watchresume. Разовый скан вручную по-прежнему работает: /scanall."
    )


async def watchresume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/watchresume — снова включить автоскан вотчлиста по расписанию."""
    chat_id = update.effective_chat.id
    await set_watch_paused(chat_id, False)
    interval = await _get_watch_interval(chat_id)
    _schedule_watchlist_job(context.application.job_queue, chat_id, interval)
    items = await get_watchlist(chat_id)
    text = f"▶️ Автоскан вотчлиста возобновлён: {len(items)} предмет(ов), пауза {interval:g} мин между прогонами."
    cooldown = steam_cooldown_remaining()
    if cooldown > 0:
        text += f"\n\n⚠️ Но Steam сейчас на кулдауне после 429 — первые {cooldown / 60:.0f} мин прогоны будут пропускаться."
    await update.message.reply_text(text)


async def scanall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/scanall — сканировать весь вотчлист прямо сейчас, не дожидаясь расписания."""
    chat_id = update.effective_chat.id
    items = await get_watchlist(chat_id)
    if not items:
        await update.message.reply_text("Вотчлист пуст. Добавь предметы: /watchadd <предмет1>, <предмет2>, ...")
        return
    if chat_id in _watchlist_running:
        await update.message.reply_text("Скан вотчлиста уже идёт, дождись его окончания.")
        return
    cooldown = steam_cooldown_remaining()
    if cooldown > 0:
        await update.message.reply_text(
            f"Steam на кулдауне после 429 (это временный бан IP, который продлевается от новых "
            f"попыток) — ещё {cooldown / 60:.0f} мин. Попробуй после этого."
        )
        return

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
        "/scanall — сканировать весь вотчлист прямо сейчас, не дожидаясь расписания.\n"
        "/watchpause — остановить автоскан (список сохраняется), /watchresume — возобновить.\n\n"
        f"/setdefaults <мин$> <макс%> — поменять значения по умолчанию "
        f"(сейчас: {def_min:.0f}$ / {def_max:.0f}%).\n"
        f"/setstreakmarkup <%> — отдельный порог наценки для лотов с {STREAK_THRESHOLD}+ "
        f"подряд одинаковыми стикерами (\"стрик\").\n"
        f"/setpricefilter <мин$> <макс$> — показывать только лоты в этом диапазоне цены "
        f"(со стикерами); /setpricefilter off — убрать фильтр.\n"
        f"/setfloatfilter <низкий> <высокий> — искать лоты с редким флоатом (близко к 0 — топ "
        f"для Factory New, близко к 1 — топ для Battle-Scarred), не связано со стикерами; "
        f"/setfloatfilter off — убрать фильтр."
    )


BOT_COMMANDS = [
    BotCommand("start", "Помощь и список команд"),
    BotCommand("scan", "Проверить предмет (автоматический запрос к Steam)"),
    BotCommand("scanfile", "Проверить предмет вручную (резерв, если /scan не удался)"),
    BotCommand("watchadd", "Добавить предмет(ы) в вотчлист"),
    BotCommand("watchdel", "Убрать предмет из вотчлиста"),
    BotCommand("watchclear", "Полностью очистить вотчлист"),
    BotCommand("watchlist", "Показать вотчлист и интервал автоскана"),
    BotCommand("watchpause", "Остановить автоскан вотчлиста"),
    BotCommand("watchresume", "Возобновить автоскан вотчлиста"),
    BotCommand("scanall", "Сканировать весь вотчлист прямо сейчас"),
    BotCommand("setdefaults", "Настроить мин. стоимость стикеров и наценку"),
    BotCommand("setstreakmarkup", "Наценка для стрик-лотов (4-5 одинаковых стикеров)"),
    BotCommand("setpricefilter", "Фильтр по итоговой цене лота"),
    BotCommand("setfloatfilter", "Искать лоты с редким флоатом"),
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

    asyncio.create_task(prewarm_loop())

    # Восстанавливаем джобы автоскана вотчлиста после рестарта/редеплоя —
    # без этого расписание жило бы только в памяти процесса и слетало каждый раз.
    chat_ids = await all_watchlist_chat_ids()
    restored = 0
    for chat_id in chat_ids:
        items = await get_watchlist(chat_id)
        if not items:
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
    app.add_handler(CommandHandler("scan", scanfile))
    app.add_handler(CommandHandler("scanfile", scan))
    app.add_handler(CommandHandler("setdefaults", setdefaults))
    app.add_handler(CommandHandler("setstreakmarkup", setstreakmarkup))
    app.add_handler(CommandHandler("setpricefilter", setpricefilter))
    app.add_handler(CommandHandler("setfloatfilter", setfloatfilter))
    app.add_handler(CommandHandler("pricefile", pricefile))
    app.add_handler(CommandHandler("clearprices", clearprices))
    app.add_handler(CommandHandler("watchadd", watchadd))
    app.add_handler(CommandHandler("watchdel", watchdel))
    app.add_handler(CommandHandler("watchclear", watchclear))
    app.add_handler(CommandHandler("watchlist", watchlist_cmd))
    app.add_handler(CommandHandler("watchpause", watchpause))
    app.add_handler(CommandHandler("watchresume", watchresume))
    app.add_handler(CommandHandler("scanall", scanall))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_selection))
    app.run_polling()


if __name__ == "__main__":
    main()
