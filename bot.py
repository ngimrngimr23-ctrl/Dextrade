"""
Telegram-бот.

Использование в чате с ботом:
    /scan <ссылка на предмет на Steam Market> [мин_стоимость_стикеров] [макс_наценка_%]

Пример:
    /scan https://steamcommunity.com/market/listings/730/AK-47%20%7C%20Slate%20%28Field-Tested%29 5 7

Если Steam блокирует запросы бота напрямую (частая история для облачных
IP типа Render) — есть запасной путь через ручную передачу JSON:

    /scanfile <ссылка на предмет> [мин$] [макс%]

Бот пришлёт ссылку на страницу JSON — открываете её в своём браузере
(с домашнего IP Steam не блокирует), сохраняете как .json (Ctrl+S) и
присылаете файл боту. Он спарсит, попросит следующую страницу, если
лотов больше 100, и в конце сам посчитает офферы — как /scan.

Если не указать числа — по умолчанию 5 баксов и 7%.

Запуск:
    export TG_BOT_TOKEN=твой_токен_от_BotFather
    pip install -r requirements.txt
    python bot.py
"""

import html as html_module
import json
import logging
import os
import re
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from steam_client import (
    fetch_all_listings,
    market_hash_name_from_url,
    render_url,
    _parse_listings_html,
    RENDER_COUNT,
    STEAM_PROXY_URL,
)
from csgo_api import search_items as search_csgo_items
from pricing import get_sticker_prices, ingest_manual_prices, clear_manual_prices, manual_prices_count
from analyzer import find_offers
from prewarm import prewarm_loop
from storage import get_chat_defaults, set_chat_defaults

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

# Ожидание выбора варианта после неоднозначного поиска по названию (несколько
# степеней износа и т.п.) — chat_id -> {"results": [...], "min_value":..., "max_markup":...}
_pending_search: dict[int, dict] = {}

# chat_id -> True, пока активен режим "жду прайс-лист стикеров" (включается
# командой /pricefile). Следующие документы от этого chat_id идут в
# ingest_manual_prices, а не в обычный парсинг листингов, пока не пришлют
# /scan или /scanfile (это выключает режим автоматически).
_pricefile_mode: set[int] = set()


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
                f"Если Steam блокирует запросы бота (частая история для облачных IP) — "
                f"попробуй /scanfile вместо /scan, там ты сам качаешь JSON из браузера."
            )
        return
    await _run_analysis(update, listings, min_value, max_markup, market_hash_name)


async def _proceed_scanfile(update: Update, market_hash_name: str, min_value: float, max_markup: float):
    if STEAM_PROXY_URL:
        # Прокси настроен (Cloudflare Worker) — Steam банит только прямые
        # запросы с IP Render, через воркер это не нужно, так что просто
        # качаем сами, как /scan, без ожидания ручного файла от пользователя.
        await _proceed_scan(update, market_hash_name, min_value, max_markup)
        return

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
    await update.message.reply_text(
        f"Ок, собираю «{market_hash_name}» по файлам.\n\n"
        f"1. Открой в браузере:\n{first_url}\n"
        f"2. Сохрани страницу как .json (Ctrl+S) или PDF\n"
        f"3. Пришли файл сюда — результат посчитаю сразу же, "
        f"без ожидания остальных страниц. Можно прислать ещё страницы позже, "
        f"когда будет время — офферы каждый раз пересчитаются заново."
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


async def _run_analysis(
    update: Update, listings, min_value: float, max_markup: float, market_hash_name: str | None = None
):
    await update.message.reply_text(f"Собрано {len(listings)} лотов. Смотрю цены на стикеры…")

    all_sticker_keys = {s for l in listings for s in l.stickers}
    sticker_prices = await get_sticker_prices(all_sticker_keys) if all_sticker_keys else {}

    offers = find_offers(listings, sticker_prices, min_value, max_markup)

    if not offers:
        await update.message.reply_text(
            f"Ничего не подошло под критерии (стикеры от ${min_value:.2f}, наценка ≤{max_markup:.0f}%)."
        )
        return

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
        stickers_html = html_module.escape(", ".join(o.stickers))
        block = (
            f"${o.price:.2f} | переплата над голым скином ${o.overpay:.2f} | "
            f"стикеры ≈${o.stickers_value:.2f} | наценка {o.markup_pct:.1f}%\n"
            f"  <code>{stickers_html}</code>"
        )
        if o.inspect_link:
            block += f'\n  <a href="{html_module.escape(o.inspect_link)}">Инспект этого лота</a>'
        lines.append(block)

    # Telegram режет сообщения по ~4096 символов — шлём частями, если офферов много
    chunk = ""
    for line in lines:
        candidate = (chunk + "\n\n" + line) if chunk else line
        if len(candidate) > 3800:
            await update.message.reply_text(chunk, parse_mode="HTML", disable_web_page_preview=True)
            chunk = line
        else:
            chunk = candidate
    if chunk:
        await update.message.reply_text(chunk, parse_mode="HTML", disable_web_page_preview=True)


async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def scanfile(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await update.message.reply_text(
            f"Собрано {got} из {total_count}. Когда будет время — вот следующая страница:\n{next_url}\n"
            f"(необязательно сразу — можно прислать в любой момент)."
        )
    else:
        await update.message.reply_text(
            f"Собрано {got} из {total_count}, но не смог понять название предмета из файла, "
            f"чтобы дать ссылку на следующую страницу. Начни через /scanfile <ссылка>."
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    def_min, def_max = await _get_defaults(chat_id)
    if STEAM_PROXY_URL:
        scanfile_line = "/scanfile — то же самое, но всегда через прокси-воркер (на случай, если что-то не так с обычным /scan).\n\n"
    else:
        scanfile_line = (
            "Если Steam блокирует бота напрямую — есть /scanfile (ручная передача JSON-файлов), "
            "можно также просто прислать файл без команды.\n\n"
        )
    await update.message.reply_text(
        "Привет! Пришли:\n"
        "/scan <ссылка или название предмета> [мин$ стикеров] [макс наценка%]\n"
        "Название — на английском: /scan AK-47 | Slate\n\n"
        + scanfile_line +
        "/pricefile — загрузить прайс-лист цен на стикеры вручную (Steam market/search JSON), "
        "/clearprices — очистить его перед обновлением.\n\n"
        f"/setdefaults <мин$> <макс%> — поменять значения по умолчанию "
        f"(сейчас: {def_min:.0f}$ / {def_max:.0f}%)."
    )


async def _on_startup(app: Application):
    import asyncio
    asyncio.create_task(prewarm_loop())


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
    app.add_handler(CommandHandler("pricefile", pricefile))
    app.add_handler(CommandHandler("clearprices", clearprices))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_selection))
    app.run_polling()


if __name__ == "__main__":
    main()
