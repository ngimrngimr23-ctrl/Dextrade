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

import json
import logging
import os
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
)
from pricing import get_sticker_prices
from analyzer import find_offers
from prewarm import prewarm_loop

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("steam_bot")

# Состояние активных /scanfile-сессий по chat_id. В памяти процесса —
# сессия живёт, пока бот не перезапустится; для одного скана этого достаточно,
# долговременно ничего хранить тут не нужно.
_file_sessions: dict[int, dict] = {}


async def _run_analysis(update: Update, listings, min_value: float, max_markup: float):
    await update.message.reply_text(f"Собрано {len(listings)} лотов. Смотрю цены на стикеры…")

    all_sticker_keys = {s for l in listings for s in l.stickers}
    sticker_prices = await get_sticker_prices(all_sticker_keys) if all_sticker_keys else {}

    offers = find_offers(listings, sticker_prices, min_value, max_markup)

    if not offers:
        await update.message.reply_text(
            f"Ничего не подошло под критерии (стикеры от ${min_value:.2f}, наценка ≤{max_markup:.0f}%)."
        )
        return

    lines = [f"Найдено {len(offers)} офферов:\n"]
    for o in offers[:20]:
        lines.append(
            f"${o.price:.2f} | стикеры ≈${o.stickers_value:.2f} | наценка {o.markup_pct:.1f}%\n"
            f"  {', '.join(o.stickers)}"
        )
    await update.message.reply_text("\n\n".join(lines))


async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(
            "Формат: /scan <ссылка на предмет Steam Market> [мин$ стикеров=5] [макс наценка%=7]"
        )
        return

    url = args[0]
    min_value = float(args[1]) if len(args) > 1 else 5.0
    max_markup = float(args[2]) if len(args) > 2 else 7.0

    try:
        market_hash_name = market_hash_name_from_url(url)
    except Exception:
        await update.message.reply_text("Не смог разобрать ссылку. Дай ссылку вида .../market/listings/730/<предмет>")
        return

    await update.message.reply_text(f"Тяну лоты по «{market_hash_name}»… это может занять пару минут.")

    try:
        listings = await fetch_all_listings(market_hash_name)
    except Exception as e:
        log.exception("fetch_all_listings failed")
        await update.message.reply_text(
            f"Не смог получить листинги: {e}\n\n"
            f"Если Steam блокирует запросы бота (частая история для облачных IP) — "
            f"попробуй /scanfile вместо /scan, там ты сам качаешь JSON из браузера."
        )
        return

    await _run_analysis(update, listings, min_value, max_markup)


async def scanfile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(
            "Формат: /scanfile <ссылка на предмет Steam Market> [мин$ стикеров=5] [макс наценка%=7]"
        )
        return

    url = args[0]
    min_value = float(args[1]) if len(args) > 1 else 5.0
    max_markup = float(args[2]) if len(args) > 2 else 7.0

    try:
        market_hash_name = market_hash_name_from_url(url)
    except Exception:
        await update.message.reply_text("Не смог разобрать ссылку. Дай ссылку вида .../market/listings/730/<предмет>")
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
        f"2. Сохрани страницу как .json (Ctrl+S)\n"
        f"3. Пришли файл сюда"
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    session = _file_sessions.get(chat_id)
    if not session:
        return  # файл без активной /scanfile-сессии — просто игнорируем

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

    total_count = data.get("total_count", 0)
    html = data.get("results_html", "")
    new_listings = _parse_listings_html(html)
    session["listings"].extend(new_listings)
    session["total_count"] = total_count
    session["next_start"] += RENDER_COUNT

    got = len(session["listings"])
    if session["next_start"] < total_count:
        next_url = render_url(session["market_hash_name"], session["next_start"])
        await update.message.reply_text(
            f"Принято, всего собрано {got} из {total_count} лотов.\n\n"
            f"Дальше:\n{next_url}\n"
            f"Сохрани и пришли так же."
        )
        return

    # все страницы собраны — считаем офферы и закрываем сессию
    listings = session["listings"]
    min_value = session["min_value"]
    max_markup = session["max_markup"]
    del _file_sessions[chat_id]

    await update.message.reply_text(f"Все лоты собраны ({got} из {total_count}).")
    await _run_analysis(update, listings, min_value, max_markup)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Пришли:\n"
        "/scan <ссылка на предмет Steam Market> [мин$ стикеров] [макс наценка%]\n\n"
        "Если Steam блокирует бота напрямую — есть /scanfile (ручная передача JSON-файлов)."
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
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.run_polling()


if __name__ == "__main__":
    main()
        
