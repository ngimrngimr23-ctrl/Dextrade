"""
Telegram-бот.

Использование в чате с ботом:
    /scan <ссылка на предмет на Steam Market> [мин_стоимость_стикеров] [макс_наценка_%]

Пример:
    /scan https://steamcommunity.com/market/listings/730/AK-47%20%7C%20Slate%20%28Field-Tested%29 5 7

Если не указать числа — по умолчанию 5 баксов и 7%.

Запуск:
    export TG_BOT_TOKEN=твой_токен_от_BotFather
    pip install -r requirements.txt
    python bot.py
"""

import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from steam_client import fetch_all_listings, market_hash_name_from_url
from pricing import get_sticker_prices
from analyzer import find_offers
from prewarm import prewarm_loop

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("steam_bot")


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
        await update.message.reply_text(f"Не смог получить листинги: {e}")
        return

    await update.message.reply_text(f"Получено {len(listings)} лотов. Смотрю цены на стикеры…")

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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Пришли:\n/scan <ссылка на предмет Steam Market> [мин$ стикеров] [макс наценка%]"
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
    app.run_polling()


if __name__ == "__main__":
    main()
