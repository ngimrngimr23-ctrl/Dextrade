import asyncio
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, CommandObject
from aiohttp import web
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

settings = {"percent": 5.0, "time": 15, "chat_id": None}
price_history = {}
blacklist = set()  # Хранилище для черного списка

# --- Команды Telegram ---

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    settings["chat_id"] = message.chat.id
    await message.answer(
        "🚀 Парсер запущен!\n"
        f"Настройки: Падение {settings['percent']}% за {settings['time']} мин.\n\n"
        "Команды:\n"
        "/p [число] — изменить процент (напр. /p 10)\n"
        "/t [число] — время в минутах (напр. /t 5)\n"
        "/b [монета] — добавить в ЧС (напр. /b BTCUSDT)\n"
        "/ub [монета] — убрать из ЧС (напр. /ub BTCUSDT)\n"
        "/s — статус и список ЧС"
    )

@dp.message(Command("p"))
async def set_percent(message: types.Message, command: CommandObject):
    if command.args:
        settings["percent"] = float(command.args.replace(',', '.'))
        await message.answer(f"✅ Порог падения: {settings['percent']}%")

@dp.message(Command("t"))
async def set_time(message: types.Message, command: CommandObject):
    if command.args and command.args.isdigit():
        settings["time"] = int(command.args)
        await message.answer(f"✅ Время проверки: {settings['time']} мин.")

@dp.message(Command("b"))
async def add_blacklist(message: types.Message, command: CommandObject):
    if command.args:
        coin = command.args.strip().upper()
        blacklist.add(coin)
        await message.answer(f"🚫 Монета {coin} добавлена в черный список.")
    else:
        await message.answer("Укажи тикер. Пример: /b BTCUSDT")

@dp.message(Command("ub"))
async def remove_blacklist(message: types.Message, command: CommandObject):
    if command.args:
        coin = command.args.strip().upper()
        if coin in blacklist:
            blacklist.remove(coin)
            await message.answer(f"✅ Монета {coin} удалена из черного списка.")
        else:
            await message.answer(f"Монеты {coin} нет в ЧС.")

@dp.message(Command("s"))
async def status_cmd(message: types.Message):
    bl_text = ", ".join(blacklist) if blacklist else "пусто"
    await message.answer(
        f"📊 Статус: Падение от {settings['percent']}% за {settings['time']} мин.\n"
        f"🚫 В черном списке: {bl_text}"
    )

# --- Логика Парсера ---

async def fetch_prices():
    url = "https://api.dex-trade.com/v1/public/tickers"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return {item['pair']: float(item['last']) for item in data['data']}
    except Exception as e:
        print(f"Ошибка API: {e}")
    return {}

async def parser_task():
    while True:
        if settings["chat_id"]:
            current_prices = await fetch_prices()
            
            for pair, current_price in current_prices.items():
                # Пропускаем монеты из черного списка
                if pair in blacklist:
                    continue
                    
                if pair in price_history:
                    old_price = price_history[pair]
                    if old_price > 0:
                        drop = ((old_price - current_price) / old_price) * 100
                        if drop >= settings["percent"]:
                            await bot.send_message(
                                settings["chat_id"], 
                                f"🚨 **ДАМП: {pair}**\n"
                                f"Упала на {drop:.2f}%\n"
                                f"Цена была: {old_price}\n"
                                f"Цена сейчас: {current_price}\n"
                                f"Интервал: {settings['time']} мин."
                            )
                
                price_history[pair] = current_price
                
        await asyncio.sleep(settings["time"] * 60)

# --- Настройка веб-сервера ---

async def handle_ping(request):
    return web.Response(text="Bot is running!")

async def main():
    asyncio.create_task(parser_task())
    app = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
    
