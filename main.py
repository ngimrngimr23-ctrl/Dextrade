import asyncio
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, CommandObject
from aiohttp import web
import os

# Берем токен из скрытых настроек сервера (не пишем его прямо в коде!)
BOT_TOKEN = os.getenv("BOT_TOKEN")
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Настройки по умолчанию
settings = {"percent": 5.0, "time": 15, "chat_id": None}
# Хранилище цен: {"BTCUSDT": [цена_15_мин_назад, цена_сейчас]} 
price_history = {}

# --- Команды Telegram ---

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    settings["chat_id"] = message.chat.id
    await message.answer(
        "🚀 Парсер запущен!\n"
        f"Текущие настройки: Падение {settings['percent']}% за {settings['time']} мин.\n\n"
        "Команды:\n"
        "/p [число] — изменить процент (напр. /p 10)\n"
        "/t [число] — изменить время в минутах (напр. /t 5)\n"
        "/s — статус"
    )

@dp.message(Command("p"))
async def set_percent(message: types.Message, command: CommandObject):
    if command.args:
        settings["percent"] = float(command.args.replace(',', '.'))
        await message.answer(f"✅ Процент падения установлен на {settings['percent']}%")
    else:
        await message.answer("Введи число. Пример: /p 7.5")

@dp.message(Command("t"))
async def set_time(message: types.Message, command: CommandObject):
    if command.args and command.args.isdigit():
        settings["time"] = int(command.args)
        await message.answer(f"✅ Время проверки установлено на {settings['time']} минут")
    else:
        await message.answer("Введи целое число минут. Пример: /t 10")

@dp.message(Command("s"))
async def status_cmd(message: types.Message):
    await message.answer(f"📊 Статус: Ищем падения от {settings['percent']}% за последние {settings['time']} мин.")

# --- Логика Парсера Dex-Trade ---

async def fetch_prices():
    # Используем публичный API Dex-Trade для получения всех тикеров
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
                if pair in price_history:
                    old_price = price_history[pair]
                    if old_price > 0:
                        # Считаем процент падения
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
                
                # Обновляем историю цен
                price_history[pair] = current_price
                
        # Ждем указанное время перед следующей проверкой
        await asyncio.sleep(settings["time"] * 60)

# --- Настройка веб-сервера (заглушка для бесплатного хостинга) ---

async def handle_ping(request):
    return web.Response(text="Bot is running!")

async def main():
    # Запускаем парсер в фоне
    asyncio.create_task(parser_task())
    
    # Настраиваем веб-сервер
    app = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Порт берется из хостинга (Render), по умолчанию 8080
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

    # Запускаем бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
