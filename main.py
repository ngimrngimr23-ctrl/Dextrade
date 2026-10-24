"""
Запуск бота командой `python main.py`.

Настоящая точка входа — bot.py: так записано в Procfile и render.yaml
(startCommand: python bot.py). Но у сервиса, заведённого в Render вручную,
команда запуска может быть `python main.py` — 2026-10-24 такой деплой падал
с «can't open file 'main.py'», хотя сборка проходила. С этим файлом рабочие
обе команды.
"""

from bot import main

if __name__ == "__main__":
    main()
