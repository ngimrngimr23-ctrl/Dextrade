"""
Чтение числовых настроек из окружения так, чтобы бот от них не падал.

Зачем понадобилось. 2026-09-08 деплой лёг с этим:

    logship.py: INTERVAL_MINUTES = float(os.environ.get("LOG_SHIP_MINUTES", "30"))
    ValueError: could not convert string to float: ''

Переменную не удалили, а ОЧИСТИЛИ — в дашборде Render это делается одним
движением и выглядит как удаление. Но пустая строка это существующее
значение: os.environ.get отдаёт '', а не умолчание, и float('') падает.
Падает при импорте модуля, то есть процесс не стартует вовсе, а Render
крутит рестарты и откатывается на старую версию.

Мест с таким разбором было тридцать восемь, и каждое — та же мина: любая
случайно опустошённая переменная убивает бота целиком. Причём диагноз по
самому боту не поставить, потому что он не запускается и в лог ничего не
пишет; выяснять пришлось по логам сборки Render.

Поэтому правило простое: НАСТРОЙКА НЕ ИМЕЕТ ПРАВА РОНЯТЬ ПРОЦЕСС. Пустое или
неразборчивое значение — это «человек не задал», и работает умолчание. О
подмене говорим в лог: молча игнорировать чужой ввод тоже нельзя, иначе
опечатка в числе будет неделю выглядеть как «настройка не применяется».
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger("steam_bot.env")


def _raw(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None          # пустая строка = не задано


def env_float(name: str, default: float) -> float:
    raw = _raw(name)
    if raw is None:
        return default
    try:
        return float(raw.replace(",", "."))
    except ValueError:
        log.warning(
            "%s=%r — не число, беру умолчание %s. Настройка не должна ронять "
            "процесс, поэтому просто игнорирую значение.", name, raw, default,
        )
        return default


def env_int(name: str, default: int) -> int:
    raw = _raw(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        # Целое из «10.0» достаём намеренно: человек пишет число, а не тип.
        try:
            return int(float(raw.replace(",", ".")))
        except ValueError:
            log.warning(
                "%s=%r — не целое, беру умолчание %s.", name, raw, default,
            )
            return default
