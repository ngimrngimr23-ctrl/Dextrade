"""
Хранилище цен стикеров: Upstash Redis через REST API как основное
хранилище, локальный JSON-файл — fallback, если Redis не настроен
или временно недоступен. Тот же паттерн, что уже используется в
GiftSatteliteAdapter (Upstash + локальный fallback).

На Render (без платного постоянного диска) файловая система эфемерна и
обнуляется при каждом деплое/рестарте — поэтому цены стикеров нужно
держать во внешнем хранилище (Upstash free tier с запасом хватает под
такой объём: пара тысяч ключей-строк).

Настройка (переменные окружения на Render):
    UPSTASH_REDIS_REST_URL="https://xxx.upstash.io"
    UPSTASH_REDIS_REST_TOKEN="..."

Если их нет — бот не падает, просто работает через локальный JSON-файл
(годится для локального запуска/разработки, но на Render не переживёт
редеплой).
"""

import json
import os
import time
from pathlib import Path
from typing import Optional

import aiohttp

REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")
REDIS_ENABLED = bool(REDIS_URL and REDIS_TOKEN)

LOCAL_FALLBACK_PATH = Path(__file__).parent / "sticker_prices_local.json"
KEY_PREFIX = "stickerprice:"
INDEX_KEY = "stickerprice_index"  # Redis SET со всеми известными ключами — нужен для prewarm


def _local_load() -> dict:
    if LOCAL_FALLBACK_PATH.exists():
        try:
            return json.loads(LOCAL_FALLBACK_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _local_save(data: dict) -> None:
    LOCAL_FALLBACK_PATH.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


async def _redis_cmd(*args):
    """Одна команда Upstash REST. Бросает исключение при ошибке — вызывающий код ловит и падает на fallback."""
    async with aiohttp.ClientSession() as session:
        async with session.post(
            REDIS_URL,
            headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
            json=list(args),
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data = await resp.json()
            if "error" in data:
                raise RuntimeError(data["error"])
            return data.get("result")


async def get_price(key: str) -> Optional[dict]:
    """Возвращает {'matched_name':..., 'price':..., 'updated_at':...} или None, если записи нет."""
    if REDIS_ENABLED:
        try:
            raw = await _redis_cmd("GET", KEY_PREFIX + key)
            return json.loads(raw) if raw else None
        except Exception:
            pass  # Upstash недоступен — работаем через локальный файл как fallback

    return _local_load().get(key)


async def set_price(key: str, matched_name: Optional[str], price: float, ttl_seconds: int) -> None:
    entry = {"matched_name": matched_name, "price": price, "updated_at": time.time()}
    value = json.dumps(entry, ensure_ascii=False)

    if REDIS_ENABLED:
        try:
            await _redis_cmd("SET", KEY_PREFIX + key, value, "EX", str(ttl_seconds))
            await _redis_cmd("SADD", INDEX_KEY, key)
            return
        except Exception:
            pass  # тоже падаем на локальный файл, чтобы данные не потерялись

    data = _local_load()
    data[key] = entry
    _local_save(data)


async def _redis_pipeline(commands: list[list]) -> list:
    """Несколько команд Upstash REST одним HTTP-запросом. Возвращает список {'result':...}/{'error':...} по порядку команд."""
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{REDIS_URL}/pipeline",
            headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
            json=commands,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            return await resp.json()


async def get_prices_batch(keys: list[str]) -> dict[str, Optional[dict]]:
    """
    То же самое, что get_price(), но сразу для нескольких ключей одним
    запросом (Redis MGET) вместо отдельного HTTP-соединения на каждый —
    на десятках стикеров с одного лота такое батчирование убирает
    заметную паузу при проверке кэша.
    """
    if not keys:
        return {}

    if REDIS_ENABLED:
        try:
            full_keys = [KEY_PREFIX + k for k in keys]
            raws = await _redis_cmd("MGET", *full_keys)
            return {key: (json.loads(raw) if raw else None) for key, raw in zip(keys, raws)}
        except Exception:
            pass  # Upstash недоступен — работаем через локальный файл как fallback

    local = _local_load()
    return {key: local.get(key) for key in keys}


async def set_prices_batch(entries: list[tuple[str, Optional[str], float, int]]) -> None:
    """
    То же самое, что set_price(), но сразу для нескольких ключей одним
    pipeline-запросом вместо SET+SADD по одному на каждый ключ.
    entries: (key, matched_name, price, ttl_seconds).
    """
    if not entries:
        return

    if REDIS_ENABLED:
        try:
            commands = []
            for key, matched_name, price, ttl_seconds in entries:
                value = json.dumps(
                    {"matched_name": matched_name, "price": price, "updated_at": time.time()},
                    ensure_ascii=False,
                )
                commands.append(["SET", KEY_PREFIX + key, value, "EX", str(ttl_seconds)])
                commands.append(["SADD", INDEX_KEY, key])
            results = await _redis_pipeline(commands)
            if any(isinstance(r, dict) and "error" in r for r in results):
                raise RuntimeError(f"pipeline вернул ошибку хотя бы по одной команде: {results}")
            return
        except Exception:
            pass  # тоже падаем на локальный файл, чтобы данные не потерялись

    data = _local_load()
    for key, matched_name, price, ttl_seconds in entries:
        data[key] = {"matched_name": matched_name, "price": price, "updated_at": time.time()}
    _local_save(data)


async def all_known_keys() -> list[str]:
    """Все ключи стикеров, которые бот когда-либо оценивал — нужно для фонового prewarm."""
    if REDIS_ENABLED:
        try:
            result = await _redis_cmd("SMEMBERS", INDEX_KEY)
            return result or []
        except Exception:
            pass

    return list(_local_load().keys())


# ---------------------------------------------------------------------------
# Дефолты /setdefaults по chat_id. Раньше жили только в памяти процесса и
# слетали на каждом рестарте/редеплое бота — теперь через то же хранилище,
# что и цены стикеров, переживают рестарты.
# ---------------------------------------------------------------------------

DEFAULTS_KEY_PREFIX = "chatdefaults:"
LOCAL_DEFAULTS_PATH = Path(__file__).parent / "chat_defaults_local.json"


def _local_defaults_load() -> dict:
    if LOCAL_DEFAULTS_PATH.exists():
        try:
            return json.loads(LOCAL_DEFAULTS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _local_defaults_save(data: dict) -> None:
    LOCAL_DEFAULTS_PATH.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


async def get_chat_defaults(chat_id: int) -> Optional[dict]:
    """Возвращает {'min_value':..., 'max_markup':...} или None, если не задавались."""
    if REDIS_ENABLED:
        try:
            raw = await _redis_cmd("GET", DEFAULTS_KEY_PREFIX + str(chat_id))
            return json.loads(raw) if raw else None
        except Exception:
            pass

    return _local_defaults_load().get(str(chat_id))


async def set_chat_defaults(chat_id: int, min_value: float, max_markup: float) -> None:
    entry = {"min_value": min_value, "max_markup": max_markup}
    value = json.dumps(entry, ensure_ascii=False)

    if REDIS_ENABLED:
        try:
            # без TTL — это осознанная настройка пользователя, не кэш
            await _redis_cmd("SET", DEFAULTS_KEY_PREFIX + str(chat_id), value)
            return
        except Exception:
            pass

    data = _local_defaults_load()
    data[str(chat_id)] = entry
    _local_defaults_save(data)
    
