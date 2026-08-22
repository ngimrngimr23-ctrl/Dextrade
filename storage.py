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
import logging
from pathlib import Path
from typing import Optional

import aiohttp

log = logging.getLogger("steam_bot.storage")

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


async def _get_chat_settings(chat_id: int) -> dict:
    """
    Все настройки чата разом (мин$ стикеров, макс наценка, наценка для
    стрик-лотов, фильтр цены лота) — один JSON-блок на chat_id, чтобы не
    плодить отдельные Redis-ключи под каждую новую настройку.
    """
    if REDIS_ENABLED:
        try:
            raw = await _redis_cmd("GET", DEFAULTS_KEY_PREFIX + str(chat_id))
            return json.loads(raw) if raw else {}
        except Exception:
            pass

    return _local_defaults_load().get(str(chat_id), {})


async def _save_chat_settings(chat_id: int, settings: dict) -> None:
    value = json.dumps(settings, ensure_ascii=False)

    if REDIS_ENABLED:
        try:
            # без TTL — это осознанная настройка пользователя, не кэш
            await _redis_cmd("SET", DEFAULTS_KEY_PREFIX + str(chat_id), value)
            return
        except Exception:
            pass

    data = _local_defaults_load()
    data[str(chat_id)] = settings
    _local_defaults_save(data)


async def get_chat_defaults(chat_id: int) -> Optional[dict]:
    """Возвращает {'min_value':..., 'max_markup':...} (и что там ещё задано) или None, если /setdefaults ещё не вызывали."""
    settings = await _get_chat_settings(chat_id)
    return settings if "min_value" in settings else None


async def set_chat_defaults(chat_id: int, min_value: float, max_markup: float) -> None:
    settings = await _get_chat_settings(chat_id)
    settings["min_value"] = min_value
    settings["max_markup"] = max_markup
    await _save_chat_settings(chat_id, settings)


async def get_watch_gap(chat_id: int) -> Optional[float]:
    """
    Пауза в минутах между концом одного прогона автоскана и началом следующего.
    None — не задана, действует значение по умолчанию из кода.
    """
    return (await _get_chat_settings(chat_id)).get("watch_gap")


async def set_watch_gap(chat_id: int, minutes: Optional[float]) -> None:
    settings = await _get_chat_settings(chat_id)
    settings["watch_gap"] = minutes
    await _save_chat_settings(chat_id, settings)


async def get_sticker_ratio(chat_id: int) -> Optional[float]:
    """
    Во сколько раз сумма наклеек должна перекрывать цену голого скина.
    None — порог не задан, лоты по этому признаку не отсеиваются.
    """
    return (await _get_chat_settings(chat_id)).get("sticker_ratio")


async def set_sticker_ratio(chat_id: int, ratio: Optional[float]) -> None:
    settings = await _get_chat_settings(chat_id)
    settings["sticker_ratio"] = ratio
    await _save_chat_settings(chat_id, settings)


async def get_streak_markup(chat_id: int) -> Optional[float]:
    """Отдельный порог наценки (%) для стрик-лотов (4-5 подряд одинаковых стикеров), либо None, если не задан."""
    return (await _get_chat_settings(chat_id)).get("streak_markup")


async def set_streak_markup(chat_id: int, pct: float) -> None:
    settings = await _get_chat_settings(chat_id)
    settings["streak_markup"] = pct
    await _save_chat_settings(chat_id, settings)


async def get_price_filter(chat_id: int) -> tuple[Optional[float], Optional[float]]:
    """(мин, макс) цена лота (с учётом стикеров) — любой из них может быть None, если не задан."""
    settings = await _get_chat_settings(chat_id)
    return settings.get("min_price"), settings.get("max_price")


async def set_price_filter(chat_id: int, min_price: Optional[float], max_price: Optional[float]) -> None:
    settings = await _get_chat_settings(chat_id)
    settings["min_price"] = min_price
    settings["max_price"] = max_price
    await _save_chat_settings(chat_id, settings)


async def get_float_filter(chat_id: int) -> tuple[Optional[float], Optional[float]]:
    """
    (низкий_порог, высокий_порог) для охоты за редким флоатом — лот интересен,
    если флоат <= низкий_порог (топ для FN) ИЛИ >= высокий_порог (топ для BS).
    Оба None, если фильтр не задан (/setfloatfilter ещё не вызывали или выключен).
    """
    settings = await _get_chat_settings(chat_id)
    return settings.get("float_low_max"), settings.get("float_high_min")


async def set_float_filter(chat_id: int, low_max: Optional[float], high_min: Optional[float]) -> None:
    settings = await _get_chat_settings(chat_id)
    settings["float_low_max"] = low_max
    settings["float_high_min"] = high_min
    await _save_chat_settings(chat_id, settings)


async def get_float_markup(chat_id: int) -> Optional[float]:
    """
    Макс. наценка (%) над самым дешёвым лотом предмета, при которой флоат-находка
    (см. get_float_filter) всё ещё считается недооценённой, а не просто дорогим
    лотом с честной ценой за редкость. None — без ограничения по цене.
    """
    return (await _get_chat_settings(chat_id)).get("float_markup_pct")


async def set_float_markup(chat_id: int, pct: Optional[float]) -> None:
    settings = await _get_chat_settings(chat_id)
    settings["float_markup_pct"] = pct
    await _save_chat_settings(chat_id, settings)


# --- Кросс-маркет арбитраж (CSFloat против Steam) ---------------------------
# Все настройки в том же блоке настроек чата. По умолчанию всё None —
# то есть арбитраж выключен и бот ведёт себя ровно как раньше.

async def get_arb_settings(chat_id: int) -> dict:
    """
    {'min_discount': %, 'min_price': $, 'max_price': $, 'min_volume': шт,
     'sticker_markup': %} — любое поле может быть None.
    min_discount = None означает, что арбитраж выключен целиком.
    """
    s = await _get_chat_settings(chat_id)
    return {
        "min_discount": s.get("arb_min_discount"),
        "min_price": s.get("arb_min_price"),
        "max_price": s.get("arb_max_price"),
        "min_volume": s.get("arb_min_volume"),
        "sticker_markup": s.get("arb_sticker_markup"),
    }


async def set_arb_setting(chat_id: int, key: str, value) -> None:
    """key — одно из: min_discount, min_price, max_price, min_volume, sticker_markup."""
    allowed = {"min_discount", "min_price", "max_price", "min_volume", "sticker_markup"}
    if key not in allowed:
        raise ValueError(f"неизвестная настройка арбитража: {key}")
    settings = await _get_chat_settings(chat_id)
    settings[f"arb_{key}"] = value
    await _save_chat_settings(chat_id, settings)


# --- Прокси, добавленные через Telegram -------------------------------------
#
# Живут в хранилище, а не в переменных окружения, чтобы добавлять их на ходу и
# не передеплоивать сервис ради каждого нового адреса. Список из переменной
# окружения при этом никуда не девается — пул складывает оба.
EXTRA_PROXIES_KEY = "extra_proxies"
LOCAL_PROXIES_PATH = Path(__file__).parent / "extra_proxies_local.json"


async def get_extra_proxies() -> list[str]:
    if REDIS_ENABLED:
        try:
            raw = await _redis_cmd("GET", EXTRA_PROXIES_KEY)
            return json.loads(raw) if raw else []
        except Exception:
            pass
    if LOCAL_PROXIES_PATH.exists():
        try:
            return json.loads(LOCAL_PROXIES_PATH.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


async def save_extra_proxies(proxies: list[str]) -> None:
    value = json.dumps(proxies, ensure_ascii=False)
    if REDIS_ENABLED:
        try:
            await _redis_cmd("SET", EXTRA_PROXIES_KEY, value)
            return
        except Exception:
            pass
    try:
        LOCAL_PROXIES_PATH.write_text(value, encoding="utf-8")
    except Exception:
        log.exception("не смог сохранить список прокси")


# --- Кэш живых цен Steam ----------------------------------------------------
#
# Зачем отдельно от кэша стикеров: здесь хранится ещё и объём продаж, а он для
# арбитража не менее важен, чем цена — предмет, который не продаётся, нельзя
# перепродать ни за какую цену.
#
# Зачем вообще: priceoverview лимитирован жёстко, и проверять каждого кандидата
# живым запросом оказалось нерабочей схемой. На проде автоскан каждые 10 минут
# выжигал все адреса пула, после чего вторая половина бота приходила к пустому
# пулу и молчала. Кандидаты при этом повторяются от прогона к прогону, так что
# кэш убирает основную массу запросов.
STEAM_PRICE_KEY_PREFIX = "steamprice:"
STEAM_PRICE_TTL_SECONDS = 12 * 60 * 60
LOCAL_STEAM_PRICES_PATH = Path(__file__).parent / "steam_prices_local.json"


def _local_steam_prices_load() -> dict:
    if not LOCAL_STEAM_PRICES_PATH.exists():
        return {}
    try:
        return json.loads(LOCAL_STEAM_PRICES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


async def get_steam_prices_batch(names: list[str]) -> dict[str, dict]:
    """Кэшированные цены Steam: имя -> {'price', 'volume'}. Отсутствующие просто не попадают в результат."""
    if not names:
        return {}

    if REDIS_ENABLED:
        try:
            raw = await _redis_cmd("MGET", *[STEAM_PRICE_KEY_PREFIX + n for n in names])
            out = {}
            for name, value in zip(names, raw or []):
                if value:
                    try:
                        out[name] = json.loads(value)
                    except Exception:
                        pass
            return out
        except Exception:
            pass

    data = _local_steam_prices_load()
    now = time.time()
    out = {}
    for name in names:
        entry = data.get(name)
        if entry and (now - entry.get("updated_at", 0)) < STEAM_PRICE_TTL_SECONDS:
            out[name] = entry
    return out


async def set_steam_price(name: str, price: float, volume: int | None) -> None:
    entry = {"price": price, "volume": volume, "updated_at": time.time()}
    if REDIS_ENABLED:
        try:
            await _redis_cmd(
                "SET", STEAM_PRICE_KEY_PREFIX + name,
                json.dumps(entry), "EX", str(STEAM_PRICE_TTL_SECONDS),
            )
            return
        except Exception:
            pass

    data = _local_steam_prices_load()
    data[name] = entry
    try:
        LOCAL_STEAM_PRICES_PATH.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        log.exception("не смог сохранить локальный кэш цен Steam")


async def get_market_settings(chat_id: int) -> dict:
    """
    Настройки сравнения с площадками (/markets).

    Значение None означает «не задано», и тогда действует значение по
    умолчанию из кода — так настройка отличается от осознанно выставленного
    нуля, что для порогов важно.
    """
    s = await _get_chat_settings(chat_id)
    return {
        "min_discount": s.get("mk_min_discount"),
        "max_discount": s.get("mk_max_discount"),
        "min_price": s.get("mk_min_price"),
        "min_profit": s.get("mk_min_profit"),
        "min_volume": s.get("mk_min_volume"),
    }


async def set_market_setting(chat_id: int, key: str, value) -> None:
    """key — одно из: min_discount, max_discount, min_price, min_profit, min_volume."""
    allowed = {"min_discount", "max_discount", "min_price", "min_profit", "min_volume"}
    if key not in allowed:
        raise ValueError(f"неизвестная настройка площадок: {key}")
    settings = await _get_chat_settings(chat_id)
    settings[f"mk_{key}"] = value
    await _save_chat_settings(chat_id, settings)


async def all_chat_ids_with_settings() -> list[int]:
    """
    Чаты, у которых вообще есть сохранённые настройки — нужно, чтобы понять,
    кому слать арбитраж (он не привязан к вотчлисту: сканируется весь рынок).
    """
    if REDIS_ENABLED:
        try:
            keys = await _redis_cmd("KEYS", DEFAULTS_KEY_PREFIX + "*")
            out = []
            for k in keys or []:
                raw_id = k.split(":", 1)[-1]
                try:
                    out.append(int(raw_id))
                except ValueError:
                    continue
            return out
        except Exception:
            pass
    return [int(x) for x in _local_defaults_load().keys() if str(x).lstrip("-").isdigit()]


# ---------------------------------------------------------------------------
# Вотчлист /watchadd: список предметов на автоскан по расписанию + интервал,
# по chat_id. Тот же паттерн хранения (Upstash + локальный fallback), что и
# остальное состояние — переживает рестарт/редеплой на Render.
# ---------------------------------------------------------------------------

WATCHLIST_KEY_PREFIX = "watchlist:"
WATCHLIST_CHATS_INDEX_KEY = "watchlist_chats_index"  # Redis SET всех chat_id с вотчлистом — для восстановления джобов после рестарта
LOCAL_WATCHLIST_PATH = Path(__file__).parent / "watchlist_local.json"


def _local_watchlist_load() -> dict:
    if LOCAL_WATCHLIST_PATH.exists():
        try:
            return json.loads(LOCAL_WATCHLIST_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _local_watchlist_save(data: dict) -> None:
    LOCAL_WATCHLIST_PATH.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


async def _get_watchlist_entry(chat_id: int) -> dict:
    if REDIS_ENABLED:
        try:
            raw = await _redis_cmd("GET", WATCHLIST_KEY_PREFIX + str(chat_id))
            return json.loads(raw) if raw else {}
        except Exception:
            pass
    return _local_watchlist_load().get(str(chat_id), {})


async def _save_watchlist_entry(chat_id: int, **updates) -> None:
    """
    Обновляет только переданные поля записи, остальные сохраняет как были.
    Раньше запись собиралась заново из фиксированного набора полей, и каждое
    новое поле приходилось протаскивать через все вызовы — легко было случайно
    затереть соседнее. Теперь достаточно передать то, что меняется.
    """
    entry = await _get_watchlist_entry(chat_id)
    entry.update(updates)
    value = json.dumps(entry, ensure_ascii=False)

    if REDIS_ENABLED:
        try:
            await _redis_cmd("SET", WATCHLIST_KEY_PREFIX + str(chat_id), value)
            await _redis_cmd("SADD", WATCHLIST_CHATS_INDEX_KEY, str(chat_id))
            return
        except Exception:
            pass  # тоже падаем на локальный файл, чтобы данные не потерялись

    data = _local_watchlist_load()
    data[str(chat_id)] = entry
    _local_watchlist_save(data)


async def get_watchlist(chat_id: int) -> list[str]:
    """Список предметов (market_hash_name) в вотчлисте чата — охота по стикерам."""
    return (await _get_watchlist_entry(chat_id)).get("items", [])


async def set_watchlist(chat_id: int, items: list[str]) -> None:
    await _save_watchlist_entry(chat_id, items=items)


async def get_float_watchlist(chat_id: int) -> list[str]:
    """
    Отдельный список предметов под охоту за редким флоатом (/floatadd).
    Намеренно не смешан с обычным вотчлистом: флоат интересен на конкретных
    скинах, а гонять его по всему списку — лишняя работа и лишний шум.
    """
    return (await _get_watchlist_entry(chat_id)).get("float_items", [])


async def set_float_watchlist(chat_id: int, items: list[str]) -> None:
    await _save_watchlist_entry(chat_id, float_items=items)


async def get_watch_paused(chat_id: int) -> bool:
    """Приостановлен ли автоскан вотчлиста (/watchpause). Сам список при этом сохраняется."""
    return bool((await _get_watchlist_entry(chat_id)).get("paused"))


async def set_watch_paused(chat_id: int, paused: bool) -> None:
    await _save_watchlist_entry(chat_id, paused=paused)


async def all_watchlist_chat_ids() -> list[int]:
    """Все chat_id, у которых когда-либо был непустой вотчлист — для восстановления джобов после рестарта бота."""
    if REDIS_ENABLED:
        try:
            result = await _redis_cmd("SMEMBERS", WATCHLIST_CHATS_INDEX_KEY)
            return [int(x) for x in (result or [])]
        except Exception:
            pass
    return [int(x) for x in _local_watchlist_load().keys()]


# ---------------------------------------------------------------------------
# Дедупликация офферов автоскана вотчлиста: не слать один и тот же лот
# повторно в один чат, если его уже присылали в пределах SENT_OFFER_TTL_SECONDS.
# ---------------------------------------------------------------------------

SENT_OFFER_KEY_PREFIX = "sentoffer:"
SENT_OFFER_TTL_SECONDS = 5 * 60 * 60  # 5 часов
LOCAL_SENT_OFFERS_PATH = Path(__file__).parent / "sent_offers_local.json"


def _local_sent_offers_load() -> dict:
    if LOCAL_SENT_OFFERS_PATH.exists():
        try:
            return json.loads(LOCAL_SENT_OFFERS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _local_sent_offers_save(data: dict) -> None:
    LOCAL_SENT_OFFERS_PATH.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


async def was_offer_sent_recently(chat_id: int, offer_key: str) -> bool:
    """True, если этот же оффер уже отправляли в этот чат за последние SENT_OFFER_TTL_SECONDS."""
    full_key = f"{SENT_OFFER_KEY_PREFIX}{chat_id}:{offer_key}"

    if REDIS_ENABLED:
        try:
            return bool(await _redis_cmd("EXISTS", full_key))
        except Exception:
            pass

    sent_at = _local_sent_offers_load().get(full_key)
    return sent_at is not None and (time.time() - sent_at) < SENT_OFFER_TTL_SECONDS


async def mark_offer_sent(chat_id: int, offer_key: str) -> None:
    full_key = f"{SENT_OFFER_KEY_PREFIX}{chat_id}:{offer_key}"

    if REDIS_ENABLED:
        try:
            await _redis_cmd("SET", full_key, "1", "EX", str(SENT_OFFER_TTL_SECONDS))
            return
        except Exception:
            pass

    data = _local_sent_offers_load()
    data[full_key] = time.time()
    _local_sent_offers_save(data)


# ---------------------------------------------------------------------------
# Кулдаун Steam после 429 (см. steam_client.py), отдельно по областям —
# "listings" (/render/, вотчлист/сканы) и "pricing" (priceoverview/
# market-search, цена стикера) — у Steam это независимые лимиты, бан в одной
# не должен блокировать другую. Раньше жил только в переменных процесса — на
# каждом рестарте/редеплое Render бот "забывал" про ещё не снятый бан и тут
# же пробовал снова, получал новый 429 и тем самым продлевал реальный бан на
# стороне Steam. Храним по тому же паттерну, что и остальное состояние,
# чтобы кулдаун переживал рестарт процесса.
# ---------------------------------------------------------------------------

STEAM_COOLDOWN_KEY_PREFIX = "steam_cooldown:"
LOCAL_STEAM_COOLDOWN_PATH = Path(__file__).parent / "steam_cooldown_local.json"


def _local_steam_cooldown_load() -> dict:
    if LOCAL_STEAM_COOLDOWN_PATH.exists():
        try:
            return json.loads(LOCAL_STEAM_COOLDOWN_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _local_steam_cooldown_save(data: dict) -> None:
    LOCAL_STEAM_COOLDOWN_PATH.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


async def get_steam_cooldown(scope: str) -> Optional[dict]:
    """{'cooldown_until': epoch-секунды (time.time()), 'consecutive_429': int} либо None."""
    if REDIS_ENABLED:
        try:
            raw = await _redis_cmd("GET", STEAM_COOLDOWN_KEY_PREFIX + scope)
            return json.loads(raw) if raw else None
        except Exception:
            pass

    return _local_steam_cooldown_load().get(scope)


async def set_steam_cooldown(scope: str, cooldown_until: float, consecutive_429: int) -> None:
    value = json.dumps(
        {"cooldown_until": cooldown_until, "consecutive_429": consecutive_429}, ensure_ascii=False
    )

    if REDIS_ENABLED:
        try:
            await _redis_cmd("SET", STEAM_COOLDOWN_KEY_PREFIX + scope, value)
            return
        except Exception:
            pass

    data = _local_steam_cooldown_load()
    data[scope] = json.loads(value)
    _local_steam_cooldown_save(data)

