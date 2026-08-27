"""
Определение цены стикера по коду, вытащенному из имени файла иконки
(см. steam_client.STICKER_RE — код вида "paris2023:sig_dupreeh_champion").

ИСТОЧНИКИ ЦЕНЫ, по приоритету:
1. prices.csgotrader.app (/latest/steam.json) — статический JSON-файл на CDN
   (обновляется раз в час расширением CSGOTrader), весь каталог CS2 одним
   запросом. Основной источник, потому что и Steam priceoverview/market-search,
   И Skinport /v1/items банят датацентровые IP (Render) целиком — Steam 429-ит,
   а Skinport с 2025 закрыт Cloudflare Bot Management (403 + JS-challenge даже
   с браузерными заголовками). Это статический файл, а не живой API за
   антибот-защитой — его отдают как обычный CDN-asset, поэтому не банится
   так же охотно. См. диагностику от 2026-08-11/12.
2. Steam (priceoverview -> market/search) — резерв для того, чего нет в файле
   csgotrader.app (совсем новые/уникальные стикеры, которые ещё не попали в
   их прайс-лист). Может по-прежнему 429-ить, если IP всё ещё забанен — тогда
   просто ничего не находит, но не валит остальной пайплайн.

Это не влияет на другой источник данных — листинги скинов (какие стикеры
на них наклеены) по-прежнему приходят либо через steam_client.fetch_all_listings,
либо через вручную присланный Steam JSON (bot-16.handle_document). Здесь
только цена самого стикера.
"""

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import NamedTuple, Optional

import aiohttp

from proxy_pool import ProxyPool, mask as mask_proxy
from sticker_catalog import get_catalog
from storage import get_prices_batch, set_prices_batch
from steam_client import (
    STEAM_POOL,
    steam_cookie_header,
    throttle_steam_request,
    steam_cooldown_remaining,
    note_steam_429,
    note_steam_ok,
)

from http_session import get_session

log = logging.getLogger("steam_bot.pricing")

OVERRIDES_PATH = Path(__file__).parent / "sticker_overrides.json"
CACHE_TTL_SECONDS = 12 * 60 * 60  # 12 часов — цены на стикеры не скачут ежеминутно

# Ретраев и собственных пауз тут больше нет: темп запросов к Steam держит
# общий троттлинг из steam_client (throttle_steam_request), а на 429 там же
# ставится глобальный кулдаун вместо повторных попыток.

# --- Ручной прайс-лист от пользователя (Steam market/search/render, категория
# стикеров, скачано вручную через телефон/браузер — не банится, в отличие от
# сервера). Проверяется ПЕРВЫМ, раньше Skinport: это настоящие Steam-цены,
# точнее кросс-маркетной оценки. ---
MANUAL_PRICE_CACHE_PATH = Path(__file__).parent / "manual_sticker_prices.json"

CSGOTRADER_PRICES_URL = "https://prices.csgotrader.app/latest/steam.json"
# v3: в кэше лежат ВСЕ окна предмета, а не только выбранная цена.
#
# Зачем все: по одной цене нельзя понять, можно ли ей верить. Когда цена
# 2026-08-19 разошлась с реальной в 2.4 раза, версий было две — "взяли
# устаревшее окно" и "в файле не то, что мы думаем", — и различить их по
# сохранённому числу было нельзя. С полным набором окон это видно сразу:
# согласованные значения означают устойчивую цену, разъезжающиеся — что
# доверять ей нельзя независимо от того, какое окно мы выбрали.
#
# Имя файла меняется вместе с форматом намеренно: кэш прошлой версии читаться
# как новый не должен.
CSGOTRADER_CACHE_PATH = Path(__file__).parent / "csgotrader_prices_cache_v3.json"
CSGOTRADER_CACHE_TTL_SECONDS = 3 * 60 * 60  # на их стороне файл обновляется примерно раз в час

# collection-код из имени файла -> человекочитаемое название турнира/капсулы
COLLECTION_TO_EVENT = {
    "paris2023": "Paris 2023",
    "cph2024": "Copenhagen 2024",
    "antwerp2022": "Antwerp 2022",
    "rio2022": "Rio 2022",
    "sha2024": "Shanghai 2024",
    "rmr2020": "2020 RMR",
    "stockh2021": "Stockholm 2021",
    "berlin2019": "Berlin 2019",
    "bud2025": "Budapest 2025",
    "cologne2016": "Cologne 2016",
    "cologne2026": "Cologne 2026",
    "csgo10": "10 Year Birthday",
    "aus2025": "Austin 2025",
}

FINISH_WORDS = ("glitter", "holo", "foil", "gold", "champion", "embroidered")


def _split_code(code: str) -> tuple[str, list[str]]:
    """'sig_dupreeh_champion' -> ('dupreeh', ['champion'])"""
    parts = code.split("_")
    finishes = [p for p in parts if p in FINISH_WORDS]
    name_parts = [p for p in parts if p not in FINISH_WORDS and p != "sig"]
    return " ".join(name_parts), finishes


def _resolve_market_hash_name(sticker_key: str, overrides: dict, catalog: dict) -> tuple[str, bool]:
    """
    Возвращает (market_hash_name_или_поисковый_запрос, is_exact).
    Порядок приоритета: ручной override -> точный каталог CSGO-API -> старая эвристика (fallback).
    """
    if sticker_key in overrides:
        return overrides[sticker_key], True

    if sticker_key in catalog:
        return catalog[sticker_key], True

    collection, code = sticker_key.split(":", 1)
    name, finishes = _split_code(code)
    event = COLLECTION_TO_EVENT.get(collection, collection)
    finish_str = f" ({', '.join(f.capitalize() for f in finishes)})" if finishes else ""
    return f"{name}{finish_str} {event}", False


# ---------------------------------------------------------------------------
# Кэш прочитанных JSON-файлов в памяти процесса
#
# Прайс-лист csgotrader — это ~3 МБ JSON с двумя десятками тысяч записей, и
# читался он С ДИСКА ЗАНОВО на каждый предмет: get_sticker_prices зовёт
# _load_csgotrader_cache, а её зовут по разу на каждый предмет вотчлиста.
# Плюс поверх этого get_csgotrader_price_details каждый раз заново перебирала
# все записи, выбирая окно цены. На прогоне в 110 предметов это десятки секунд
# чистого CPU — причём БЛОКИРУЮЩЕГО: пока идёт json.loads, event loop стоит, и
# вместе с ним стоят все параллельные запросы сканера.
#
# Файл между этими чтениями не меняется (обновляется раз в три часа), так что
# держим разобранный результат в памяти и сверяемся с mtime+размером файла.
# Такой ключ переживает и подмену файла другим процессом, и правку руками, и
# при этом не требует ни явной инвалидации, ни TTL.
# ---------------------------------------------------------------------------

_JSON_MEMO: dict[Path, tuple[tuple[int, int], object]] = {}


def _file_stamp(path: Path) -> tuple[int, int] | None:
    """(mtime в нс, размер) — отпечаток файла. None, если файла нет."""
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


def _read_json(path: Path, default):
    """Прочитать JSON с диска без кэша. default при отсутствии файла или битом содержимом."""
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        log.exception("не удалось прочитать %s", path)
        return default


def _read_json_memo(path: Path, default):
    """
    То же, но с кэшем в памяти по отпечатку файла.

    ВАЖНО: возвращается ОДИН И ТОТ ЖЕ объект при повторных вызовах — менять его
    на месте нельзя. Кому нужна своя копия для правки (ingest_manual_prices),
    берёт данные через _read_json.
    """
    stamp = _file_stamp(path)
    if stamp is None:
        _JSON_MEMO.pop(path, None)
        return default

    hit = _JSON_MEMO.get(path)
    if hit is not None and hit[0] == stamp:
        return hit[1]

    data = _read_json(path, default)
    _JSON_MEMO[path] = (stamp, data)
    return data


def _load_overrides() -> dict:
    return _read_json_memo(OVERRIDES_PATH, {})


# ---------------------------------------------------------------------------
# Ручной прайс-лист (вручную скачанный Steam market/search/render по категории
# стикеров — bot.py.handle_document распознаёт такой файл и зовёт ingest_manual_prices)
# ---------------------------------------------------------------------------

def _load_manual_prices() -> dict[str, float]:
    """Только для чтения — объект общий (см. _read_json_memo)."""
    return _read_json_memo(MANUAL_PRICE_CACHE_PATH, {})


def ingest_manual_prices(items: dict[str, float]) -> int:
    """
    Сливает новые цены в постоянный локальный прайс-лист (не TTL-кэш —
    хранится, пока сам файл не перетрут явно). Вызывается из bot.py при
    получении JSON вида market/search/render с ценами по категории стикеров.
    Возвращает количество реально добавленных/обновлённых записей.
    """
    # Свежая копия с диска, а не общий кэшированный объект: его правка на месте
    # оставила бы в памяти данные, которых ещё нет в файле.
    current = dict(_read_json(MANUAL_PRICE_CACHE_PATH, {}))
    changed = 0
    for name, price in items.items():
        if not name or price is None:
            continue
        if current.get(name) != price:
            changed += 1
        current[name] = price

    MANUAL_PRICE_CACHE_PATH.write_text(json.dumps(current, ensure_ascii=False), encoding="utf-8")
    log.info("manual_prices: сохранено %s записей (%s новых/изменённых), всего в файле %s", len(items), changed, len(current))
    return changed


def clear_manual_prices() -> int:
    """Полностью стирает ручной прайс-лист (перед загрузкой свежего). Возвращает, сколько записей было."""
    count = len(_load_manual_prices())
    if MANUAL_PRICE_CACHE_PATH.exists():
        MANUAL_PRICE_CACHE_PATH.unlink()
    log.info("manual_prices: очищено (было %s записей)", count)
    return count


def manual_prices_count() -> int:
    return len(_load_manual_prices())


# ---------------------------------------------------------------------------
# prices.csgotrader.app — статический прайс-лист (замена Skinport, см. докстринг
# модуля: Skinport с 2025 закрыт Cloudflare Bot Management для датацентровых IP)
# ---------------------------------------------------------------------------

# Окна прайс-листа от самого свежего к самому старому.
CSGOTRADER_WINDOWS = ("last_24h", "last_7d", "last_30d", "last_90d")

# Окна, по которым считается «согласуется ли сегодняшняя цена с недельной»
# (см. SteamPrice.recent_spread_pct). Арбитраж берёт строго last_24h — какое
# именно окно ему нужно, решает bot.ARB_PRICE_WINDOW, а не этот список.
CSGOTRADER_RECENT_WINDOWS = ("last_24h", "last_7d")


def _load_csgotrader_cache() -> tuple[dict, float] | None:
    """Содержимое дискового кэша прайс-листа. Читается один раз на версию файла (см. _read_json_memo)."""
    data = _read_json_memo(CSGOTRADER_CACHE_PATH, None)
    if not isinstance(data, dict) or "details" not in data or "updated_at" not in data:
        return None
    return data["details"], data["updated_at"]


def _save_csgotrader_cache(details: dict) -> None:
    CSGOTRADER_CACHE_PATH.write_text(
        json.dumps({"details": details, "updated_at": time.time()}, ensure_ascii=False),
        encoding="utf-8",
    )


class SteamPrice(NamedTuple):
    """Цена предмета из прайс-листа вместе с основаниями ей верить."""

    price: float
    window: str                     # из какого окна взята цена
    windows: dict[str, float]       # все непустые окна предмета

    @property
    def spread_pct(self) -> float | None:
        """
        Разброс по ВСЕМ окнам: (max - min) / min в процентах.
        None — окно всего одно, сравнивать не с чем.
        """
        return self._spread(self.windows.values())

    @property
    def recent_spread_pct(self) -> float | None:
        """
        Разброс только между сутками и неделей — то есть «сегодняшняя цена
        согласуется с недельной?».

        Считать разброс по всем четырём окнам было ошибкой: предмет мог честно
        подешеветь за три месяца и при этом быть совершенно стабильным сейчас,
        а проверка выбрасывала его как «неустойчивый». Расхождение с
        90-дневным окном говорит о движении цены во времени, а не о том, что
        текущей цене нельзя верить.
        """
        recent = [self.windows[w] for w in CSGOTRADER_RECENT_WINDOWS if w in self.windows]
        return self._spread(recent)

    @staticmethod
    def _spread(values) -> float | None:
        values = list(values)
        if len(values) < 2:
            return None
        lo, hi = min(values), max(values)
        if lo <= 0:
            return None
        return (hi - lo) / lo * 100

    def describe(self) -> str:
        """Все окна одной строкой — для логов."""
        parts = ", ".join(f"{w.replace('last_', '')}=${self.windows[w]:.2f}"
                          for w in CSGOTRADER_WINDOWS if w in self.windows)
        spread = f", разброс {self.spread_pct:.0f}%" if self.spread_pct is not None else ""
        return f"{parts}{spread}"


def _collect_windows(entry: dict) -> dict[str, float]:
    """Все непустые окна предмета, приведённые к числам."""
    out: dict[str, float] = {}
    for window in CSGOTRADER_WINDOWS:
        value = entry.get(window)
        if value is None:
            continue
        try:
            out[window] = float(value)
        except (TypeError, ValueError):
            continue
    return out


def _steam_price_from_windows(windows: dict[str, float]) -> SteamPrice | None:
    """
    Цена = самое свежее непустое окно. Порядок фолбэка от свежего к старому:
    у неликвида первых окон часто нет, и тогда берётся более старое.

    Само по себе старое окно ещё не значит "цена неверна" — если предмет
    стабилен, цена за 30 дней ничем не хуже. Поэтому решение "верить или нет"
    принимается не по возрасту окна, а по согласованности окон между собой
    (см. SteamPrice.spread_pct); тот, кто цену использует, получает и то и
    другое.
    """
    for window in CSGOTRADER_WINDOWS:
        if window in windows:
            return SteamPrice(windows[window], window, windows)
    return None


def _pick_csgotrader_price(entry: dict) -> float | None:
    picked = _steam_price_from_windows(_collect_windows(entry))
    return picked.price if picked else None


async def _download_csgotrader_prices(session: aiohttp.ClientSession) -> dict[str, float]:
    """
    Один запрос -> весь прайс-лист CS2 (статический JSON-файл на CDN, не живой
    API за антибот-защитой). Ключ: market_hash_name -> цена в USD.
    Возвращает {} при любой ошибке (вызывающий код тогда падает на Steam-фолбэк).
    """
    try:
        async with session.get(
            CSGOTRADER_PRICES_URL, timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status != 200:
                log.warning("csgotrader: HTTP %s при получении прайс-листа", resp.status)
                return {}
            raw = await resp.json(content_type=None)
    except Exception:
        log.exception("csgotrader: не удалось скачать/распарсить прайс-лист")
        return {}

    details: dict[str, dict] = {}
    by_window: dict[str, int] = {}
    for name, entry in raw.items():
        if not isinstance(entry, dict):
            continue
        windows = _collect_windows(entry)
        picked = _steam_price_from_windows(windows)
        if picked is not None:
            details[name] = windows
            by_window[picked.window] = by_window.get(picked.window, 0) + 1

    log.info(
        "csgotrader: получено %s цен из %s записей в файле. Свежесть цены по окнам: %s",
        len(details), len(raw),
        ", ".join(f"{w} {by_window.get(w, 0)}" for w in CSGOTRADER_WINDOWS),
    )
    return details


# Разобранный прайс-лист: (отпечаток файла-кэша, готовый словарь).
# Пересборка перебирает ~20 000 записей и на слабом CPU стоит десятки
# миллисекунд — терпимо раз в три часа и совсем не терпимо на каждый предмет.
_CSGOTRADER_DERIVED: tuple[tuple[int, int], dict[str, "SteamPrice"]] | None = None


async def get_csgotrader_price_details(
    session: aiohttp.ClientSession | None = None, force_refresh: bool = False
) -> dict[str, SteamPrice]:
    """
    market_hash_name -> SteamPrice (цена, окно и ВСЕ окна предмета).
    Полный набор окон нужен тем, кто считает по цене скидку: по одному числу
    нельзя понять, устойчива цена или скачет.

    session можно не передавать — возьмётся общая сессия процесса. Скачивание
    происходит только когда дисковый кэш протух (или force_refresh), в
    остальных случаях сеть не трогается вовсе.
    """
    global _CSGOTRADER_DERIVED

    cached = _load_csgotrader_cache()
    if not force_refresh and cached and (time.time() - cached[1]) < CSGOTRADER_CACHE_TTL_SECONDS:
        details = cached[0]
    else:
        fresh = await _download_csgotrader_prices(session or get_session())
        if fresh:
            _save_csgotrader_cache(fresh)
            details = fresh
        elif cached:
            # скачивание не удалось — используем что было, пусть и протухшее
            details = cached[0]
        else:
            return {}

    stamp = _file_stamp(CSGOTRADER_CACHE_PATH)
    if stamp is not None and _CSGOTRADER_DERIVED is not None and _CSGOTRADER_DERIVED[0] == stamp:
        return _CSGOTRADER_DERIVED[1]

    out: dict[str, SteamPrice] = {}
    for name, windows in details.items():
        if not isinstance(windows, dict):
            continue
        picked = _steam_price_from_windows(windows)
        if picked is not None:
            out[name] = picked

    if stamp is not None:
        _CSGOTRADER_DERIVED = (stamp, out)
    return out


# Только цены, без окон — производная того же словаря, с ключом по тому же
# отпечатку файла.
_CSGOTRADER_PRICES_ONLY: tuple[tuple[int, int], dict[str, float]] | None = None


async def get_csgotrader_prices(
    session: aiohttp.ClientSession | None = None, force_refresh: bool = False
) -> dict[str, float]:
    """Только цены — стикерному пайплайну окна не нужны."""
    global _CSGOTRADER_PRICES_ONLY

    details = await get_csgotrader_price_details(session, force_refresh=force_refresh)

    stamp = _file_stamp(CSGOTRADER_CACHE_PATH)
    if stamp is not None and _CSGOTRADER_PRICES_ONLY is not None and _CSGOTRADER_PRICES_ONLY[0] == stamp:
        return _CSGOTRADER_PRICES_ONLY[1]

    prices = {name: sp.price for name, sp in details.items()}
    if stamp is not None:
        _CSGOTRADER_PRICES_ONLY = (stamp, prices)
    return prices


# ---------------------------------------------------------------------------
# Steam (фолбэк для того, чего нет в прайс-листе csgotrader.app)
# ---------------------------------------------------------------------------

# Пул адресов для запросов к Steam. По умолчанию тот же список, что и у
# CSFloat: отдельная переменная нужна редко, а заставлять дублировать одно и то
# же значение в двух полях Render — верный способ получить рассинхрон.
#
# Смысл тот же, что и у CSFloat: Steam банит ПО АДРЕСУ, поэтому несколько
# адресов превращают «бот молчит час» в «идём со следующего». Именно из-за
# поадресных банов в этом проекте и живёт вся машинерия кулдаунов в
# steam_client — с пулом она перестаёт быть единственной защитой.
# Пул общий со листингами и живёт в steam_client — см. комментарий там.
# Здесь только переэкспорт, чтобы вызывающий код не гадал, откуда брать.

# На сколько откладывать прокси, которому Steam ответил 429.
#
# Было 30 минут — по аналогии с баном обычного адреса. Для РОТИРУЕМЫХ
# резидентных прокси это оказалось вредно: Steam банит исходящий IP, а мы
# откладывали ЛОГИН, за которым в следующий раз стоит уже другой адрес. То есть
# каждый 429 выключал рабочий канал на полчаса ни за что. В логе это выглядело
# так: восемь запросов подряд, и все семь логинов ушли в кулдаун за четыре
# секунды, после чего бот замолчал до конца получаса.
#
# Проверить, ротируются ли адреса, можно командой /proxycheck — она спрашивает
# каждый логин дважды и показывает, меняется ли IP между запросами.
#
# Полминуты достаточно: это защита от того, чтобы долбить в одну секунду, а не
# наказание. Если у провайдера адреса закреплённые, значение стоит поднять.
STEAM_PROXY_COOLDOWN_SECONDS = int(os.environ.get("STEAM_PROXY_COOLDOWN_SECONDS", "30"))

# Сколько адресов перебрать, прежде чем сдаться по одному предмету. При
# полусотне прокси перебирать все ради одной цены расточительно — дешевле
# оставить предмет следующему прогону, кэш всё равно накапливается.
STEAM_RETRY_CAP = int(os.environ.get("STEAM_RETRY_CAP", "4"))

# Сколько проверок цены вести одновременно — НЕЗАВИСИМО от размера пула прокси.
#
# Раньше вызывающий код брал lanes = len(STEAM_POOL), и это оказалось ловушкой:
# добавив 46 прокси, пользователь получил 46 одновременных полос. Диагностика
# 2026-08-27 показала 53 запроса к priceoverview за минуту при заявленной паузе
# в 4 секунды — потому что пауза считается ПО ПОЛОСЕ, и 46 полос дают 46
# независимых очередей. priceoverview у Steam зарезан жёстче всех эндпоинтов и
# такого залпа не прощает: 429 прилетел на все маршруты, включая прямой.
#
# Больше адресов здесь не помогает вообще: лимит на этом эндпоинте, судя по
# поведению, не только поадресный. Поэтому потолок фиксированный и маленький.
PRICE_CONCURRENCY = int(os.environ.get("PRICE_CONCURRENCY", "2"))

# Пауза между запросами цены — ГЛОБАЛЬНАЯ, одна очередь на весь процесс.
#
# Для листингов пауза поадресная и это правильно: там бан подтверждённо
# поадресный, и N адресов честно дают N полос. Для priceoverview так не
# работает — см. выше. Поэтому здесь одна общая очередь и своя, более длинная
# пауза, не зависящая от MIN_REQUEST_INTERVAL листингов.
PRICE_REQUEST_INTERVAL = float(os.environ.get("PRICE_REQUEST_INTERVAL", "6.0"))

# Брать ли цену из листингов (/render/), когда priceoverview отказал.
#
# Размен осознанный. Плюс: /render/ у Steam зарезан несопоставимо мягче, и
# 2026-08-27 он работал ровно в те часы, когда priceoverview отвечал 429 на всё
# подряд — то есть без этого запасного пути проверка цен просто не работает.
# Минус: проверка переезжает в область кулдауна "listings", то есть под общий
# бюджет с вотчлистом, и если Steam забанит и её, встанет всё сразу. Объём при
# этом невелик — 8 запросов за прогон арбитража против 110 у вотчлиста.
#
# priceoverview остаётся ПЕРВЫМ вариантом: он дешевле по трафику, отдаёт ещё и
# объём продаж, и как только прямой адрес отлежится, всё вернётся к нему само.
PRICE_FALLBACK_TO_LISTINGS = os.environ.get("PRICE_FALLBACK_TO_LISTINGS", "1") not in ("0", "false", "no")


class RateLimited(Exception):
    """Внутренний маркер: Steam ответил 429 либо мы на кулдауне после недавнего 429."""


def _steam_request_headers() -> dict:
    """
    Cookie реальной Steam-сессии (если задана, см. steam_client.py) — только
    для запросов на steamcommunity.com. Session, в которой выполняются эти
    запросы, общая с запросами к csgotrader.app, поэтому куки добавляются
    точечно per-request, а не в headers самой ClientSession, чтобы не утекали
    на чужой домен.
    """
    cookie = steam_cookie_header()
    return {"Cookie": cookie} if cookie else {}


async def _get_with_retry(session: aiohttp.ClientSession, url: str, params: dict, label: str):
    """
    Один запрос к Steam за ценой, с перебором маршрутов внутри.

    Область кулдауна своя (scope="pricing"), отдельная от листингов:
    priceoverview и market/search Steam банит независимо от /render/ и заметно
    охотнее (см. steam_client.py), поэтому бан здесь не должен тормозить сбор
    листингов.

    ПОРЯДОК МАРШРУТОВ: сначала прямой, потом прокси. Раньше было наоборот — при
    непустом пуле прокси брался всегда, а прямой адрес не пробовался ни разу.
    Это дало парадокс, пойманный на проде 2026-08-27: пока пул был мёртвый,
    проверка цен ходила напрямую и работала; как только в пул добавили живые
    резидентные адреса, ВСЕ восемь проверок за прогон стали получать 429 с
    первого же запроса, и арбитраж перестал подтверждать хоть что-нибудь. Дешёвые
    резидентные IP у Steam давно в чёрных списках (их жгли другие клиенты
    провайдера), тогда как прямой адрес Render в тот же час спокойно отдавал
    листингам HTTP 200.

    ПОЧЕМУ ПЕРЕБОР ЗДЕСЬ, А НЕ СНАРУЖИ. Напрашивалось проще: поймать 429 на
    прямом, поставить кулдаун области и дать внешнему циклу
    (get_steam_market_price_retrying) уйти на прокси. Так делать нельзя —
    note_steam_429 через _apply_collateral_cooldown придерживает ОСТАЛЬНЫЕ
    области, то есть один 429 на проверке цены остановил бы вотчлист на
    полчаса. Поэтому смена маршрута происходит тут же, без глобального
    кулдауна, и он ставится только когда кончились все маршруты — ровно как в
    steam_client.fetch_all_listings.
    """
    # Область уже на кулдауне — не лезем вообще, ни прямо, ни через прокси.
    #
    # Раньше здесь при кулдауне сразу брался прокси, и это выходило боком: в
    # прогоне арбитража первый же исчерпавший маршруты кандидат ставил кулдаун,
    # а остальные семь продолжали долбить Steam через пул — то есть ровно во
    # время бана, продлевая его. У priceoverview лимит не только поадресный,
    # так что «на другом адресе можно» здесь не работает.
    if steam_cooldown_remaining(scope="pricing") > 0:
        raise RateLimited()

    routes: list[str | None] = [None]  # прямой маршрут пробуем первым
    tries_left = min(max(1, len(STEAM_POOL)), STEAM_RETRY_CAP)

    last_status: int | None = None
    while True:
        if routes:
            proxy = routes.pop(0)
        else:
            if tries_left <= 0:
                break
            proxy = STEAM_POOL.next() if STEAM_POOL.enabled() else None
            if proxy is None:
                break
            tries_left -= 1

        # Через прокси куку аккаунта НЕ шлём.
        #
        # steamLoginSecure — авторизация конкретной сессии, и Steam привязывает
        # её к адресу. Гонять один логин одновременно с десятков резидентных IP
        # выглядит ровно как кража сессии: в лучшем случае разлогинит, в худшем
        # пометит аккаунт. priceoverview — публичный эндпоинт, логин ему не
        # нужен, а bMarketOptOut=1 работает и без него.
        headers = _steam_request_headers() if proxy is None else {"Cookie": "bMarketOptOut=1"}

        # Пауза ОБЩАЯ на всю область, а не по адресу (lane пустой намеренно).
        #
        # Для листингов пауза поадресная, и там это верно. Здесь пробовали так
        # же — и получили 53 запроса в минуту при «паузе 4 сек»: 46 адресов
        # дали 46 независимых очередей, priceoverview ответил 429 на всё
        # подряд, включая прямой адрес. Один этот эндпоинт держим одной
        # очередью и с более длинной паузой (PRICE_REQUEST_INTERVAL).
        await throttle_steam_request(
            scope="pricing", lane="", interval=PRICE_REQUEST_INTERVAL
        )
        resp_ctx = session.get(url, params=params, headers=headers, proxy=proxy)
        try:
            resp = await resp_ctx.__aenter__()
        except aiohttp.ClientError as e:
            # Сбой САМОГО соединения (в т.ч. ClientHttpProxyError 403, когда
            # провайдер отказал в обслуживании) — это НЕ ответ Steam. Раньше
            # такое исключение улетало наверх мимо ветки "resp.status == 429" и
            # мимо retry-цикла снаружи, и кандидат сразу считался «Steam не
            # ответил». На проде это выглядело как «25 из 25 запросов не прошли —
            # Steam ограничил доступ», хотя Steam тут был ни при чём.
            if proxy:
                if getattr(e, "status", None) == 403:
                    STEAM_POOL.mark_refused(proxy, STEAM_PROXY_COOLDOWN_SECONDS, f"HTTP 403: {e}")
                else:
                    STEAM_POOL.mark_exhausted(proxy, STEAM_PROXY_COOLDOWN_SECONDS, f"ошибка соединения: {e}")
            log.warning(
                "%s: маршрут %s не работает (%s) для запроса %r",
                label, mask_proxy(proxy) if proxy else "прямой", e,
                params.get("query") or params.get("market_hash_name"),
            )
            continue

        try:
            if resp.status == 429:
                log.warning(
                    "%s: HTTP 429 для запроса %r (маршрут: %s)",
                    label, params.get("query") or params.get("market_hash_name"),
                    mask_proxy(proxy) if proxy else "напрямую",
                )
                last_status = 429
                if proxy:
                    # Забанен адрес, а не мы целиком: откладываем его и берём
                    # следующий. Общий кулдаун области — только когда маршруты
                    # кончатся совсем (см. ниже), иначе один плохой адрес
                    # тормозил бы вообще всё, включая вотчлист.
                    STEAM_POOL.mark_exhausted(proxy, STEAM_PROXY_COOLDOWN_SECONDS, "Steam ответил 429")
                continue

            await note_steam_ok(scope="pricing")
            STEAM_POOL.mark_ok(proxy)  # прошло — забываем прошлые отказы адреса
            if resp.status != 200:
                log.warning(
                    "%s: HTTP %s для запроса %r",
                    label, resp.status, params.get("query") or params.get("market_hash_name"),
                )
                return None
            return await resp.json()
        finally:
            await resp_ctx.__aexit__(None, None, None)

    # Маршруты кончились. Кулдаун области ставим только теперь и только если
    # причиной был именно 429 — на сетевых сбоях прокси Steam ни при чём, и
    # наказывать за них прямой адрес (а через collateral — ещё и вотчлист)
    # было бы прямо вредно.
    if last_status == 429:
        await note_steam_429(scope="pricing", headers={})
    raise RateLimited()


def _money(raw) -> float | None:
    """'$28.18' / '1,234.56 руб.' -> число. Разделители тысяч выкидываем."""
    if not raw:
        return None
    cleaned = "".join(c for c in str(raw) if c.isdigit() or c in ".,")
    # Обрезаем разделители по краям. Без этого точка из суффикса валюты
    # («1 234,56 руб.») прилипала к числу, ветка ниже считала строку
    # десятичной по наличию точки и выдавала 123456 вместо 1234.56.
    cleaned = cleaned.strip(".,")
    # Запятая может быть и разделителем тысяч, и десятичной. Считаем десятичной
    # только последнюю, если после неё ровно две цифры.
    if "," in cleaned and "." not in cleaned:
        head, _, tail = cleaned.rpartition(",")
        cleaned = f"{head.replace(',', '')}.{tail}" if len(tail) == 2 else cleaned.replace(",", "")
    else:
        cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


class SteamMarketPrice(NamedTuple):
    """Живой ответ Steam priceoverview по одному предмету."""

    lowest: float | None    # нижняя цена в стакане — столько стоит КУПИТЬ сейчас
    median: float | None    # медиана продаж за сутки
    volume: int | None      # сколько продано за сутки


async def get_steam_market_price_retrying(
    session: aiohttp.ClientSession, market_hash_name: str, attempts: int | None = None
) -> SteamMarketPrice | None:
    """
    То же, что get_steam_market_price, но при отказе пробует следующий адрес.

    Смысл именно в смене адреса. Steam ограничивает по исходящему IP, а у нас
    их много — значит отказ на одном не говорит ничего про остальные, и
    сдаваться после первой же неудачи неправильно. Раньше так и было: один 429,
    и кандидат уходил в «не проверен» до следующего прогона.

    Число попыток по умолчанию — размер пула, но не больше STEAM_RETRY_CAP:
    при полусотне адресов перебирать все ради одного предмета не стоит, дешевле
    оставить его на потом.
    """
    limit = attempts if attempts is not None else min(max(1, len(STEAM_POOL)), STEAM_RETRY_CAP)
    for attempt in range(limit):
        try:
            return await get_steam_market_price(session, market_hash_name)
        except RateLimited:
            if attempt + 1 >= limit:
                break
            # Адрес уже помечен в _get_with_retry, следующий заход возьмёт другой.
            continue

    # priceoverview недоступен — берём цену из листингов.
    #
    # Это не «ещё одна попытка того же», а ДРУГОЙ эндпоинт с другим лимитом:
    # /render/ у Steam режется несопоставимо мягче. 2026-08-27 это было видно
    # в одном логе — все проверки цены получали 429, а вотчлист в те же часы
    # собирал листинги с HTTP 200 и с того же прямого адреса.
    if PRICE_FALLBACK_TO_LISTINGS:
        listed = await _lowest_price_from_listings(market_hash_name)
        if listed is not None:
            return listed
    raise RateLimited()


async def _lowest_price_from_listings(market_hash_name: str) -> SteamMarketPrice | None:
    """
    Нижняя цена в стакане — из листингов (/render/) вместо priceoverview.

    Steam отдаёт лоты отсортированными от дешёвых к дорогим (см. докстринг
    steam_client.fetch_all_listings), поэтому цена первого лота и есть lowest —
    ровно то же число, что даёт priceoverview.lowest_price.

    Чего здесь НЕТ: медианы и объёма продаж за сутки. Медиана арбитражу не
    нужна (он считает по lowest), а ликвидность и так определяется по окнам
    прайс-листа и по reference.quantity от CSFloat — см. bot._fill_steam_prices.
    Возвращаем volume=None честно, а не нулём: «не знаем» и «не продаётся» —
    разные вещи, и на этом уже обжигались.

    Запросов столько же, сколько у priceoverview: один на предмет. Дороже он
    только по трафику (страница листингов против крошечного JSON), но входящий
    трафик на Render бесплатен.
    """
    from steam_client import fetch_all_listings  # локально: steam_client импортирует pricing

    try:
        listings = await fetch_all_listings(market_hash_name, max_listings=1)
    except Exception as e:
        log.info("цена из листингов: %s — не получилось (%s)", market_hash_name, e)
        return None

    prices = [l.price for l in listings if l.price and l.price > 0]
    if not prices:
        return None
    lowest = min(prices)
    log.info(
        "цена из листингов: %s — $%.2f (priceoverview недоступен, взяли из /render/)",
        market_hash_name, lowest,
    )
    return SteamMarketPrice(lowest=lowest, median=None, volume=None)


async def get_steam_market_price(
    session: aiohttp.ClientSession, market_hash_name: str
) -> SteamMarketPrice | None:
    """
    Настоящая цена предмета — у самого Steam, а не по чужой оценке.

    Понадобилось, потому что двух источников оказалось мало. Прайс-лист
    csgotrader и справка CSFloat расходятся ВДВОЕ и систематически: на всех
    находках 2026-08-20 отношение держалось около 2.3, причём справка почти
    точно совпадала с ценой лота (она и считается по рынку CSFloat, это не
    цена Steam). Какой из двух прав, по ним самим не определить никогда —
    нужен третий источник, и другого настоящего нет.

    Пакетного эндпоинта цен у Steam нет, priceoverview отвечает по одному
    предмету. Поэтому звать это можно ТОЛЬКО по горстке кандидатов, уже
    прошедших дешёвый отбор, а не по всему просмотренному рынку.

    lowest — то, что реально заплатишь; median — медиана продаж, то же по
    смыслу, что даёт прайс-лист. Для арбитража нужен lowest.
    """
    data = await _get_with_retry(
        session,
        "https://steamcommunity.com/market/priceoverview/",
        {"appid": 730, "currency": 1, "market_hash_name": market_hash_name},
        "priceoverview",
    )
    if not data or not data.get("success"):
        return None

    volume = None
    if data.get("volume"):
        try:
            volume = int(str(data["volume"]).replace(",", "").replace(" ", ""))
        except ValueError:
            volume = None

    return SteamMarketPrice(
        lowest=_money(data.get("lowest_price")),
        median=_money(data.get("median_price")),
        volume=volume,
    )


async def _price_overview(session: aiohttp.ClientSession, market_hash_name: str) -> float | None:
    url = "https://steamcommunity.com/market/priceoverview/"
    params = {"appid": 730, "currency": 1, "market_hash_name": market_hash_name}
    data = await _get_with_retry(session, url, params, "priceoverview")

    if not data or not data.get("success"):
        return None

    # Через _money, а не самодельной склейкой цифр: та склеивала разделитель
    # тысяч с числом ("$1,234.56" -> 1234.56 только по счастливой случайности,
    # а "1 234,56 руб." -> 123456).
    return _money(data.get("median_price") or data.get("lowest_price"))


async def _search_price(session: aiohttp.ClientSession, query: str) -> tuple[str, float] | None:
    url = "https://steamcommunity.com/market/search/render/"
    params = {
        "query": query, "start": 0, "count": 1,
        "search_descriptions": 0, "appid": 730, "norender": 1,
    }
    data = await _get_with_retry(session, url, params, "market/search")

    if not data:
        return None
    results = data.get("results") or []
    if not results:
        return None

    item = results[0]
    price_cents = item.get("sell_price")
    if price_cents is None:
        return None
    return item.get("name", query), price_cents / 100.0


async def _fetch_from_steam(session: aiohttp.ClientSession, key: str, name_or_query: str, is_exact: bool) -> tuple[str, float, bool]:
    """Возвращает (имя, цена, ok). ok=False -> рейт-лимит, не кэшировать как 0.0."""
    try:
        if is_exact:
            price = await _price_overview(session, name_or_query)
            if price is not None:
                return name_or_query, price, True
            found = await _search_price(session, name_or_query)
            return (found[0], found[1], True) if found else (name_or_query, 0.0, True)

        found = await _search_price(session, name_or_query)
        return (found[0], found[1], True) if found else (name_or_query, 0.0, True)

    except RateLimited:
        return name_or_query, 0.0, False


async def _fetch_one_price(session: aiohttp.ClientSession, key: str, overrides: dict, catalog: dict) -> tuple[str, float]:
    """
    Обратная совместимость со старой сигнатурой — её напрямую импортирует
    prewarm.py. Логика та же: csgotrader.app -> Steam, но без разделения на
    "не найдено" / "рейт-лимит" (тут по старому контракту всегда возвращаем
    число, 0.0 в худшем случае). Основной путь get_sticker_prices() эту
    функцию не использует — там своя, более аккуратная логика с ретраями.
    """
    name_or_query, is_exact = _resolve_market_hash_name(key, overrides, catalog)

    if is_exact:
        ct_prices = await get_csgotrader_prices(session)
        ct_price = ct_prices.get(name_or_query)
        if ct_price is not None:
            return name_or_query, ct_price

    if steam_cooldown_remaining(scope="pricing") > 0:
        return name_or_query, 0.0

    matched_name, price, _ok = await _fetch_from_steam(session, key, name_or_query, is_exact)
    return matched_name, price


# ---------------------------------------------------------------------------
# Основная точка входа — сигнатура не менялась, вызывающий код (в т.ч.
# обработка вручную присланного Steam-JSON в bot-16.handle_document) не трогаем.
# ---------------------------------------------------------------------------

async def get_sticker_prices(sticker_keys: set[str]) -> dict[str, float]:
    """
    На входе — множество ключей вида 'paris2023:sig_dupreeh_champion'.
    На выходе — {ключ: цена в USD}.
    Порядок поиска цены: кэш -> ручной прайс-лист -> csgotrader.app -> Steam
    (только для того, чего нет в csgotrader.app).
    Ключи, упёршиеся в Steam-рейт-лимит, не кэшируются нулём — подхватятся в следующем прогоне.
    """
    overrides = _load_overrides()
    catalog = await get_catalog()
    manual_prices = _load_manual_prices()

    now = time.time()
    result: dict[str, float] = {}
    to_fetch = []

    # Одним запросом (MGET) проверяем кэш сразу по всем ключам, вместо
    # отдельного HTTP-похода в Upstash на каждый стикер по очереди —
    # на лоте с десятками стикеров это раньше было главной причиной паузы.
    keys_list = list(sticker_keys)
    cached_map = await get_prices_batch(keys_list)
    for key in keys_list:
        cached = cached_map.get(key)
        if cached and (now - cached["updated_at"]) < CACHE_TTL_SECONDS:
            result[key] = cached["price"]
        else:
            to_fetch.append(key)

    if not to_fetch:
        return result

    # Ручной прайс-лист проверяем сразу, без сети — это настоящие Steam-цены,
    # присланные пользователем вручную (собраны с телефона, не банятся).
    still_to_fetch = []
    manual_hits: list[tuple[str, Optional[str], float, int]] = []
    for key in to_fetch:
        name_or_query, is_exact = _resolve_market_hash_name(key, overrides, catalog)
        if is_exact and name_or_query in manual_prices:
            price = manual_prices[name_or_query]
            manual_hits.append((key, name_or_query, price, CACHE_TTL_SECONDS))
            result[key] = price
        else:
            still_to_fetch.append(key)

    if manual_hits:
        await set_prices_batch(manual_hits)
    if manual_prices and len(still_to_fetch) < len(to_fetch):
        log.info("get_sticker_prices: %s ключей закрыто ручным прайс-листом", len(to_fetch) - len(still_to_fetch))

    to_fetch = still_to_fetch
    if not to_fetch:
        return result

    steam_fallback_needed = []
    rate_limited_count = 0

    # Своя сессия тут больше не поднимается: get_csgotrader_prices почти всегда
    # отвечает из памяти и в сеть не ходит вовсе, а когда ходит — берёт общую
    # сессию процесса. Раньше на каждый предмет вотчлиста заводилось новое
    # TLS-соединение, которое в 99 случаях из 100 не использовалось ни разу.
    ct_prices = await get_csgotrader_prices()
    if not ct_prices:
        log.warning("get_sticker_prices: прайс-лист csgotrader.app пуст/недоступен, все ключи уйдут сразу на Steam-фолбэк")

    ct_hits: list[tuple[str, Optional[str], float, int]] = []
    for key in to_fetch:
        name_or_query, is_exact = _resolve_market_hash_name(key, overrides, catalog)

        ct_price = ct_prices.get(name_or_query) if is_exact else None
        if ct_price is not None:
            ct_hits.append((key, name_or_query, ct_price, CACHE_TTL_SECONDS))
            result[key] = ct_price
        else:
            steam_fallback_needed.append((key, name_or_query, is_exact))

    if ct_hits:
        await set_prices_batch(ct_hits)

    if steam_fallback_needed:
        # Живой путь (/scan, /scanfile, автоскан вотчлиста) больше НЕ ходит в
        # Steam за ценой стикера вообще — только в статичный прайс-лист
        # csgotrader.app выше. Причина: 2026-08-14/15 дважды подряд (даже
        # после подъёма паузы до 12 сек и 6+ часов отдыха между попытками)
        # именно priceoverview/market-search — НЕ /render/, которым идёт сбор
        # листингов, — сразу ловил 429 на первом же обращении. У этих двух
        # эндпоинтов, похоже, отдельный и гораздо более жёсткий лимит у Steam,
        # чем у /render/, и никакая пауза между запросами его не обходит.
        # Ронять ВЕСЬ автоскан на 30+ минут ради пары стикеров, которых
        # почти наверняка нет в csgotrader.app просто потому что они очень
        # новые, того не стоит. Эти ключи остаются "не найдено" сейчас —
        # их фоном подтянет prewarm_loop (раз в 30 мин, тоже уважает
        # кулдаун — см. prewarm.py). Ничего не кэшируем нулём, чтобы при
        # следующем прогоне (когда prewarm их уже посчитает) взялась
        # актуальная цена.
        log.info(
            "get_sticker_prices: %s ключей не нашлись в прайс-листе csgotrader.app — "
            "оставляю их фоновому prewarm'у, живой путь к Steam за ценой стикера не ходит",
            len(steam_fallback_needed),
        )
        rate_limited_count += len(steam_fallback_needed)

    if rate_limited_count:
        log.info(
            "get_sticker_prices: %s ключей пока без цены (ждут prewarm)",
            rate_limited_count,
        )

    return result
