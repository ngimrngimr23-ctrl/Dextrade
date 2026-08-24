"""
Работа со Steam Community Market: получение всех листингов по предмету
и разбор цены + стикеров на каждом лоте.

Steam отдаёт данные через публичный (не требующий логина) эндпоинт:
    https://steamcommunity.com/market/listings/730/<market_hash_name>/render/
        ?query=&start=<N>&count=100&country=US&language=english&currency=1

За один запрос отдаёт максимум 100 лотов, поэтому листаем через start.

ЧТО НА САМОМ ДЕЛЕ ЧИНИЛО ПРОБЛЕМУ (не переписывать по памяти — история
разбирательства длинная и большинство "очевидных" гипотез оказались ложными).
Симптом был: /render/ отдаёт HTML-страницу сайта вместо JSON. Причина —
Steam включает бета-версию торговой площадки на аккаунт/сессию, и легаси-
эндпоинт для беты отвечает SSR-страницей. Лечится кукой bMarketOptOut=1
(она не секретная, не привязана к аккаунту и работает даже анонимно,
поэтому шлётся безусловно) плюс AJAX-заголовками штатного фронтенда Steam
и encoded=True на URL. Подробности — коммиты e8ae3fc и 8eaefce.

ВАЖНО: прокси эту проблему НЕ решал. Ни прокси, ни куки сессии, ни
браузерные заголовки, ни прогрев сессии не помогали, пока причина была
определена неверно. Прежняя версия этого докстринга утверждала, что
запросы идут через Cloudflare Worker, "потому что Steam банит датацентровые
IP" — это осталось от той ранней ложной гипотезы и вводило в заблуждение
(в т.ч. при разборе совсем другой задачи по CSFloat). Бан по IP у Steam
здесь подтверждён не был.

STEAM_PROXY_URL (Cloudflare Worker вида https://<имя>.<sub>.workers.dev,
интерфейс: GET /proxy?url=<целевой URL>) остаётся опциональным и по
умолчанию пустым — бот ходит в Steam напрямую. Воркер живой, но в рабочей
конфигурации не используется.

Опционально: куки реальной Steam-сессии (STEAM_LOGIN_SECURE и т.п., см.
steam_cookie_header()) — авторизованные запросы получают заметно более
мягкий рейт-лимит, чем анонимные (так делают открытые Steam-market боты
на GitHub, напр. woctezuma/steam-market). ВАЖНО: куки добавляются в
заголовок запроса, который уходит к STEAM_PROXY_URL, если он задан, — то
есть попадают к твоему Cloudflare Worker, а не напрямую в Steam. Чтобы
куки реально дошли до Steam, Worker должен сам переслать заголовок
Cookie на steamcommunity.com (код воркера не в этом репозитории).
"""

import asyncio
import logging
import os
import re
import time
import urllib.parse
from collections import deque
from dataclasses import dataclass, field

import aiohttp

from http_session import get_session
from proxy_pool import ProxyPool, mask as mask_proxy
import yarl

log = logging.getLogger("steam_bot.steam_client")

APP_ID = 730  # CS2 / CS:GO
RENDER_COUNT = 100  # максимум, который отдаёт Steam за раз
DEFAULT_MAX_LISTINGS = 100  # автосканы (/scanfile, вотчлист) смотрят столько самых дешёвых лотов = ровно 1 запрос на предмет

# --- Рейт-лимит Steam ---------------------------------------------------
# Изначально стояло 4 сек (по опыту сообщества), потом временно подняли до
# 12 — в логах 2026-08-14/15 казалось, что 4-й запрос подряд при 4 сек ловит
# 429. При более внимательном разборе оказалось: в ОБОИХ зафиксированных
# случаях 429 приходил именно на priceoverview/market-search (цена стикера),
# а /render/ (листинги — то, от чего зависит вотчлист) ни разу не подводил,
# даже при паузе всего 4 сек. Цена стикера теперь полностью изолирована от
# живого пути (см. pricing.get_sticker_prices) и имеет свой отдельный
# кулдаун (scope="pricing") — так что 12 сек для листингов были подстраховкой
# от несуществующей для них угрозы. Вернули обратно к 4.
# Ключевое: 429 у Steam это НЕ "подожди секунду", а временный бан IP (по
# отзывам — вплоть до нескольких часов), который ПРОДЛЕВАЕТСЯ каждой новой
# попыткой во время бана. Поэтому ретраить 429 нельзя — этим бот только
# закапывает себя глубже. Вместо ретраев: сразу сдаёмся и ставим кулдаун
# для соответствующей области, в течение которого её не трогаем вообще.
MIN_REQUEST_INTERVAL = 4.0  # секунд между ЛЮБЫМИ двумя запросами к Steam (глобально на весь процесс)

# Пауза для РУЧНОГО скана. Меньше фоновой намеренно: ручной запуск случается
# редко и человек ждёт ответа, тогда как фоновый молотит круглосуточно, и
# именно суммарная нагрузка за час доводит Steam до 429, а не промежуток между
# двумя соседними запросами. Разовый всплеск на пару минут он переносит,
# постоянный поток — нет.
MANUAL_REQUEST_INTERVAL = float(os.environ.get("MANUAL_REQUEST_INTERVAL", "2.0"))
COOLDOWN_AFTER_429_SECONDS = 30 * 60  # получили 429 -> не трогаем Steam столько времени
COOLDOWN_MAX_SECONDS = 6 * 60 * 60  # потолок при повторных 429 подряд (бан может быть длинным)

STEAM_PROXY_URL = os.environ.get("STEAM_PROXY_URL", "").rstrip("/")

# Пул резидентных прокси для Steam — ЗАПАСНОЙ маршрут, не основной.
#
# Живёт здесь, а не в pricing, по двум причинам. Во-первых, pricing импортирует
# steam_client, и обратный импорт замкнул бы круг. Во-вторых, по существу: пул
# нужен обоим потребителям — и ценам, и листингам, — а листинги живут тут.
#
# По умолчанию берётся список CSFloat: заводить вторую переменную ради того же
# набора адресов значит гарантированно однажды получить рассинхрон.
STEAM_POOL = ProxyPool(
    os.environ.get("STEAM_HTTP_PROXY") or os.environ.get("CSFLOAT_HTTP_PROXY", ""),
    name="steam",
)

# Сколько раз сменить адрес, прежде чем признать поражение на одном предмете.
STEAM_LISTINGS_RETRY = int(os.environ.get("STEAM_LISTINGS_RETRY", "3"))

# На сколько откладывать адрес, получивший 429 на листингах. Коротко: адреса
# резидентные и ротируемые (проверено /proxycheck), поэтому длинный кулдаун
# бьёт по своим — банится IP, а откладывается логин, за которым в следующий
# раз будет уже другой адрес.
LISTINGS_PROXY_COOLDOWN = int(os.environ.get("LISTINGS_PROXY_COOLDOWN", "60"))

# Куки реальной Steam-сессии — все опциональны, бот работает и без них
# (просто более анонимно и, судя по опыту, более подвержено рейт-лимитам).
# Достать: залогиниться в steamcommunity.com в браузере -> DevTools ->
# Application/Storage -> Cookies -> steamcommunity.com.
STEAM_LOGIN_SECURE = os.environ.get("STEAM_LOGIN_SECURE", "")
STEAM_SESSION_ID = os.environ.get("STEAM_SESSION_ID", "")
STEAM_BROWSER_ID = os.environ.get("STEAM_BROWSER_ID", "")


class SteamRateLimited(RuntimeError):
    """Steam ответил 429 либо мы сами на кулдауне после недавнего 429."""


# Кулдаун/счётчик 429 подряд — РАЗДЕЛЬНО по областям (scope), не один общий
# на весь процесс. Причина: 2026-08-14/15 дважды подряд (даже после подъёма
# паузы между запросами и часов отдыха) именно priceoverview/market-search
# (цена стикера, pricing.py) сразу ловил 429, тогда как /render/ (листинги,
# от которых зависит весь вотчлист/сканы) — ни разу. Похоже, у Steam это два
# независимых лимита, разной жёсткости. Раньше 429 на ЛЮБОМ из них ставил
# ОДИН общий кулдаун — бан на некритичной цене стикера останавливал заодно и
# критичный сбор листингов. Теперь у каждой области свой кулдаун:
#   "listings" — /render/ (steam_client.fetch_all_listings)
#   "pricing"  — priceoverview/market-search (pricing.py, только фоновый
#                prewarm.py их трогает — см. комментарий в pricing.py)
# throttle_steam_request() (пауза между запросами) — по-прежнему общий на обе
# области: пейсинг сам по себе не был причиной банов, разделять его незачем.
#
# cooldown_until — в epoch-секундах (time.time()), НЕ time.monotonic(): его
# значение сохраняется в постоянное хранилище (storage.py) и должно остаться
# осмысленным после рестарта процесса (Render передеплоивает бота на каждый
# git push, а monotonic() при рестарте обнуляется). Без этого при каждом
# рестарте бот "забывал" бы про ещё не снятый бан Steam, тут же пробовал
# снова и продлевал реальный бан. _last_request_at — просто пейсинг внутри
# одного запуска процесса, ему персистентность не нужна, оставляем monotonic.
_request_lock = asyncio.Lock()
_last_request_at = 0.0
# scope -> {"cooldown_until": float, "consecutive_429": int, "last_429_at": float}
_cooldowns: dict[str, dict] = {}

# Все области, которые ходят в Steam. Нужен явный список: при 429 в одной из них
# остальные надо придержать (Steam банит IP, а не эндпоинт — см. note_steam_429),
# а узнать о ещё не созданных состояниях из _cooldowns нельзя.
KNOWN_SCOPES = ("listings", "pricing", "inventory")

# Области, чей 429 НЕ придерживает остальные (см. _apply_collateral_cooldown).
#
# Общее правило — «бан у Steam по IP, а не по эндпоинту», и для листингов с
# priceoverview оно подтвердилось. Но /inventory/ из этого правила выпадает:
# он режется несопоставимо жёстче всего остального, и 429 там прилетает даже
# на первом запросе за час с датацентрового адреса. Считать это признаком
# общего бана по IP нельзя — иначе одна проверка инвентаря раз в час
# останавливала бы вотчлист и /markets на полчаса, чего и близко не требуется.
#
# Обратное направление сохраняется: 429 на листингах инвентарь придержит.
COLLATERAL_EXEMPT_SCOPES = ("inventory",)


def _cooldown_state(scope: str) -> dict:
    return _cooldowns.setdefault(
        scope, {"cooldown_until": 0.0, "consecutive_429": 0, "last_429_at": 0.0}
    )


# --- Диагностика рейт-лимита -------------------------------------------
# Кольцевой журнал моментов ВСЕХ запросов к Steam. Нужен, чтобы при 429 в лог
# попал не просто факт бана, а его "координаты": сколько запросов реально ушло
# за последнюю минуту / 5 / 15 / 60 мин и за сутки, отдельно по области и
# суммарно. Без этого каждый бан остаётся загадкой, а подбор паузы —
# угадыванием (чем мы и занимались 2026-08-14/15, меняя интервал 4 -> 12 -> 4
# по одиночным наблюдениям без данных). Лимиты Steam недокументированы и,
# судя по всему, не сводятся к "N секунд между запросами" — поэтому сначала
# собираем факты, потом решаем.
REQUEST_LOG_WINDOW_SECONDS = 24 * 60 * 60
_PERIODIC_STATS_EVERY = 200  # раз в столько запросов кидаем в лог текущий темп

_request_log: deque[tuple[float, str]] = deque()
_requests_since_stats = 0

_STAT_WINDOWS = (("1мин", 60), ("5мин", 300), ("15мин", 900), ("60мин", 3600), ("24ч", REQUEST_LOG_WINDOW_SECONDS))


def _record_steam_request(scope: str) -> None:
    global _requests_since_stats

    now = time.time()
    _request_log.append((now, scope))
    cutoff = now - REQUEST_LOG_WINDOW_SECONDS
    while _request_log and _request_log[0][0] < cutoff:
        _request_log.popleft()

    _requests_since_stats += 1
    if _requests_since_stats >= _PERIODIC_STATS_EVERY:
        _requests_since_stats = 0
        log.info("Темп запросов к Steam: %s", _format_counts(request_counts()))


def request_counts(scope: str | None = None) -> dict[str, int]:
    """Сколько запросов ушло за скользящие окна — всего либо только по одной области."""
    now = time.time()
    return {
        label: sum(
            1 for ts, sc in _request_log if ts >= now - seconds and (scope is None or sc == scope)
        )
        for label, seconds in _STAT_WINDOWS
    }


def _format_counts(counts: dict[str, int]) -> str:
    return ", ".join(f"{label}={n}" for label, n in counts.items())


def _log_429_diagnostics(scope: str, headers: dict | None) -> None:
    """
    Полный слепок обстоятельств бана — то, ради чего заведён журнал запросов.
    Логируем и заголовки ответа: Steam может прислать Retry-After или что-то
    из X-RateLimit-*, и это единственный источник, где лимит назван явно.
    """
    state = _cooldown_state(scope)
    last_429 = state.get("last_429_at") or 0.0
    since_last = f"{(time.time() - last_429) / 60:.1f} мин назад" if last_429 else "впервые с рестарта"

    log.warning(
        "ДИАГНОСТИКА 429 [%s]: запросов этой области — %s | всех областей — %s | "
        "прошлый 429 в этой области: %s | текущая пауза между запросами: %.0f сек",
        scope,
        _format_counts(request_counts(scope)),
        _format_counts(request_counts()),
        since_last,
        MIN_REQUEST_INTERVAL,
    )

    if headers:
        retry_after = next((v for k, v in headers.items() if k.lower() == "retry-after"), None)
        if retry_after:
            log.warning("ДИАГНОСТИКА 429 [%s]: Steam прислал Retry-After=%s", scope, retry_after)
        log.warning(
            "ДИАГНОСТИКА 429 [%s]: заголовки ответа: %s",
            scope, {k: v for k, v in headers.items()},
        )
    else:
        log.warning("ДИАГНОСТИКА 429 [%s]: заголовки ответа не переданы вызывающим кодом", scope)


def steam_cooldown_remaining(scope: str = "listings") -> float:
    """Сколько секунд ещё нельзя трогать Steam в этой области (0, если можно)."""
    return max(0.0, _cooldown_state(scope)["cooldown_until"] - time.time())


async def _persist_cooldown(scope: str) -> None:
    from storage import set_steam_cooldown  # локальный импорт — storage не обязателен для остального модуля

    state = _cooldown_state(scope)
    try:
        await set_steam_cooldown(scope, state["cooldown_until"], state["consecutive_429"])
    except Exception:
        log.exception("не смог сохранить кулдаун Steam (%s) в постоянное хранилище", scope)


async def load_persisted_cooldown() -> None:
    """
    Восстанавливает кулдаун/счётчик 429 подряд (по каждой области) после
    рестарта процесса. Вызывать один раз при старте бота, до первого
    обращения к Steam.
    """
    from storage import get_steam_cooldown

    for scope in KNOWN_SCOPES:
        try:
            persisted = await get_steam_cooldown(scope)
        except Exception:
            log.exception("не смог загрузить сохранённый кулдаун Steam (%s)", scope)
            continue
        if not persisted:
            continue
        state = _cooldown_state(scope)
        state["cooldown_until"] = persisted.get("cooldown_until", 0.0)
        state["consecutive_429"] = persisted.get("consecutive_429", 0)
        remaining = steam_cooldown_remaining(scope)
        if remaining > 0:
            log.warning(
                "Восстановлен кулдаун Steam (%s) после рестарта процесса: ещё %.0f мин (%s-й 429 подряд)",
                scope, remaining / 60, state["consecutive_429"],
            )


async def note_steam_429(scope: str = "listings", headers: dict | None = None) -> float:
    """
    Зафиксировать 429 в указанной области. Возвращает длину кулдауна для неё.
    headers — заголовки ответа Steam, попадают в диагностический слепок.

    ВАЖНО про области: разделение на "listings" и "pricing" — НАША выдумка,
    Steam её не соблюдает. Он банит IP целиком, независимо от того, на какой
    эндпоинт пришёл запрос. Раньше кулдаун ставился только своей области, и
    это было видно в логах как абсурд: prewarm заваливал pricing, Steam банил
    IP, а вотчлист следом делал СВОЙ ПЕРВЫЙ запрос за весь процесс и мгновенно
    получал 429 — диагностика показывала "запросов этой области 24ч=1". То есть
    listings не заработал бан, а унаследовал чужой, но всё равно наращивал себе
    счётчик и удваивал кулдаун, а сама попытка ещё и продлевала реальный бан.

    Поэтому остальным областям ставится "сопутствующий" кулдаун — базовый
    COOLDOWN_AFTER_429_SECONDS, БЕЗ роста и БЕЗ увеличения их счётчика: они ни
    в чём не виноваты, наказывать их эскалацией не за что, но и лезть на
    забаненный IP им нельзя.
    """
    state = _cooldown_state(scope)
    state["consecutive_429"] += 1
    seconds = min(COOLDOWN_AFTER_429_SECONDS * (2 ** (state["consecutive_429"] - 1)), COOLDOWN_MAX_SECONDS)
    log.warning(
        "Steam (%s) вернул 429 (%s-й подряд) — кулдаун на %.0f мин для этой области",
        scope, state["consecutive_429"], seconds / 60,
    )
    # Слепок снимаем ДО обновления last_429_at, чтобы в нём было видно, сколько
    # времени прошло с ПРОШЛОГО бана, а не ноль.
    _log_429_diagnostics(scope, headers)

    state["cooldown_until"] = max(state["cooldown_until"], time.time() + seconds)
    state["last_429_at"] = time.time()
    await _persist_cooldown(scope)

    await _apply_collateral_cooldown(scope)
    return seconds


async def _apply_collateral_cooldown(banned_scope: str) -> None:
    """
    Придержать остальные области после 429 — бан у Steam по IP, а не по эндпоинту.
    Счётчик 429 подряд им НЕ трогаем: он отражает "сколько раз эта область
    провинилась", а она не провинилась.

    Исключение — COLLATERAL_EXEMPT_SCOPES: у /inventory/ лимит настолько
    жёстче остальных, что его 429 ничего не говорит про доступность прочих
    эндпоинтов, и тормозить из-за него весь бот неправильно.
    """
    if banned_scope in COLLATERAL_EXEMPT_SCOPES:
        log.info(
            "Steam: 429 в области %s остальных не придерживает — у этого эндпоинта "
            "свой, гораздо более жёсткий лимит",
            banned_scope,
        )
        return

    until = time.time() + COOLDOWN_AFTER_429_SECONDS
    for other in KNOWN_SCOPES:
        if other == banned_scope:
            continue
        other_state = _cooldown_state(other)
        if other_state["cooldown_until"] >= until:
            continue  # там уже стоит кулдаун подлиннее — не укорачиваем
        other_state["cooldown_until"] = until
        log.warning(
            "Steam: область %s придержана на %.0f мин заодно с %s — бан у Steam по IP, "
            "а не по эндпоинту",
            other, COOLDOWN_AFTER_429_SECONDS / 60, banned_scope,
        )
        await _persist_cooldown(other)


async def note_steam_ok(scope: str = "listings") -> None:
    """Успешный ответ в указанной области — сбрасываем счётчик 429 подряд для неё."""
    state = _cooldown_state(scope)
    if state["consecutive_429"] != 0:
        state["consecutive_429"] = 0
        await _persist_cooldown(scope)


def blocking_cooldown(scope: str = "listings") -> float:
    """
    Сколько ещё ждать, если ждать действительно нужно. 0 — можно работать.

    Отличается от steam_cooldown_remaining тем, что учитывает пул: кулдаун
    ставится на ПРЯМОЙ адрес, а при свободном прокси запрос уйдёт с него, и
    отменять из-за этого весь прогон незачем.

    Без этой разницы получалась нелепость: мы научили скан переходить на прокси
    при 429, но проверка стояла ДО запроса и отменяла прогон целиком — бот
    писал «Steam на кулдауне, прогоны будут пропускаться», имея наготове семь
    рабочих адресов.
    """
    remaining = steam_cooldown_remaining(scope)
    if remaining <= 0:
        return 0.0
    if STEAM_POOL.available():
        return 0.0
    return remaining


def raise_if_cooling_down(scope: str = "listings") -> None:
    remaining = blocking_cooldown(scope)
    if remaining > 0:
        raise SteamRateLimited(
            f"Steam недавно ответил 429 (это временный бан IP, который продлевается "
            f"от новых попыток), поэтому запросы к нему приостановлены ещё на "
            f"{remaining / 60:.0f} мин. Попробуй после этого."
        )


_lane_locks: dict[str, asyncio.Lock] = {}
_lane_last: dict[str, float] = {}

# Сколько секунд суммарно простояли в паузе троттлинга с прошлого замера.
# Нужно, чтобы в итогах прогона отличать «ждали Steam по нашему же расписанию»
# от «Steam долго отвечал» — без этого разделения непонятно, что оптимизировать.
_throttle_wait_seconds = 0.0


def take_throttle_wait() -> float:
    """Суммарная пауза троттлинга с прошлого вызова, в секундах (счётчик обнуляется)."""
    global _throttle_wait_seconds
    value = _throttle_wait_seconds
    _throttle_wait_seconds = 0.0
    return value


async def throttle_steam_request(
    scope: str = "listings", lane: str = "", interval: float | None = None
) -> None:
    """
    Держит паузу MIN_REQUEST_INTERVAL между запросами к Steam и попутно
    отмечает запрос в журнале (см. _record_steam_request) — через эту функцию
    проходит КАЖДЫЙ запрос к Steam, так что журнал полон по построению и не
    зависит от того, не забыл ли вызывающий код что-то учесть.

    lane — по какому исходящему адресу считать паузу. Пусто (по умолчанию) —
    общая очередь на весь процесс, как было всегда.

    Зачем разделять: Steam банит ПО АДРЕСУ, а не по аккаунту в целом. Пока
    очередь была одна на процесс, пул прокси не давал никакого выигрыша в
    скорости — сколько бы адресов ни было, запросы всё равно выстраивались
    друг за другом по 4 секунды. С поадресной паузой N адресов дают N полос.

    ВАЖНО для конвейера (bot._run_watchlist_scan): пауза отсчитывается от
    момента ОТПРАВКИ предыдущего запроса, а не от получения ответа — отметка
    _lane_last ставится здесь, до того как вызывающий код уйдёт в session.get.
    Поэтому несколько предметов можно обрабатывать параллельно, и Steam всё
    равно увидит ровно один запрос в interval секунд: лишние просто подождут
    на этом локе. Параллельность убирает простой (разбор ответа, походы в
    Upstash), но НЕ повышает нагрузку на Steam.
    """
    global _last_request_at, _throttle_wait_seconds
    lock = _lane_locks.get(lane)
    if lock is None:
        lock = _lane_locks[lane] = asyncio.Lock()
    async with lock:
        gap = MIN_REQUEST_INTERVAL if interval is None else interval
        wait = gap - (time.monotonic() - _lane_last.get(lane, 0.0))
        if wait > 0:
            _throttle_wait_seconds += wait
            await asyncio.sleep(wait)
        now = time.monotonic()
        _lane_last[lane] = now
        if not lane:
            _last_request_at = now
        _record_steam_request(scope)


def steam_cookie_header() -> str:
    """
    Собирает Cookie-заголовок для запросов к Steam.

    bMarketOptOut=1 — не секретная, не привязанная к аккаунту кука "выход из
    бета-теста торговой площадки" (проверено вручную: без неё легаси-эндпоинт
    /render/ отдаёт HTML новой торговой площадки вместо JSON, даже у анонимной
    сессии без логина). Шлём её всегда, безусловно.
    """
    parts = ["bMarketOptOut=1"]
    if STEAM_LOGIN_SECURE:
        parts.append(f"steamLoginSecure={STEAM_LOGIN_SECURE}")
    if STEAM_SESSION_ID:
        parts.append(f"sessionid={STEAM_SESSION_ID}")
    if STEAM_BROWSER_ID:
        parts.append(f"browserid={STEAM_BROWSER_ID}")
    return "; ".join(parts)


@dataclass
class Listing:
    price: float
    stickers: list[str] = field(default_factory=list)  # человекочитаемые имена стикеров
    inspect_link: str | None = None  # steam://...csgo_econ_action_preview... — конкретно этот экземпляр


def market_hash_name_from_url(url: str) -> str:
    """
    Из ссылки на лот/список лотов вида
    https://steamcommunity.com/market/listings/730/AK-47%20%7C%20Slate%20%28Field-Tested%29
    достаём market_hash_name в исходном (не url-encoded) виде.
    """
    parsed = urllib.parse.urlparse(url)
    parts = parsed.path.rstrip("/").split("/")
    # .../market/listings/730/<n>
    name_encoded = parts[-1]
    return urllib.parse.unquote(name_encoded)


def render_url(market_hash_name: str, start: int) -> str:
    encoded = urllib.parse.quote(market_hash_name, safe="")
    return (
        f"https://steamcommunity.com/market/listings/{APP_ID}/{encoded}/render/"
        f"?query=&start={start}&count={RENDER_COUNT}&country=US&language=english&currency=1"
    )


def _build_request(steam_url: str) -> tuple[str, dict | None]:
    """
    Если задан STEAM_PROXY_URL — идём в Steam через Cloudflare Worker (не
    забаненный IP), передавая настоящий steam_url как query-параметр.
    Иначе (по умолчанию) — прямой запрос, как раньше.
    render_url() всегда возвращает исходную steam-ссылку — она же уходит в
    сообщения бота (open in browser и т.п.), прокси используется только
    для самого HTTP-запроса.

    ВАЖНО: steam_url НЕ кодируем вручную (urllib.parse.quote) — если
    передать его как уже закодированную строку внутри итогового URL,
    aiohttp/yarl при парсинге этой строки ЗАНОВО кодирует query-часть,
    что даёт двойное кодирование (%20 -> %2520 и т.п.) и ломает прокси.
    Вместо этого отдаём steam_url отдельным параметром через params= —
    так aiohttp кодирует его ровно один раз, корректно.
    """
    if not STEAM_PROXY_URL:
        # encoded=True — иначе yarl "нормализует" уже закодированный путь и
        # раскодирует %28/%29 обратно в (), т.е. в Steam уйдёт не байт-в-байт
        # тот же путь, что открывает браузер.
        return yarl.URL(steam_url, encoded=True), None
    return f"{STEAM_PROXY_URL}/proxy", {"url": steam_url}


# Регэксп ищет цену первого блока (это как раз "их" цена продажи с учётом комиссии)
PRICE_RE = re.compile(r'\$([\d,]+\.\d+)')

# Стикеры зашиты в src картинки: .../stickers/<collection>/<code>.<hash>.png
STICKER_RE = re.compile(r'stickers/([^/]+)/([a-zA-Z0-9_\-]+)\.[a-f0-9]{20,}\.png')

# Инспект-ссылка конкретного экземпляра предмета (float/паттерн/стикеры именно
# этого лота). Это НЕ ссылка на покупку — Steam не даёт публичных ссылок на
# покупку конкретного лота — но это единственный способ однозначно привязать
# ссылку к конкретному офферу, а не к предмету вообще.
INSPECT_RE = re.compile(r'''href=["'](steam://[^"']*csgo_econ_action_preview[^"']*)["']''')


def _sticker_code_to_display(collection: str, code: str) -> str:
    """
    Грубое человекочитаемое представление кода стикера для дальнейшего поиска цены.
    Это НЕ точное имя в Steam Market (Steam использует "Sticker | Name (Finish) | Event"),
    поэтому в pricing.py делаем поиск по нескольким вариантам названия.
    Здесь просто нормализуем то, что можем достать из имени файла.
    """
    return f"{collection}:{code}"


def item_page_url(market_hash_name: str) -> str:
    """Обычная (не /render/) страница предмета на Steam Market."""
    return f"https://steamcommunity.com/market/listings/{APP_ID}/{urllib.parse.quote(market_hash_name, safe='')}"


_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)
_UA_HINTS = {
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}


def _ajax_headers(market_hash_name: str) -> dict:
    """
    Заголовки AJAX-запроса к легаси-эндпоинту /render/ — именно так его дёргает
    штатный фронтенд Steam Market (он написан на Prototype.js, отсюда
    X-Prototype-Version и X-Requested-With).

    Прошлая версия просила 'Accept: text/html,...' — то есть буквально сама
    просила у Steam HTML вместо JSON; это было ошибкой, здесь исправлено.
    """
    return {
        "User-Agent": _UA,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "X-Prototype-Version": "1.7",
        "Referer": item_page_url(market_hash_name),
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        **_UA_HINTS,
    }


# Признаки того, что Steam отдал страницу НОВОЙ (бета) торговой площадки вместо
# JSON легаси-эндпоинта. Бета включается на аккаунт/сессию, и для неё /render/
# перестаёт быть JSON-эндпоинтом — никакими заголовками/IP/прокси это не лечится,
# нужно выйти из бета-теста на том аккаунте, чьи куки использует бот.
_BETA_PAGE_MARKERS = ("/ssr/", "DesktopUI", "<!DOCTYPE html")


async def fetch_all_listings(
    market_hash_name: str,
    max_listings: int | None = DEFAULT_MAX_LISTINGS,
    request_interval: float | None = None,
) -> list[Listing]:
    """
    Тянем страницы листингов для предмета и парсим цену + стикеры каждого лота.

    Steam отдаёт лоты отсортированными от дешёвых к дорогим, поэтому первые
    max_listings уже покрывают все реалистичные офферы — дальше идут лоты
    дороже, которые почти никогда не выгодны. max_listings=None — собрать
    вообще всё (может быть много страниц на популярных предметах).
    """
    listings: list[Listing] = []
    cookie = steam_cookie_header()

    def _with_cookie(headers: dict) -> dict:
        return {**headers, "Cookie": cookie} if cookie else headers

    log.info(
        "fetch_all_listings: предмет %r, прокси=%s, куки Steam-сессии=%s",
        market_hash_name, STEAM_PROXY_URL or "нет (прямой запрос)", "есть" if cookie else "нет",
    )

    raise_if_cooling_down()

    # Сессия общая на процесс (см. http_session), своя тут больше не поднимается.
    # Раньше на каждый предмет заводилось новое TCP+TLS соединение к
    # steamcommunity.com — два лишних round-trip'а перед каждым запросом, и
    # так 110 раз за прогон. Теперь соединение переиспользуется.
    session = get_session()
    # Прогревочного захода на страницу предмета здесь больше нет: он был
    # добавлен под гипотезу "первый запрос холодной сессии отдаёт HTML",
    # которая не подтвердилась (настоящей причиной был bMarketOptOut=1).
    # А лишний запрос на каждый предмет — прямой путь к 429.
    # route = None означает прямой запрос с адреса Render. На прокси
    # переходим ТОЛЬКО после 429 — прямой маршрут бесплатен, и тратить
    # платный трафик, пока он работает, незачем.
    # Если прямой адрес уже на кулдауне — начинаем сразу с прокси, а не
    # пробуем его «на всякий случай». Каждый такой запрос гарантированно
    # получит 429 и ПРОДЛИТ реальный бан, то есть навредит дважды: и время
    # потеряем, и выход из бана отодвинем.
    route = None
    if steam_cooldown_remaining("listings") > 0:
        route = STEAM_POOL.next()
        if route:
            log.info(
                "fetch_all_listings: прямой адрес на кулдауне — начинаю сразу с %s",
                mask_proxy(route),
            )
    attempts_left = STEAM_LISTINGS_RETRY
    start = 0
    total_count = None
    while total_count is None or start < total_count:
        if max_listings is not None and start >= max_listings:
            break
        steam_url = render_url(market_hash_name, start)
        final_url, params = _build_request(steam_url)
        # Через прокси куку аккаунта НЕ шлём: steamLoginSecure привязан к
        # сессии, и один логин, гуляющий по резидентным адресам, выглядит
        # как кража сессии. Публичному /render/ логин не нужен, достаточно
        # bMarketOptOut=1 (см. steam_cookie_header).
        headers = (
            _ajax_headers(market_hash_name)
            if route
            else _with_cookie(_ajax_headers(market_hash_name))
        )
        if route:
            headers = {**headers, "Cookie": "bMarketOptOut=1"}

        await throttle_steam_request(scope="listings", lane=route or "", interval=request_interval)
        resp_ctx = session.get(final_url, params=params, headers=headers, proxy=route)
        try:
            resp = await resp_ctx.__aenter__()
        except aiohttp.ClientError as e:
            # Прокси не смог даже соединиться (или сам отверг CONNECT — вот
            # ровно так и выглядел разбор 2026-08-22: ClientHttpProxyError 403
            # от flameproxies, когда протухли/заблокировали разом ВСЕ его
            # сессии). Это НЕ ответ Steam, а поломка транспорта, но раньше сюда
            # не было отдельной ветки — исключение улетало наверх, ловилось
            # общим except Exception в _watchlist_scan_item, и предмет просто
            # тихо пропускался. Из-за этого следующий предмет каждый раз
            # начинал заново с прямого адреса, СНОВА ловил 429 у Steam и
            # СНОВА продлевал реальный бан — а формального SteamRateLimited
            # (и связанного с ним кулдауна) никогда не возникало, потому что
            # STEAM_POOL.next() исправно находил "свободный" (на деле мёртвый)
            # прокси и код был уверен, что альтернативы ещё остались.
            #
            # Теперь ведём себя как при 429: пробуем следующий адрес, а если
            # адресов не осталось — сдаёмся организованно.
            if route:
                status = getattr(e, "status", None)
                if status == 403:
                    # 403 — это отказ авторизации самого прокси-сервиса
                    # (просрочка/бан аккаунта), а не разовая сетевая заминка.
                    # Повторные попытки на этот же адрес ничего не изменят.
                    STEAM_POOL.mark_refused(route, LISTINGS_PROXY_COOLDOWN, f"HTTP 403: {e}")
                else:
                    STEAM_POOL.mark_exhausted(route, LISTINGS_PROXY_COOLDOWN, f"ошибка соединения: {e}")
            next_route = STEAM_POOL.next() if attempts_left > 0 else None
            if next_route:
                attempts_left -= 1
                log.warning(
                    "fetch_all_listings: маршрут %s не работает (%s) — перехожу на %s "
                    "и повторяю ту же страницу",
                    mask_proxy(route) if route else "прямой", e, mask_proxy(next_route),
                )
                route = next_route
                continue

            seconds = await note_steam_429(scope="listings", headers={})
            raise SteamRateLimited(
                f"Прокси не пропускают запрос: {STEAM_POOL.failure_hint()}.\n"
                f"Последний ответ: {e}\n"
                f"Запросы к Steam приостановлены на {seconds / 60:.0f} мин, чтобы не "
                f"долбить прямой адрес и не продлевать его бан."
            )

        # Дальше — та же логика, что раньше жила под "async with session.get(...)
        # as resp:". Теперь вход в контекст-менеджер и обработку ошибки
        # соединения пришлось развести руками (чтобы поймать именно сбой
        # СОЕДИНЕНИЯ, а не ответ Steam), поэтому выход из него — тоже руками,
        # но так же надёжно: finally срабатывает и на "continue" (смена
        # маршрута), и на "raise" (сдаёмся), и при обычном успехе.
        try:
            if resp.status == 429:
                # Повторять с ТОГО ЖЕ адреса нельзя: у Steam 429 — это
                # временный бан IP, который каждая новая попытка продлевает.
                # А вот сменить адрес — можно и нужно: бан поадресный, и на
                # другом IP лимит свой.
                #
                # Раньше этой ветки не было вовсе: 429 обрывал весь скан и
                # ставил кулдаун на 30 минут с удвоением до шести часов,
                # хотя в пуле стояли свободные адреса.
                if route:
                    STEAM_POOL.mark_exhausted(route, LISTINGS_PROXY_COOLDOWN, "Steam 429")
                next_route = STEAM_POOL.next() if attempts_left > 0 else None
                if next_route:
                    attempts_left -= 1
                    log.warning(
                        "fetch_all_listings: 429 с маршрута %s — перехожу на %s "
                        "и повторяю ту же страницу",
                        mask_proxy(route) if route else "прямого",
                        mask_proxy(next_route),
                    )
                    route = next_route
                    continue

                # Свободных адресов не осталось — только теперь сдаёмся.
                seconds = await note_steam_429(scope="listings", headers=dict(resp.headers))
                raise SteamRateLimited(
                    f"Steam ответил 429 (Too Many Requests) — это временный бан IP, "
                    f"который продлевается от новых попыток, поэтому не ретраю. "
                    f"Свободных прокси нет ({STEAM_POOL.describe()}). "
                    f"Запросы к Steam приостановлены на {seconds / 60:.0f} мин."
                )
            await note_steam_ok()
            STEAM_POOL.mark_ok(route)
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            log.info(
                "fetch_all_listings: start=%s -> HTTP %s, Content-Type=%r, итоговый URL=%s",
                start, resp.status, content_type, resp.url,
            )
            if "application/json" not in content_type:
                body = await resp.text()
                snippet = body[:300].replace("\n", " ").strip()
                if any(marker in body[:2000] for marker in _BETA_PAGE_MARKERS):
                    # Самый частый и неочевидный случай: аккаунт, чьи куки
                    # использует бот, включён в бета-тест новой торговой
                    # площадки. Для беты /render/ перестаёт быть JSON-API и
                    # отдаёт SSR-страницу — ни заголовки, ни прокси, ни смена
                    # IP это не исправят, помогает только выход из бета-теста.
                    raise RuntimeError(
                        "Steam отдал HTML-страницу новой (бета) торговой площадки вместо JSON. "
                        "Похоже, аккаунт, чьи куки заданы в STEAM_LOGIN_SECURE, включён в бета-тест "
                        "Steam Market — для беты легаси-эндпоинт /render/ больше не отдаёт JSON. "
                        "Зайди в браузере под ЭТИМ аккаунтом, нажми «Выйти из бета-теста торговой "
                        "площадки», заново экспортируй куки и обнови их на Render. "
                        f"Начало ответа: {snippet!r}"
                    )
                raise RuntimeError(
                    f"Steam вернул не JSON, а {content_type!r}. "
                    f"Начало ответа: {snippet!r}"
                )

            data = await resp.json()
        finally:
            await resp_ctx.__aexit__(None, None, None)

        total_count = data.get("total_count", 0)
        html = data.get("results_html", "")
        listings.extend(_parse_listings_html(html))

        start += RENDER_COUNT
        # паузы между страницами держит throttle_steam_request() выше

    return listings


def _parse_listings_html(html: str) -> list[Listing]:
    """
    results_html — это HTML-фрагмент со всеми строками таблицы листингов.
    Режем по границам строк и вытаскиваем цену + стикеры каждой отдельно,
    чтобы стикеры одного лота не утекали в соседний.
    """
    blocks = re.split(r'class="market_listing_row market_recent_listing_row', html)[1:]
    out = []
    for block in blocks:
        # обрезаем на случай хвостового мусора/следующего листинга
        cut = len(block)
        for marker in ('"assets":{', '"listinginfo":{'):
            idx = block.find(marker)
            if idx != -1:
                cut = min(cut, idx)
        b = block[:cut]

        price_m = PRICE_RE.search(b)
        if not price_m:
            continue
        price = float(price_m.group(1).replace(",", ""))

        stickers = [
            _sticker_code_to_display(coll, code)
            for coll, code in STICKER_RE.findall(b)
        ][:5]  # максимум 5 слотов на оружии

        inspect_m = INSPECT_RE.search(b)
        inspect_link = inspect_m.group(1) if inspect_m else None

        out.append(Listing(price=price, stickers=stickers, inspect_link=inspect_link))

    return out

