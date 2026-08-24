"""
Чтение инвентаря CS2 из Steam.

Эндпоинт публичный и не требует логина — при условии, что инвентарь открыт в
настройках приватности профиля:

    https://steamcommunity.com/inventory/<steamid64>/730/2?l=english&count=2000

Почему это дёшево, в отличие от вотчлиста. Там каждый предмет стоит отдельного
запроса к /render/, и весь темп скана упирается в паузы. Здесь ВЕСЬ инвентарь
приходит одним ответом (или несколькими страницами на очень больших), а цены
берутся из прайс-листа csgotrader, который уже лежит разобранным в памяти
процесса (см. pricing._read_json_memo) и сети не требует вовсе. То есть
проверка инвентаря на 500 предметов — это один сетевой запрос, а не пятьсот.

l=english обязателен: market_hash_name должен совпадать с ключами прайс-листа,
а на другом языке Steam отдаёт локализованные названия, которые ни с чем не
сойдутся.

Своя область кулдауна ("inventory"): у Steam лимиты считаются по эндпоинтам
независимо (см. steam_client.KNOWN_SCOPES), и бан на инвентаре не должен
останавливать сбор листингов. Обратное тоже верно, но общий бан по IP
по-прежнему придержит всех — этим занимается _apply_collateral_cooldown.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import aiohttp

from http_session import get_session
from steam_client import (
    SteamRateLimited,
    note_steam_429,
    note_steam_ok,
    raise_if_cooling_down,
    throttle_steam_request,
)

log = logging.getLogger("steam_bot.inventory")

APP_ID = 730
CONTEXT_ID = 2  # обычный инвентарь CS2

# Сколько предметов просить за раз. Steam режет запрос молча, если попросить
# слишком много, поэтому берём заведомо рабочее значение и листаем.
PAGE_SIZE = 2000
# Предохранитель от бесконечного листания, если Steam будет отдавать more_items
# без движения курсора. Инвентарей больше 20 000 предметов не бывает.
MAX_PAGES = 10

_STEAMID64_RE = re.compile(r"^7656\d{13}$")
_PROFILE_URL_RE = re.compile(r"steamcommunity\.com/profiles/(7656\d{13})")
_VANITY_URL_RE = re.compile(r"steamcommunity\.com/id/([A-Za-z0-9_.-]+)")
# steamid в HTML страницы профиля — так вычисляется vanity-адрес без ключа Web API.
_STEAMID_IN_HTML_RE = re.compile(r'"steamid"\s*:\s*"(7656\d{13})"')


class InventoryError(RuntimeError):
    """Инвентарь не прочитать: закрыт, пуст или профиль не найден."""


@dataclass
class InventoryItem:
    """Позиция инвентаря, свёрнутая по названию (одинаковые предметы сложены)."""

    market_hash_name: str
    count: int


async def resolve_steamid(raw: str) -> str:
    """
    Привести что угодно к steamid64: сам id, ссылку на /profiles/, ссылку на
    /id/<vanity> или голое vanity-имя.

    Vanity решается чтением страницы профиля, а не ResolveVanityURL — иначе
    понадобился бы ключ Steam Web API, то есть ещё одна переменная окружения и
    ещё один способ сломаться на ровном месте. В HTML профиля steamid лежит
    открыто.
    """
    raw = (raw or "").strip()
    if not raw:
        raise InventoryError("Пустой адрес профиля.")

    if _STEAMID64_RE.match(raw):
        return raw

    found = _PROFILE_URL_RE.search(raw)
    if found:
        return found.group(1)

    vanity_match = _VANITY_URL_RE.search(raw)
    vanity = vanity_match.group(1) if vanity_match else (raw if "/" not in raw else None)
    if not vanity:
        raise InventoryError(
            f"Не разобрал {raw!r}. Пришли ссылку на профиль или steamid64 "
            "(17 цифр, начинается с 7656)."
        )

    url = f"https://steamcommunity.com/id/{vanity}"
    await throttle_steam_request(scope="inventory")
    session = get_session()
    try:
        async with session.get(url, params={"xml": 1}) as resp:
            if resp.status == 429:
                seconds = await note_steam_429(scope="inventory", headers=dict(resp.headers))
                raise SteamRateLimited(
                    f"Steam ответил 429 при поиске профиля — запросы приостановлены "
                    f"на {seconds / 60:.0f} мин."
                )
            await note_steam_ok(scope="inventory")
            body = await resp.text()
    except aiohttp.ClientError as e:
        raise InventoryError(f"Не удалось открыть профиль: {e}") from None

    # xml=1 отдаёт <steamID64>...</steamID64>; на обычной странице тот же id
    # лежит в JS-блоке g_rgProfileData. Пробуем оба — формат Steam менял не раз.
    xml_id = re.search(r"<steamID64>(7656\d{13})</steamID64>", body)
    if xml_id:
        return xml_id.group(1)
    html_id = _STEAMID_IN_HTML_RE.search(body)
    if html_id:
        return html_id.group(1)

    raise InventoryError(
        f"Профиль {vanity!r} не найден (или скрыт). Проверь ссылку — проще всего "
        "прислать steamid64 из адреса вида /profiles/7656..."
    )


async def fetch_inventory(steamid: str) -> list[InventoryItem]:
    """
    Весь инвентарь CS2 указанного аккаунта, свёрнутый по market_hash_name.

    Возвращает только ПРОДАВАЕМЫЕ предметы (marketable=1): на непродаваемых
    рыночной цены не существует, следить за их ростом бессмысленно.
    """
    raise_if_cooling_down(scope="inventory")

    session = get_session()
    url = f"https://steamcommunity.com/inventory/{steamid}/{APP_ID}/{CONTEXT_ID}"

    counts: dict[str, int] = {}
    start_assetid: str | None = None
    total_assets = 0
    skipped_unmarketable = 0

    for page in range(MAX_PAGES):
        params: dict[str, str | int] = {"l": "english", "count": PAGE_SIZE}
        if start_assetid:
            params["start_assetid"] = start_assetid

        await throttle_steam_request(scope="inventory")
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 429:
                    seconds = await note_steam_429(scope="inventory", headers=dict(resp.headers))
                    raise SteamRateLimited(
                        f"Steam ответил 429 на инвентарь — это временный бан IP, "
                        f"запросы приостановлены на {seconds / 60:.0f} мин."
                    )
                if resp.status in (401, 403):
                    # Самая частая причина, и она не чинится ни ретраем, ни прокси.
                    raise InventoryError(
                        "Steam не отдаёт инвентарь (HTTP %s) — скорее всего он закрыт. "
                        "Открой: Steam → Профиль → Редактировать профиль → Настройки "
                        "приватности → «Инвентарь» = Открытый." % resp.status
                    )
                if resp.status == 404:
                    raise InventoryError(
                        f"Профиль {steamid} не найден. Проверь steamid64 — он из 17 цифр "
                        "и начинается с 7656."
                    )
                await note_steam_ok(scope="inventory")
                if resp.status != 200:
                    raise InventoryError(f"Steam ответил HTTP {resp.status} на запрос инвентаря.")
                data = await resp.json(content_type=None)
        except aiohttp.ClientError as e:
            raise InventoryError(f"Сетевая ошибка при запросе инвентаря: {e}") from None

        if not data or not data.get("success"):
            # success=0 приходит и на закрытый инвентарь, и на пустой — Steam их
            # не различает, поэтому и в тексте называем оба варианта.
            raise InventoryError(
                "Steam вернул отказ на инвентарь. Обычно это значит, что он закрыт "
                "настройками приватности, либо в нём нет предметов CS2."
            )

        assets = data.get("assets") or []
        descriptions = data.get("descriptions") or []
        total_assets += len(assets)

        # descriptions приходят отдельным списком, у одного описания может быть
        # много экземпляров в assets — связь по паре classid+instanceid.
        by_class: dict[tuple[str, str], dict] = {
            (str(d.get("classid")), str(d.get("instanceid"))): d for d in descriptions
        }
        for asset in assets:
            key = (str(asset.get("classid")), str(asset.get("instanceid")))
            description = by_class.get(key)
            if not description:
                continue
            name = description.get("market_hash_name")
            if not name:
                continue
            if not description.get("marketable"):
                skipped_unmarketable += 1
                continue
            try:
                amount = int(asset.get("amount", 1))
            except (TypeError, ValueError):
                amount = 1
            counts[name] = counts.get(name, 0) + amount

        if not data.get("more_items"):
            break
        next_cursor = data.get("last_assetid")
        if not next_cursor or next_cursor == start_assetid:
            # Курсор не двигается — дальше листать бессмысленно, иначе цикл
            # будет ходить по одной и той же странице до MAX_PAGES.
            log.warning("inventory: Steam просит листать дальше, но курсор не двигается — останавливаюсь")
            break
        start_assetid = str(next_cursor)
    else:
        log.warning("inventory: упёрся в MAX_PAGES=%d, часть инвентаря могла не попасть", MAX_PAGES)

    log.info(
        "inventory: %s — предметов всего %d, продаваемых уникальных %d (пропущено непродаваемых %d)",
        steamid, total_assets, len(counts), skipped_unmarketable,
    )
    return [InventoryItem(market_hash_name=name, count=count) for name, count in counts.items()]
