"""
Настоящие ордера на покупку CSFloat: поставить, держать первыми, заметить исполнение.

Три части, и у каждой своё дело:
  buy_orders      — считает цену (без сети, проверяется целиком на столе);
  csfloat_client  — ходит в CSFloat;
  order_desk      — здесь: решения, которые связывают первое со вторым деньгами.

ЧТО ЗДЕСЬ РЕШАЕТСЯ.

1. Постановка. Сколько всего денег бот держит в своих ордерах (бюджет), сколько
   ордеров разом, не стоит ли уже ордер на этот предмет — руками или ботом,
   второй на те же деньги не нужен, — и хватит ли баланса CSFloat.

2. Поддержка. Ордер, поставленный и забытый, перестаёт работать за часы: кто-то
   встаёт на шаг выше, и продавцы уходят к нему. Поэтому раз в несколько
   десятков минут каждый ботовый ордер сверяется со стаканом и переставляется
   по тому же правилу, что при постановке (buy_orders.reprice).

3. Исполнение. Исполненный ордер просто пропадает из списка — как и снятый на
   сайте. Отличает их список сделок: у исполненного там есть покупка.

КАК ЗАЩИЩЕНЫ ДЕНЬГИ.
  * Бот трогает только СВОИ ордера — те, что в реестре (storage.get_bot_orders).
    Ручные он показывает, но не переставляет и снимает только по прямой просьбе.
  * Перестановка — сначала отмена, потом новый ордер. Обратный порядок на миг
    держал бы два ордера на один предмет, и исполниться могли бы оба.
  * Постановка не повторяется вслепую: если CSFloat не ответил, сначала смотрим
    свой список ордеров (см. csfloat_client.CSFloatUnknownOutcome).
  * Без списка своих ордеров и без реестра не ставится ничего: иначе нельзя
    проверить ни бюджет, ни дубли.
  * Поддержка никогда не поднимает ордер выше потолка, посчитанного при
    постановке по живой цене Steam. Опустить потолок она может (Steam
    подешевел, человек поднял порог прибыли), поднять — только новый /orders.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time
from typing import NamedTuple

import aiohttp

import buy_orders
import csfloat_client
import envcfg
from csfloat_client import (
    CSFloatError,
    CSFloatOrderRejected,
    CSFloatRateLimited,
    CSFloatUnknownOutcome,
    MyBuyOrder,
)
from scan_errors import scrub
from storage import (
    StorageUnavailable,
    get_bot_orders,
    get_steam_prices_batch,
    save_bot_orders,
)

log = logging.getLogger("steam_bot.orders")

# Сколько всего денег бот держит в своих ордерах разом, если человек не задал
# иначе (/orderbudget). Это предохранитель, а не цель: поддержка поднимает цены
# сама, и без общего потолка десяток ордеров по $65 незаметно превращается в
# шестьсот пятьдесят долларов обязательств.
DEFAULT_BUDGET_USD = envcfg.env_float("ORDER_BUDGET_USD", 100.0)
DEFAULT_MAX_COUNT = envcfg.env_int("ORDER_MAX_COUNT", 10)

# Как часто сверять ботовые ордера со стаканом. Каждый ордер — запрос к
# CSFloat (стакан), перестановка — ещё два, а квота 200 в час на ключ общая
# с широким сканом. Полчаса на десяток ордеров — около двадцати запросов в
# час: заметно, но не больно.
DEFAULT_AUTO_MINUTES = envcfg.env_int("ORDER_AUTO_MINUTES", 30)
MIN_AUTO_MINUTES = 10

# Сколько ордеров проверять за один прогон поддержки. Остальные — в
# следующий: проверяются по очереди, начиная с давно не проверенных.
MAINTAIN_PER_ROUND = envcfg.env_int("ORDER_MAINTAIN_PER_ROUND", 12)

# Виды событий поддержки — по ним бот решает, о чём писать в чат.
FILLED = "filled"        # исполнен: в сделках есть покупка
GONE = "gone"            # пропал из списка, покупки не видно
RAISED = "raised"
LOWERED = "lowered"
CANCELLED = "cancelled"  # снят ботом: прибыли не осталось
OUTBID = "outbid"        # обошли, а ответить нельзя
FAILED = "failed"

_locks: dict[int, asyncio.Lock] = {}


def _lock(chat_id: int) -> asyncio.Lock:
    """
    Один замок на чат для всего, что меняет ордера.

    Постановка, поддержка и отмена читают реестр, ходят в CSFloat и пишут
    реестр обратно. Две такие операции разом затёрли бы запись друг друга,
    а две перестановки одного ордера — поставили бы его дважды.
    """
    lock = _locks.get(chat_id)
    if lock is None:
        lock = _locks[chat_id] = asyncio.Lock()
    return lock


def busy(chat_id: int) -> bool:
    return _lock(chat_id).locked()


class Ticket(NamedTuple):
    """Ордер, который человек велел поставить. Цены в центах."""

    market_hash_name: str
    price_cents: int
    ceiling_cents: int
    steam_price_cents: int
    profit_pct: float
    listing_id: str | None
    source: str
    instant: bool = False


class PlaceOutcome(NamedTuple):
    placed: list[tuple[Ticket, MyBuyOrder]]
    # Ушли в CSFloat и исполнились тут же: ордера в списке нет, а значит,
    # лот куплен (см. buy_orders.OrderPlan.instant).
    filled_now: list[Ticket]
    refused: list[tuple[Ticket, str]]
    stopped: str | None       # почему остальные не ставили
    committed_cents: int      # сколько теперь в ботовых ордерах
    count: int                # сколько ботовых ордеров
    balance_cents: int | None


class Event(NamedTuple):
    kind: str
    name: str
    text: str


class Round(NamedTuple):
    events: list[Event]
    checked: int
    blind: int                # стакан не прочитан — ордер не трогали
    active: int               # ботовых ордеров после прогона
    committed_cents: int
    problem: str | None       # прогон не состоялся — почему


class CancelOutcome(NamedTuple):
    cancelled: list[MyBuyOrder]
    failed: list[tuple[MyBuyOrder, str]]
    problem: str | None


class Snapshot(NamedTuple):
    orders: list[MyBuyOrder]
    registry: dict[str, dict] | None   # None — реестр не прочитан
    balance_cents: int | None


def _session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession()


def _reason(e: BaseException) -> str:
    return scrub(str(e)) or type(e).__name__


def _parse_time(raw) -> float | None:
    """ISO-время из ответа CSFloat в epoch-секунды. None — не разобрали."""
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return dt.datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _entry(ticket: Ticket, order: MyBuyOrder, now: float) -> dict:
    """Запись реестра о только что поставленном ордере."""
    return {
        "name": ticket.market_hash_name,
        "price": order.price_cents,
        "qty": order.qty,
        # Потолок и цена Steam — с момента постановки, когда цена Steam была
        # проверена живым запросом. Выше этого потолка поддержка не поднимет.
        "ceiling": ticket.ceiling_cents,
        "steam": ticket.steam_price_cents,
        "listing_id": ticket.listing_id,
        "source": ticket.source,
        "placed_at": now,
        "checked_at": now,
        "reprices": 0,
        "outbid_noted": False,
    }


async def _find_new_order(
    session: aiohttp.ClientSession, name: str, price_cents: int, known: set[str],
) -> MyBuyOrder | None:
    """
    Найти только что поставленный ордер в списке своих.

    Нужна, когда CSFloat не ответил (ордер мог встать) или ответил успехом без
    тела ордера. Ищем новый id с тем же именем и ценой — старые ордера в known.
    """
    try:
        current = await csfloat_client.my_buy_orders(session)
    except (CSFloatError, CSFloatRateLimited) as e:
        log.warning("orders: не смог перечитать свои ордера — %s", _reason(e))
        return None
    for order in current:
        if order.order_id in known:
            continue
        if order.market_hash_name == name and order.price_cents == price_cents:
            return order
    return None


async def place(
    chat_id: int, tickets: list[Ticket], *, budget_cents: int, max_count: int,
) -> PlaceOutcome:
    """
    Поставить ордера по очереди, пока хватает бюджета и лимита.

    Порядок tickets — порядок важности: что не влезло в бюджет, то и не
    поставлено. Каждый поставленный ордер записывается в реестр СРАЗУ, а не
    в конце: упади процесс на середине — уже стоящие ордера останутся
    известными боту, а не повиснут ничьими.
    """
    placed: list[tuple[Ticket, MyBuyOrder]] = []
    filled_now: list[Ticket] = []
    refused: list[tuple[Ticket, str]] = []

    async with _lock(chat_id):
        try:
            registry = await get_bot_orders(chat_id)
        except StorageUnavailable as e:
            return PlaceOutcome(
                [], [], [], f"реестр ботовых ордеров не прочитан ({_reason(e)}) — "
                "не ставлю: без него не проверить бюджет", 0, 0, None,
            )

        async with _session() as session:
            try:
                current = await csfloat_client.my_buy_orders(session)
            except (CSFloatError, CSFloatRateLimited) as e:
                return PlaceOutcome(
                    [], [], [], f"не смог прочитать свои ордера на CSFloat "
                    f"({_reason(e)}) — без этого не ставлю: можно задвоить ордер",
                    0, 0, None,
                )
            balance = await csfloat_client.account_balance(session)

            live = {o.order_id: o for o in current}
            mine = [live[oid] for oid in registry if oid in live]
            committed = sum(o.price_cents * o.qty for o in mine)
            count = len(mine)
            taken = {o.market_hash_name for o in current if o.market_hash_name}
            known = set(live)
            stopped: str | None = None

            for t in tickets:
                if t.market_hash_name in taken:
                    refused.append((t, "на этот предмет ордер уже стоит"))
                    continue
                if count >= max_count:
                    refused.append((t, f"уже {count} ботовых ордер(ов) — это лимит"))
                    continue
                if committed + t.price_cents > budget_cents:
                    refused.append((
                        t, f"не влезает в бюджет: занято ${committed / 100:.2f} "
                           f"из ${budget_cents / 100:.2f}",
                    ))
                    continue
                if balance is not None and t.price_cents > balance:
                    refused.append((
                        t, f"дороже доступного баланса CSFloat (${balance / 100:.2f})",
                    ))
                    continue
                # Последняя проверка самой цены: деньги уходят следующей
                # строкой, а ошибка расчёта выше — не повод их потратить.
                if (not 1 <= t.price_cents <= t.ceiling_cents
                        or not buy_orders.is_valid_price(t.price_cents)):
                    log.error("orders: цена вне потолка или сетки: %s", t)
                    refused.append((t, "цена вне потолка или сетки — ошибка расчёта, не ставлю"))
                    continue

                try:
                    order = await csfloat_client.create_buy_order(
                        session, t.market_hash_name, t.price_cents,
                    )
                except CSFloatOrderRejected as e:
                    refused.append((t, f"CSFloat отказал: {_reason(e)}"))
                    continue
                except CSFloatUnknownOutcome as e:
                    log.warning("orders: %s — проверяю список", _reason(e))
                    order = await _find_new_order(session, t.market_hash_name, t.price_cents, known)
                    if order is None:
                        refused.append((
                            t, "CSFloat не ответил, и в списке ордера нет — "
                               "проверь /myorders",
                        ))
                        continue
                except CSFloatRateLimited as e:
                    stopped = _reason(e)
                    break
                except CSFloatError as e:
                    refused.append((t, _reason(e)))
                    continue

                if order is None:
                    order = await _find_new_order(session, t.market_hash_name, t.price_cents, known)
                if order is None:
                    if t.instant:
                        # Ордер дотягивал до витрины — CSFloat купил лот в тот
                        # же момент, и ордера в списке уже нет.
                        filled_now.append(t)
                        taken.add(t.market_hash_name)
                        log.info(
                            "orders: chat_id=%s ордер на «%s» за $%.2f исполнился сразу",
                            chat_id, t.market_hash_name, t.price_cents / 100,
                        )
                    else:
                        refused.append((
                            t, "CSFloat ответил успехом, но в списке ордера не "
                               "видно — проверь /myorders",
                        ))
                    continue

                now = time.time()
                registry[order.order_id] = _entry(t, order, now)
                known.add(order.order_id)
                taken.add(t.market_hash_name)
                committed += order.price_cents * order.qty
                count += 1
                placed.append((t, order))
                log.info(
                    "orders: chat_id=%s поставлен ордер %s на «%s» за $%.2f "
                    "(потолок $%.2f, Steam $%.2f, %s)",
                    chat_id, order.order_id, t.market_hash_name,
                    order.price_cents / 100, t.ceiling_cents / 100,
                    t.steam_price_cents / 100, t.source,
                )
                try:
                    await save_bot_orders(chat_id, registry)
                except StorageUnavailable as e:
                    stopped = (
                        f"ордер на «{t.market_hash_name}» поставлен, но в реестр "
                        f"не записан ({_reason(e)}) — бот будет считать его "
                        "ручным. Остальные не ставлю"
                    )
                    break

    return PlaceOutcome(placed, filled_now, refused, stopped, committed, count, balance)


def _ceiling_now(
    entry: dict, cached: dict | None, *, min_profit_pct: float, max_order_cents: int,
) -> int:
    """
    Потолок ордера на сейчас: не выше того, что был при постановке.

    Опускают его две вещи: свежая цена Steam ниже прежней и пороги, которые
    человек поменял после постановки (прибыль, лимит на ордер). Поднять его
    не может ничто — для этого нужна новая живая проверка, то есть /orders.
    """
    steam = int(entry.get("steam") or 0)
    if cached and cached.get("price") and cached.get("updated_at", 0) > entry.get("placed_at", 0):
        steam = int(round(float(cached["price"]) * 100))
    stored = int(entry.get("ceiling") or 0)
    # Без цены Steam пересчитать потолок не из чего — остаётся прежний, а не
    # ноль: ноль значил бы «прибыли нет» и снял бы живой ордер.
    by_profit = buy_orders.ceiling_for(steam, min_profit_pct) if steam > 0 else stored
    return min(stored, by_profit, int(max_order_cents))


def _rivals(book: list, order_id: str, own_price: int) -> list[int]:
    """
    Чужие простые ордера из стакана: свой вычёркиваем, условные — тоже.

    Если сервер отдаёт id ордеров, свой узнаём по id. Если нет — вычёркиваем
    одну запись по нашей цене: это мы. Вторая запись с той же ценой, если
    есть, — соперник вровень, и она остаётся.
    """
    simple = [b for b in book if not b.conditional]
    if any(b.order_id for b in simple):
        return [b.price_cents for b in simple if b.order_id != order_id]
    prices = [b.price_cents for b in simple]
    if own_price in prices:
        prices.remove(own_price)
    return prices


async def maintain(
    chat_id: int,
    *,
    min_profit_pct: float,
    max_order_cents: int,
    budget_cents: int,
    limit: int = MAINTAIN_PER_ROUND,
) -> Round:
    """
    Один прогон поддержки: заметить исполненные, переставить обойдённые.

    Ордера проверяются по очереди, начиная с давно не проверенных, не больше
    limit за раз — стакан каждого стоит запроса из общей квоты ключа.
    """
    events: list[Event] = []
    checked = blind = 0

    async with _lock(chat_id):
        try:
            registry = await get_bot_orders(chat_id)
        except StorageUnavailable as e:
            return Round([], 0, 0, 0, 0, f"реестр не прочитан: {_reason(e)}")
        if not registry:
            return Round([], 0, 0, 0, 0, None)

        async with _session() as session:
            try:
                current = await csfloat_client.my_buy_orders(session)
            except (CSFloatError, CSFloatRateLimited) as e:
                return Round(
                    [], 0, 0, len(registry), 0,
                    f"свои ордера не прочитаны: {_reason(e)}",
                )
            live = {o.order_id: o for o in current}
            known = set(live)

            # --- Пропавшие: исполнены или сняты на сайте ------------------
            vanished = [oid for oid in registry if oid not in live]
            if vanished:
                purchases = await csfloat_client.my_recent_purchases(session)
                used: set[str | None] = set()
                for oid in vanished:
                    entry = registry.pop(oid)
                    name = entry.get("name") or "?"
                    match = None
                    for p in purchases or []:
                        if p.market_hash_name != name or p.trade_id in used:
                            continue
                        bought_at = _parse_time(p.created_at)
                        # Покупка раньше постановки — это не наш ордер, а
                        # прошлая сделка с тем же предметом.
                        if bought_at is not None and bought_at < entry.get("placed_at", 0) - 60:
                            continue
                        match = p
                        break
                    if match is not None:
                        used.add(match.trade_id)
                        price = match.price_cents or entry.get("price") or 0
                        events.append(Event(
                            FILLED, name,
                            f"куплен за ${price / 100:.2f}. Продавец пришлёт "
                            "трейд в Steam — его нужно принять",
                        ))
                    elif purchases is None:
                        events.append(Event(
                            GONE, name,
                            "пропал из списка ордеров: либо исполнился, либо "
                            "снят на сайте (сделки прочитать не вышло)",
                        ))
                    else:
                        events.append(Event(
                            GONE, name, "снят вне бота — покупки по нему нет",
                        ))
                    log.info("orders: chat_id=%s ордер %s на «%s» пропал — %s",
                             chat_id, oid, name, events[-1].kind)
                try:
                    await save_bot_orders(chat_id, registry)
                except StorageUnavailable as e:
                    return Round(events, 0, 0, len(registry), 0,
                                 f"реестр не записан: {_reason(e)}")

            committed = sum(
                live[oid].price_cents * live[oid].qty for oid in registry
            )
            names = sorted({e.get("name") for e in registry.values() if e.get("name")})
            try:
                cached_prices = await get_steam_prices_batch(names) if names else {}
            except Exception:  # noqa: BLE001 — без кэша потолок просто прежний
                log.warning("orders: кэш цен Steam не прочитан", exc_info=True)
                cached_prices = {}

            queue = sorted(registry.items(), key=lambda kv: kv[1].get("checked_at", 0))
            for oid, entry in queue[:limit]:
                order = live[oid]
                name = entry.get("name") or order.market_hash_name or "?"
                now = time.time()

                book = None
                listing_id = entry.get("listing_id")
                if listing_id:
                    book = await csfloat_client.order_book(session, listing_id)
                if book is None:
                    # Лот, по которому читали стакан, мог уйти — ищем другой
                    # по имени. Это /listings, то есть через прокси.
                    try:
                        lots, _ = await csfloat_client.fetch_listings_page(
                            session, limit=1, sort_by="lowest_price",
                            market_hash_name=name,
                        )
                    except (CSFloatError, CSFloatRateLimited) as e:
                        log.info("orders: лот для стакана «%s» не нашёл — %s",
                                 name, _reason(e))
                        lots = []
                    if lots:
                        listing_id = lots[0].listing_id
                        entry["listing_id"] = listing_id
                        book = await csfloat_client.order_book(session, listing_id)
                entry["checked_at"] = now
                checked += 1
                if book is None:
                    blind += 1
                    continue

                ceiling = _ceiling_now(
                    entry, cached_prices.get(name),
                    min_profit_pct=min_profit_pct, max_order_cents=max_order_cents,
                )
                decision = buy_orders.reprice(
                    current_cents=order.price_cents,
                    ceiling_cents=ceiling,
                    rivals_cents=_rivals(book, oid, order.price_cents),
                )
                log.info(
                    "orders: chat_id=%s «%s» $%.2f, потолок $%.2f — %s: %s",
                    chat_id, name, order.price_cents / 100, ceiling / 100,
                    decision.action, decision.why,
                )

                if decision.action == buy_orders.KEEP:
                    if decision.outbid and not entry.get("outbid_noted"):
                        events.append(Event(OUTBID, name, decision.why))
                    entry["outbid_noted"] = decision.outbid
                    continue

                if decision.action == buy_orders.RAISE:
                    after = committed - order.price_cents * order.qty + decision.price_cents * order.qty
                    if after > budget_cents:
                        if not entry.get("outbid_noted"):
                            events.append(Event(
                                OUTBID, name,
                                f"обошли; встать выше (${decision.price_cents / 100:.2f}) "
                                f"не даёт бюджет ${budget_cents / 100:.2f}",
                            ))
                        entry["outbid_noted"] = True
                        continue

                # Перестановка: сначала снять, потом поставить (см. докстринг
                # модуля — обратный порядок на миг держал бы два ордера).
                try:
                    removed = await csfloat_client.cancel_buy_order(session, oid)
                except (CSFloatError, CSFloatRateLimited) as e:
                    events.append(Event(
                        FAILED, name, f"не смог снять ордер для перестановки: {_reason(e)}",
                    ))
                    continue
                registry.pop(oid)
                committed -= order.price_cents * order.qty

                if not removed:
                    events.append(Event(
                        GONE, name,
                        "пропал в момент перестановки — похоже, исполнился; "
                        "проверь сделки на CSFloat",
                    ))
                elif decision.action == buy_orders.CANCEL:
                    events.append(Event(CANCELLED, name, decision.why))
                else:
                    new = None
                    problem = None
                    try:
                        new = await csfloat_client.create_buy_order(
                            session, name, decision.price_cents, order.qty,
                        )
                    except CSFloatUnknownOutcome:
                        pass
                    except (CSFloatError, CSFloatRateLimited) as e:
                        problem = _reason(e)
                    if new is None and problem is None:
                        new = await _find_new_order(session, name, decision.price_cents, known)
                    if new is None:
                        events.append(Event(
                            FAILED, name,
                            f"старый ордер снят, новый за ${decision.price_cents / 100:.2f} "
                            f"не встал ({problem or 'в списке его не видно'}). "
                            "Поставить заново: /orders",
                        ))
                    else:
                        known.add(new.order_id)
                        registry[new.order_id] = {
                            **entry,
                            "price": new.price_cents,
                            "qty": new.qty,
                            "repriced_at": now,
                            "reprices": int(entry.get("reprices") or 0) + 1,
                            "outbid_noted": decision.outbid,
                        }
                        committed += new.price_cents * new.qty
                        kind = RAISED if decision.action == buy_orders.RAISE else LOWERED
                        events.append(Event(
                            kind, name,
                            f"${order.price_cents / 100:.2f} → "
                            f"${new.price_cents / 100:.2f}: {decision.why}",
                        ))
                        log.info(
                            "orders: chat_id=%s ордер на «%s» переставлен %s -> %s ($%.2f)",
                            chat_id, name, oid, new.order_id, new.price_cents / 100,
                        )
                try:
                    await save_bot_orders(chat_id, registry)
                except StorageUnavailable as e:
                    return Round(events, checked, blind, len(registry), committed,
                                 f"реестр не записан: {_reason(e)}")

            try:
                await save_bot_orders(chat_id, registry)
            except StorageUnavailable as e:
                return Round(events, checked, blind, len(registry), committed,
                             f"реестр не записан: {_reason(e)}")

    return Round(events, checked, blind, len(registry), committed, None)


async def snapshot(chat_id: int) -> Snapshot:
    """
    Все ордера на CSFloat, реестр бота и баланс — для /myorders.

    Список ордеров обязателен (сбой — исключение наружу), остальное — как
    получится: без реестра просто не видно, какие ордера ботовые.
    """
    async with _session() as session:
        orders = await csfloat_client.my_buy_orders(session)
        balance = await csfloat_client.account_balance(session)
    try:
        registry = await get_bot_orders(chat_id)
    except StorageUnavailable as e:
        log.warning("orders: реестр для /myorders не прочитан — %s", _reason(e))
        registry = None
    return Snapshot(orders, registry, balance)


async def cancel(
    chat_id: int, *, ids: list[str] | None = None, include_manual: bool = False,
) -> CancelOutcome:
    """
    Снять ордера: перечисленные по id, все ботовые или вообще все.

    Ботовые без реестра не определить — тогда отказ, а не «сниму всё подряд».
    """
    cancelled: list[MyBuyOrder] = []
    failed: list[tuple[MyBuyOrder, str]] = []
    async with _lock(chat_id):
        try:
            registry: dict[str, dict] | None = await get_bot_orders(chat_id)
        except StorageUnavailable as e:
            if ids is None and not include_manual:
                return CancelOutcome([], [], f"реестр не прочитан ({_reason(e)}) — "
                                             "не знаю, какие ордера ботовые")
            registry = None

        async with _session() as session:
            try:
                current = await csfloat_client.my_buy_orders(session)
            except (CSFloatError, CSFloatRateLimited) as e:
                return CancelOutcome([], [], f"свои ордера не прочитаны: {_reason(e)}")
            if ids is not None:
                wanted = set(ids)
                targets = [o for o in current if o.order_id in wanted]
            elif include_manual:
                targets = list(current)
            else:
                targets = [o for o in current if o.order_id in (registry or {})]

            for order in targets:
                try:
                    await csfloat_client.cancel_buy_order(session, order.order_id)
                except CSFloatRateLimited as e:
                    failed.append((order, _reason(e)))
                    break
                except CSFloatError as e:
                    failed.append((order, _reason(e)))
                    continue
                cancelled.append(order)
                log.info("orders: chat_id=%s снят ордер %s на «%s» ($%.2f)",
                         chat_id, order.order_id, order.market_hash_name,
                         order.price_cents / 100)
                if registry is not None:
                    registry.pop(order.order_id, None)

        if registry is not None and cancelled:
            try:
                await save_bot_orders(chat_id, registry)
            except StorageUnavailable as e:
                return CancelOutcome(cancelled, failed, f"реестр не записан: {_reason(e)}")
    return CancelOutcome(cancelled, failed, None)
