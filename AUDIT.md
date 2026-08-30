# Dextrade — архитектурный аудит и потенциальные слабые места

> Дата: 2026-08-30
> Статус: анализ текущего `main`, без изменений логики бота.
> Важно: этот файл содержит наблюдения и рекомендации. Никакие исправления из него не считаются согласованными к внесению.

## 1. Краткое описание

Dextrade — не просто парсер Steam. Это система поиска потенциально недооценённых CS2-предметов с несколькими независимыми источниками и сигналами:

- Steam Market listings и цены;
- стикеры и их оценка;
- float/inspect;
- CSFloat arbitrage;
- SIH / CSGOTrader и другие источники;
- история цен;
- dips / отклонения от истории;
- watchlists;
- inventory;
- Redis/Upstash для состояния и кэшей;
- proxy/rate-limit слой;
- Telegram как интерфейс и канал уведомлений.

Упрощённая схема:

```text
Sources
  ├─ Steam
  ├─ CSFloat
  ├─ CSGOTrader / SIH
  └─ Inspect / Float
        ↓
Normalization / caching
        ↓
Signals
  ├─ Sticker
  ├─ Float
  ├─ Arbitrage
  ├─ Dips
  └─ Watchlist
        ↓
Filters / valuation
        ↓
Dedup / cooldown
        ↓
Telegram alert
```

## 2. Что сделано хорошо

### Steam networking

- Есть throttling и разные scope.
- Есть обработка 429 и persistent cooldown.
- Учитывается, что restart процесса не должен мгновенно забывать cooldown.
- ProxyPool не удаляет proxy после единичной ошибки.
- Используется общая aiohttp session/keep-alive вместо создания нового TCP/TLS соединения на каждый запрос.

### ProxyPool

- Есть распределение адресов.
- Ошибки считаются по proxy.
- Есть cooldown/dead-состояния.
- Успешный запрос восстанавливает состояние proxy.

### Storage / Redis

- Redis используется как persistent storage.
- Есть batch/pipeline-подход.
- Локальный fallback позволяет не падать сразу при проблемах Redis, хотя он не является полноценным persistent storage на Render.

### Sticker engine

- Используется не только номинальная sticker value.
- Считается floor, overpay и markup.
- Есть отдельная логика streak одинаковых stickers.
- Есть кэширование и prewarm.

### Float hunting

- Float извлекается из inspect link локально, без лишнего сетевого запроса.
- Есть отдельные диапазоны для FN/BS.
- Float-поиск отделён от sticker-поиска.

### CSFloat

- Код учитывает, что лимит API связан с API key, а не просто с IP.
- Не делается наивное умножение скорости через множество proxy.
- Есть отдельный глобальный интервал запросов.
- Документация/комментарии фиксируют реальные наблюдения о 403/429 и антифроде.

### Prewarm

- Не обновляет весь огромный каталог stickers.
- Обновляет только реально встречавшиеся ключи.
- Есть initial delay после Render restart.
- Перед запросами проверяется Steam pricing cooldown.

## 3. Главные потенциальные слабые места

### 🔴 P1 — ложноположительные находки

Главный бизнес-риск Dextrade — не падение бота, а сообщение «выгодно», когда реальная перепродажа не даёт заявленной прибыли.

Причины могут складываться:

- аномальный floor;
- устаревшая цена;
- номинальная sticker value вместо реальной sticker premium;
- редкий float без доказанной рыночной премии;
- низкая ликвидность;
- исчезновение исходного listing;
- комиссии;
- слабая подтверждённость Steam valuation;
- разница между теоретической ценой и ценой, по которой предмет реально можно продать.

Приоритет: очень высокий.

### 🔴 P1 — floor как единственная опорная точка

Сейчас логика может использовать самый дешёвый listing как floor. Один выброс способен сильно изменить overpay/markup всех остальных лотов.

Пример:

```text
$50  ← единичный выброс
$62
$64
$65
$66
```

Тогда floor=$50, хотя устойчивый рынок находится ближе к $62–66.

Что проверить/возможное направление:

- raw floor;
- 2nd/3rd cheapest;
- median нижних N listings;
- price density;
- возраст listing, если доступен;
- исключение очевидных выбросов.

Важно: это предложение для будущего изменения, не внесённое изменение.

### 🟠 P2 — отрицательный overpay смешивает разные сигналы

Если listing дешевле floor:

```text
listing < floor
```

то overpay становится отрицательным. Это может быть отличной находкой, но по смыслу это не sticker overpay.

Желательно концептуально разделять:

```text
discount_to_floor
sticker_markup
```

Иначе одна шкала пытается описывать две разные причины покупки.

### 🔴 P1 — nominal sticker value ≠ реальная sticker premium

Например, sticker market value $200 не означает, что craft добавляет $200 к skin.

Реальная premium зависит от:

- самого skin;
- позиции sticker;
- количества одинаковых stickers;
- сочетания stickers;
- ликвидности;
- популярности craft;
- конкретной площадки;
- объёма реальных продаж.

Текущий markup — полезный фильтр, но не полноценная valuation model.

Это, вероятно, крупнейший резерв качества sticker-сигналов.

### 🟠 P2 — float valuation пока эвристическая

Редкий float + небольшая премия к floor — хороший hunting-сигнал, но не полноценная модель стоимости float.

Премия по float нелинейна и сильно зависит от конкретного skin/pattern/рынка.

В будущем можно собирать реальные comparable listings/sales и оценивать empirical premium для диапазона float.

### 🔴 P1 — data health / stale data

Система может получить HTTP 200 и формально валидный объект, но экономически плохие или неполные данные.

Особенно опасно:

```text
источник ответил
≠
данные пригодны для valuation
```

Нужен мысленный/будущий слой Data Health:

- свежесть;
- полнота;
- подтверждение вторым источником;
- объём продаж;
- стабильность цены;
- время последнего успешного обновления.

И затем confidence score находки.

### 🟠 P2 — CSFloat внешне может «работать», но молча терять valuation

Из комментариев текущего клиента следует важный исторический случай: поле `item.scm` исчезло, из-за чего цена Steam/volume/sticker data перестали приходить. При этом сам listing и цена продолжали разбираться.

Опасность: пользователь может видеть просто «арбитраж ничего не нашёл», хотя реальная причина — деградация входных данных.

Нужны явные health metrics/logging для обязательных полей.

### 🟠 P2 — Cloudflare Worker как дополнительная точка отказа

Маршрут вида:

```text
Dextrade → Worker → CSFloat
```

добавляет ещё один слой:

- Worker;
- исходящий IP Worker;
- ограничения Worker;
- передача Authorization;
- host allowlist;
- CSFloat anti-bot.

Если маршрут используется, желательно отдельно понимать, какой процент запросов/ошибок относится к каждому слою.

### 🟠 P2 — scheduler/orchestration concurrency

Есть несколько фоновых систем:

- watchlist;
- prewarm;
- arbitrage;
- dips;
- price history;
- inventory;
- ручные команды.

Нужно гарантировать, что одинаковая тяжёлая задача не запускается одновременно после restart/manual trigger/timeout.

Особенно проверить:

```text
background scan
+ manual scan
+ watchlist scan
```

и влияние на общий Steam throttle.

### 🟠 P2 — общий Steam throttle может сделать ручные команды медленными

При большом количестве watchlist items один общий throttle защищает Steam, но фоновые задачи могут занимать очередь.

Результат: ручная команда может ждать фоновые запросы.

Нужно проверить приоритеты очереди:

```text
manual user request > scheduled scan
```

или другое желаемое поведение.

### 🟠 P2 — дедупликация

Нужно проверить точный ключ dedup.

Слишком простой ключ, например только:

```text
market_hash_name + price
```

может спутать два разных listing с одинаковой ценой.

Слишком подробный ключ может, наоборот, пропускать один и тот же listing повторно после изменения второстепенного поля.

Желательно использовать максимально стабильный идентификатор listing, если источник его предоставляет, плюс разумный fallback.

### 🟡 P3 — Redis local fallback не persistent

Local JSON полезен как аварийный fallback, но на Render нельзя считать его гарантированно сохранённым после restart/redeploy.

Семантика:

```text
Redis fallback = пережить временную ошибку
```

а не:

```text
Redis fallback = долговременное хранилище
```

### 🟡 P3 — prewarm масштабируется линейно

Сейчас stale sticker keys обновляются последовательно с паузой.

Для сотен/тысяч keys полный цикл может быть долгим.

Это правильная консервативная стратегия для Steam, но при росте каталога потребуется продуманная очередь/приоритизация, а не просто увеличение concurrency.

### 🟡 P3 — синхронный JSON parsing

Большие static price files читаются/парсятся синхронно. При текущем размере это не критично, но при существенном росте файла может блокироваться event loop.

## 4. Архитектурный риск: слишком большой bot.py

`bot.py` является огромным центральным orchestrator-модулем.

Риск не обязательно проявляется сегодня, но стоимость изменения растёт:

- Telegram handlers;
- orchestration;
- scheduler;
- settings;
- scanning;
- formatting;
- dedup;
- background tasks;
- business logic.

Всё это в одном центральном модуле повышает вероятность регрессий при дальнейших изменениях.

Будущее направление рефакторинга:

```text
bot/
  handlers/
  services/
  scanners/
  notifications/
  schedulers/
  models/
```

Но рефакторинг нельзя делать автоматически только потому, что файл большой: сначала нужно понять реальные зависимости.

## 5. Что проверить следующим этапом

### A. Полный путь одного Steam listing

```text
Steam response
→ parser
→ normalized object
→ sticker/float enrichment
→ filters
→ valuation
→ dedup
→ Telegram
→ storage
```

Цель: найти точные места возникновения ложных находок и повторных уведомлений.

### B. Watchlist

Проверить:

- расписание;
- concurrency;
- очередь;
- ручной запуск;
- retry;
- overlap;
- cooldown;
- Telegram latency.

### C. Sticker engine

Проверить:

- источник цены;
- TTL;
- stale values;
- fallback;
- unknown stickers;
- batch loading;
- реальные последствия отсутствия sticker price.

### D. Arbitrage

Проверить формулы:

```text
CSFloat price
→ fees
→ Steam valuation
→ volume
→ sticker adjustment
→ float adjustment
→ expected profit
```

И отдельно проверить, может ли математическая прибыль быть нереализуемой.

### E. Dedup / state

Проверить точный ключ, TTL и поведение после restart.

### F. Failure modes

Симулировать логически:

- Steam 429;
- CSFloat 403/429;
- proxy death;
- Redis unavailable;
- Render restart;
- timeout;
- malformed listing;
- missing price;
- missing stickers;
- missing float;
- duplicate scheduler;
- concurrent manual/background scans.

## 6. Главная стратегическая рекомендация

Не добавлять бесконечно новые фильтры, пока не улучшена оценка качества существующей находки.

Цель следующего поколения Dextrade должна быть ближе к:

```text
buy price
expected resale range
realistic sticker premium
float premium
liquidity
fees
price stability
source freshness
confidence
expected profit
```

а не только:

```text
sticker value
markup
floor discount
```

Идеальный результат — не «найти больше находок», а **снизить число ложных находок и повысить долю сигналов, которые реально превращаются в прибыльную сделку**.

## 7. Приоритеты

### P1 — сделать сначала

1. Проверить ложноположительные находки.
2. Проверить floor/outlier problem.
3. Проверить data freshness/completeness.
4. Проверить dedup.
5. Проверить scheduler overlap.
6. Проверить реальную формулу прибыли/комиссий в arbitrage.

### P2 — после этого

1. Empirical sticker premium.
2. Empirical float premium.
3. Confidence score.
4. Приоритет ручных запросов над фоновыми.
5. Улучшение диагностики CSFloat.

### P3 — позже

1. Декомпозиция `bot.py`.
2. Оптимизация больших static price files.
3. Более умный prewarm scheduler.

---

## Важное ограничение этого документа

Это **аудит и список гипотез/рисков**, а не список подтверждённых багов.

Перед любым исправлением каждый пункт должен быть проверен по конкретному коду и, где возможно, реальному runtime-поведению.

**Никакие изменения логики Dextrade из этого документа автоматически не разрешены.**
