"""
Медиана сделок за 30 дней, продажи за сутки и нижний лот — со страниц нового рынка Steam.

Старый адрес /market/listings/730/<имя> отвечает 302 на страницу группы
(/market/listings/730/G...), а в ней — история продаж ВСЕХ вариантов скина
(износы, StatTrak, Souvenir) и нижний лот каждого. Поэтому один запрос
закрывает всю группу. Пишет по строке на вариант в medians.jsonl; повторный
запуск продолжает с места остановки.

Запуск: python3 collect_groups.py [секунд_работы]. Список групп — в
data/names_rest.json (группы, по которым замеров ещё нет; сначала те, где
есть скины в полосе $2.5–100). На 429 ждёт 1–4 минуты и пробует снова.
"""
import json, re, subprocess, sys, time, urllib.parse
from pathlib import Path

HERE = Path(__file__).parent
OUT = HERE / "data" / "medians.jsonl"
TMP = HERE / "pages" / "current.html"
PAUSE = 3.5
DAY = 86400
SSR = re.compile(r'<script[^>]*id="valve-ssr-data"[^>]*>(.*?)</script>', re.S)


def weighted_median(pairs):
    total = sum(n for _, n in pairs)
    if total <= 0:
        return None
    seen = 0
    for price, n in sorted(pairs):
        seen += n
        if seen >= total / 2:
            return price
    return None


def fetch(name):
    url = ("https://steamcommunity.com/market/listings/730/"
           + urllib.parse.quote(name, safe=""))
    proc = subprocess.run(
        ["curl", "-sSL", "--compressed", "--max-time", "60", "-o", str(TMP),
         "-w", "%{http_code} %{url_effective}", url],
        capture_output=True, text=True,
    )
    code, _, final = proc.stdout.strip().partition(" ")
    return code, final, proc.stderr.strip()


def parse(html, at):
    """{имя: строка} по всем вариантам на странице."""
    match = SSR.search(html)
    if not match:
        return None
    data = json.loads(match.group(1))
    queries = json.loads(data["renderContext"]["queryData"])["queries"]
    lowest = {}
    for chunk in data.get("loaderData") or []:
        try:
            obj = json.loads(chunk) if isinstance(chunk, str) else chunk
        except ValueError:
            continue
        if not isinstance(obj, dict):
            continue
        for bucket in obj.get("buckets") or []:
            try:
                lowest[bucket["bucket_id"]] = int(bucket["min_price"]) / 100
            except (KeyError, TypeError, ValueError):
                pass
    histories = {}
    for q in queries:
        key = q.get("queryKey") or []
        if key[:2] == ["market", "pricehistory"] and len(key) >= 4:
            histories[key[3]] = (q.get("state") or {}).get("data") or {}
    # Конец истории — самая свежая точка группы: Steam отстаёт на пару часов,
    # и «за сутки» считаем как 24 часа до неё, а не до момента запроса. Если
    # свежая точка старше 6 ч, это последняя продажа, а не конец истории, —
    # тогда отставание считаем трёхчасовым.
    newest = max((p["time"] for h in histories.values() for p in h.get("prices") or []),
                 default=None)
    if newest is None or at - newest > 6 * 3600:
        newest = at - 3 * 3600
    rows = {}
    for name, h in histories.items():
        points = [(p["time"], float(p["price_median"]), int(p["purchases"]))
                  for p in h.get("prices") or [] if p.get("purchases")]
        month = [(price, n) for ts, price, n in points if ts >= at - 30 * DAY and price > 0]
        row = {
            "name": name, "at": at, "currency": h.get("ecurrency"),
            "median30": weighted_median(month),
            "sold30": sum(n for _, n in month),
            "sold7": sum(n for ts, _, n in points if ts >= at - 7 * DAY),
            "sold24": (sum(n for ts, _, n in points if ts > newest + 3600 - DAY)
                       if newest else None),
            "history_end": newest,
            "lowest": lowest.get(name),
        }
        rows[name] = row
    for name, price in lowest.items():
        rows.setdefault(name, {"name": name, "at": at, "lowest": price, "no_history": True})
    return rows


def main():
    names = json.loads((HERE / "data" / "names_rest.json").read_text(encoding="utf-8"))
    limit = len(names)
    # необязательный аргумент — сколько секунд работать, потом выйти
    deadline = time.time() + float(sys.argv[1]) if len(sys.argv) > 1 else None
    done = set()
    if OUT.exists():
        for line in OUT.read_text(encoding="utf-8").splitlines():
            try:
                done.add(json.loads(line)["name"])
            except (ValueError, KeyError):
                pass
    todo = [n for n in names[:limit] if n not in done]
    print(f"к опросу до {len(todo)} (уже есть {len(done)})", flush=True)
    TMP.parent.mkdir(exist_ok=True)
    strikes = pages = 0
    with OUT.open("a", encoding="utf-8") as f:
        for i, name in enumerate(todo, 1):
            if name in done:
                continue
            if deadline and time.time() > deadline:
                print("время вышло — продолжу в следующем запуске", flush=True)
                return
            while True:
                code, final, err = fetch(name)
                if code == "429":
                    strikes += 1
                    wait = (60, 90, 120, 180, 240)[min(strikes, 5) - 1]
                    print(f"429 на {i}/{len(todo)} ({strikes} подряд) — жду {wait} с", flush=True)
                    if deadline and time.time() + wait > deadline:
                        print("время выходит — продолжу в следующем запуске", flush=True)
                        return
                    time.sleep(wait)
                    continue
                strikes = 0
                break
            at = time.time()
            pages += 1
            rows = None
            if code == "200":
                try:
                    rows = parse(TMP.read_text(encoding="utf-8", errors="replace"), at)
                except (ValueError, KeyError, TypeError) as e:
                    print(f"не разобрал {name}: {e}", flush=True)
            group = final.rsplit("/", 1)[-1].split("?", 1)[0]
            for row in (rows or {}).values():
                if row["name"] in done:
                    continue
                row["group"] = group
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                done.add(row["name"])
            if name not in done:
                f.write(json.dumps({"name": name, "at": at, "code": code, "group": group,
                                    "missing": True, "err": err[:200]},
                                   ensure_ascii=False) + "\n")
                done.add(name)
            f.flush()
            if pages % 20 == 0:
                print(f"{i}/{len(todo)}, страниц {pages}, предметов {len(done)}", flush=True)
            time.sleep(PAUSE)
    print(f"готово: страниц {pages}, предметов {len(done)}", flush=True)


main()
