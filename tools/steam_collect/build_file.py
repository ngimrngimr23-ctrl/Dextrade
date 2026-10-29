"""Файл для бота: прошлый файл (data/base.csv) + свежие продажи за сутки и медианы.

Объём и медиана — со страниц групп (data/medians.jsonl), объём ещё и из
priceoverview (data/volumes.jsonl). У каждой строки остаётся самый свежий замер. Первая
строка — строка основы: по ней бот считает возраст цен.
"""
import json, time
from pathlib import Path

ROOT = Path(__file__).parent / "data"
OUT = Path(__file__).parent / "out" / "cs2_skins_steam_prices_history.csv"
SRC = ROOT / "base.csv"
stamp = lambda at: time.strftime("%Y-%m-%d %H:%M", time.gmtime(at))
money = lambda v: f"{v:.2f}".replace(".", ",")

best = {}   # имя -> (время, продано за сутки или None, медиана или None)
def offer(name, at, sold, median):
    if sold is None and median is None:
        return
    if name not in best or at > best[name][0]:
        best[name] = (at, sold, median)
for line in (ROOT / "medians.jsonl").read_text(encoding="utf-8").splitlines():
    try: r = json.loads(line)
    except ValueError: continue
    if r.get("missing") or r.get("currency") != 1:
        continue
    offer(r["name"], r["at"], r.get("sold24"), r.get("median30"))
vol = ROOT / "volumes.jsonl"
for line in (vol.read_text(encoding="utf-8").splitlines() if vol.exists() else []):
    try: r = json.loads(line)
    except ValueError: continue
    if "sold24" in r and (r["name"] not in best or best[r["name"]][0] < r["at"]):
        old = best.get(r["name"])
        best[r["name"]] = (r["at"], r["sold24"], old[2] if old else None)

text = SRC.read_bytes().decode("utf-8-sig")
nl = "\r\n" if "\r\n" in text else "\n"
lines = text.splitlines()
head = lines[0].split(";")
iv, it, im = (head.index(c) for c in ("продано_за_сутки", "объём_снят_utc", "медиана_30д_$"))
out = [lines[0]]
fresh = nv = nm = 0
for line in lines[1:]:
    f = line.split(";")
    row = best.get(f[0])
    if row is not None:
        at, sold, median = row
        old_at = time.mktime(time.strptime(f[it], "%Y-%m-%d %H:%M")) - time.timezone if f[it] else 0
        if at > old_at + 60:
            f[iv] = "" if sold is None else str(sold)
            f[it] = stamp(at)
            if median:
                f[im] = money(median)
            fresh += 1
    nv += bool(f[iv]); nm += bool(f[im])
    out.append(";".join(f))
OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_bytes(("\ufeff" + nl.join(out) + nl).encode("utf-8"))
print(f"строк {len(out)-1}: обновлено {fresh}, объём у {nv}, медиана у {nm}")
