"""quotes.jsonl (collect_quotes.py) -> steam_quotes.csv, который пересылают боту.

    python3 build_quotes_file.py quotes.jsonl steam_quotes.csv

Бот дописывает котировки к уже загруженному снимку цен Steam и берёт их как
свежую цену для /orders, пока они не старше окна /fresh.
У каждого имени остаётся самый свежий успешный замер.
"""
import json
import sys
import time
from pathlib import Path


def money(value):
    return "" if value is None else f"{value:.2f}".replace(".", ",")


def main():
    best = {}
    for line in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("code") != 200 or not row.get("ask"):
            continue
        if row["name"] not in best or row["at"] > best[row["name"]]["at"]:
            best[row["name"]] = row
    rows = ["market_hash_name;ask_usd;median24_usd;volume24;quote_at_utc"]
    for name, row in sorted(best.items()):
        stamp = time.strftime("%Y-%m-%d %H:%M", time.gmtime(row["at"]))
        volume = "" if row.get("volume") is None else str(row["volume"])
        rows.append(f"{name};{money(row['ask'])};{money(row.get('median'))};{volume};{stamp}")
    Path(sys.argv[2]).write_text("﻿" + "\n".join(rows) + "\n", encoding="utf-8")
    print(f"{len(best)} котировок -> {sys.argv[2]}")


if __name__ == "__main__":
    main()
