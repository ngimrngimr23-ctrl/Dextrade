"""Свежие цены Steam по списку предметов — для окна свежести бота (/fresh).

Для каждого имени спрашивает priceoverview (низ стакана, медиана сделок за
сутки, продано за сутки) обычным curl, без подмены User-Agent, и дописывает
строку в quotes.jsonl. Прерванный запуск продолжает с места остановки:
имена, уже опрошенные за последние RESUME_HOURS часов, пропускаются.

    python3 collect_quotes.py names.json quotes.jsonl [секунд]

names.json — список имён. Третий аргумент — сколько секунд поработать
(без него — до конца списка). На 429 ждёт и пробует снова, а после
нескольких подряд уходит на долгую паузу: адрес, который Steam уже режет,
частотой не вылечить.
"""
import json
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

GAP = 3.6            # секунд между запросами
RESUME_HOURS = 3
SHORT_WAIT = 90      # пауза после одиночного 429
LONG_WAIT = 900      # после LONG_AFTER подряд
LONG_AFTER = 4
GIVE_UP_AFTER = 12   # подряд — завершаем, пусть решает человек


def money(text):
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit() or ch in ".,")
    digits = digits.replace(",", "")
    try:
        return float(digits)
    except ValueError:
        return None


def ask(name):
    url = ("https://steamcommunity.com/market/priceoverview/?appid=730&currency=1"
           "&market_hash_name=" + urllib.parse.quote(name, safe=""))
    out = subprocess.run(
        ["curl", "-sS", "--compressed", "-m", "25", "-w", "\n%{http_code}", url],
        capture_output=True,
    )
    # Ответ бывает сжат или обрезан обрывом — декодируем без падения.
    text = out.stdout.decode("utf-8", errors="replace")
    body, _, code = text.rpartition("\n")
    return int(code) if code.strip().isdigit() else 0, body


def main():
    names = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    out_path = Path(sys.argv[2])
    budget = float(sys.argv[3]) if len(sys.argv) > 3 else None
    started = time.time()
    done = {}
    if out_path.exists():
        for line in out_path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            done[row["name"]] = row["at"]
    todo = [n for n in names if time.time() - done.get(n, 0) > RESUME_HOURS * 3600]
    print(f"к опросу {len(todo)} из {len(names)}", flush=True)
    streak = 0
    with out_path.open("a", encoding="utf-8") as sink:
        for i, name in enumerate(todo):
            if budget and time.time() - started > budget:
                print("время вышло, продолжу с места остановки", flush=True)
                return
            while True:
                code, body = ask(name)
                if code == 0:          # сеть оборвалась — короткая пауза и ещё раз
                    time.sleep(10)
                    code, body = ask(name)
                if code == 429:
                    streak += 1
                    if streak >= GIVE_UP_AFTER:
                        print(f"{streak} раз подряд 429 — стоп", flush=True)
                        return
                    wait = LONG_WAIT if streak >= LONG_AFTER else SHORT_WAIT
                    print(f"429 ({streak} подряд), жду {wait} с", flush=True)
                    time.sleep(wait)
                    if budget and time.time() - started > budget:
                        return
                    continue
                break
            streak = 0
            row = {"name": name, "at": time.time(), "code": code}
            try:
                data = json.loads(body)
            except ValueError:
                data = {}
            if code == 200 and data.get("success"):
                row["ask"] = money(data.get("lowest_price"))
                row["median"] = money(data.get("median_price"))
                row["volume"] = int(money(data.get("volume")) or 0) if data.get("volume") else None
            sink.write(json.dumps(row, ensure_ascii=False) + "\n")
            sink.flush()
            if i % 50 == 0:
                print(f"{i}/{len(todo)}", flush=True)
            time.sleep(GAP)
    print("готово", flush=True)


if __name__ == "__main__":
    main()
