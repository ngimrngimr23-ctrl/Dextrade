"""
Ошибки прогона: собрать, сгруппировать, вычистить секреты, показать человеку.

Зачем отдельным модулем. Раньше сбой по предмету попадал только в лог Render:
_watchlist_scan_item ловил исключение, писал warning и возвращал False. Со
стороны чата это было неотличимо от «проверено 618 предметов, ничего не
нашлось» — то есть полностью сломанный прогон выглядел ровно как спокойный.
Один такой случай уже был: Steam отдавал HTML-страницу бета-версии площадки
вместо JSON, падали ВСЕ предметы, а бот бодро рапортовал, что всё в порядке.

Три вещи, ради которых это не пара строк по месту.

ГРУППИРОВКА. Слать по сообщению на предмет нельзя: при сбое у Steam падает
не один предмет, а весь список, и это шестьсот сообщений подряд. Причина при
этом у всех одна, поэтому показываем причины, а не случаи.

ЧИСТКА СЕКРЕТОВ. Текст исключения уходит в чат, а в него легко попадает
пароль от прокси (они лежат в URL целиком) или кусок куки. Сообщение в
Telegram — это уже наружу, и вычищать надо до отправки, а не надеяться.

ОТРЕЗАНИЕ ХВОСТА. Некоторые исключения дописывают в текст начало ответа
сервера. Оно у каждого предмета своё, и без отрезания одна и та же причина
рассыпалась бы на шестьсот разных «групп» по одной штуке.
"""

from __future__ import annotations

import re

# Сколько РАЗНЫХ причин показывать. Больше трёх не бывает полезно: если причин
# много, важна не каждая, а сам факт, что прогон разваливается.
DEFAULT_REASON_LIMIT = 3
# Сколько названий предметов приводить в примере к причине.
DEFAULT_NAME_LIMIT = 3
# Длина причины в символах. Достаточно, чтобы уместилось «Steam отдал
# HTML-страницу новой (бета) торговой площадки вместо JSON» целиком.
REASON_LIMIT = 160

# Хвосты, которые у каждого предмета свои и потому мешают группировке.
_TAIL = re.compile(r"\s*(Начало ответа|Ответ сервера|body=|url=)", re.IGNORECASE)

# Пароль в адресе прокси: они задаются как http://логин:пароль@хост:порт и
# целиком попадают в текст сетевых исключений.
_CREDS = re.compile(r"//[^/\s:@]+:[^/\s@]+@")
# Токен бота Telegram: цифры, двоеточие, длинный хвост. Без \b в начале:
# в URL токен идёт как «…/bot8378613745:AAG…», и границы слова перед цифрами
# там нет — с \b правило молча не срабатывало ровно на самом частом случае.
_BOT_TOKEN = re.compile(r"\d{6,}:[A-Za-z0-9_-]{20,}")
# Именованные секреты: steamLoginSecure=…, api_key: …, token=… и подобное.
_NAMED_SECRET = re.compile(
    # «secure» отдельно от «secret»: главный секрет этого бота называется
    # steamLoginSecure, и без него правило промахивалось мимо самого важного.
    r"(?i)\b([a-z_]*(?:token|key|secret|secure|passw|cookie|session|auth)[a-z_]*)"
    r"(\s*[=:]\s*)([^\s,;&\"')]{6,})"
)

# Исключения, у которых текст пустой: показывать голое имя класса бесполезно.
_BY_CLASS = {
    "TimeoutError": "таймаут запроса",
    "ServerTimeoutError": "таймаут запроса",
    "CancelledError": "запрос отменён",
    "ConnectionResetError": "соединение разорвано",
    "ClientOSError": "обрыв соединения",
}


def scrub(text: str) -> str:
    """
    Убрать из текста то, что нельзя показывать: пароли прокси, токены, куки.

    Порядок важен: сначала пароль в адресе (иначе именованное правило съело бы
    только часть), потом токены, потом всё остальное по имени.
    """
    text = _CREDS.sub("//***@", text)
    text = _BOT_TOKEN.sub("***", text)
    text = _NAMED_SECRET.sub(r"\1\2***", text)
    return text


def reason(exc: BaseException) -> str:
    """
    Короткая причина — она же ключ группировки.

    Берём текст исключения, а не имя класса: RuntimeError ни о чём не говорит,
    а «Steam вернул не JSON» говорит всё. Имя класса идёт в дело только когда
    текста нет вовсе (так бывает у таймаутов).
    """
    name = type(exc).__name__
    text = " ".join(scrub(str(exc)).split())
    text = _TAIL.split(text)[0].strip().rstrip(".,;:")
    if not text:
        return _BY_CLASS.get(name, name)
    if len(text) > REASON_LIMIT:
        text = text[:REASON_LIMIT].rstrip() + "…"
    return text


def group(errors: list[tuple[str, BaseException]]) -> list[tuple[str, list[str]]]:
    """
    [(предмет, исключение)] -> [(причина, [предметы])], частые причины первыми.

    Порядок внутри причины сохраняем как есть — он совпадает с порядком
    прогона, и по первому названию видно, с чего началось.
    """
    by_reason: dict[str, list[str]] = {}
    for name, exc in errors:
        by_reason.setdefault(reason(exc), []).append(name)
    return sorted(by_reason.items(), key=lambda kv: (-len(kv[1]), kv[0]))


def summarize(
    errors: list[tuple[str, BaseException]],
    *,
    items: int,
    reason_limit: int = DEFAULT_REASON_LIMIT,
    name_limit: int = DEFAULT_NAME_LIMIT,
) -> list[str]:
    """
    Блок для итогового сообщения. Пустой список, если ошибок не было.

    items — сколько предметов прогон вообще успел взять. Нужно, чтобы отличить
    «двенадцать из шестисот» от «все шестьсот»: это принципиально разные
    новости, и вторую нельзя подавать тем же тоном, что первую.
    """
    if not errors:
        return []

    groups = group(errors)
    failed = len(errors)

    if items and failed >= items:
        head = f"❌ Прогон сорвался: ошибка на ВСЕХ {items} предмет(ах)."
    else:
        head = f"⚠️ Ошибки: {failed} предмет(ов)" + (f" из {items}" if items else "") + "."
    lines = ["", head]

    for reason_text, names in groups[:reason_limit]:
        lines.append(f"• {reason_text} — {len(names)} шт.")
        shown = ", ".join(names[:name_limit])
        if len(names) > name_limit:
            shown += f" и ещё {len(names) - name_limit}"
        lines.append(f"  {shown}")

    if len(groups) > reason_limit:
        others = sum(len(names) for _, names in groups[reason_limit:])
        lines.append(f"• …и ещё {len(groups) - reason_limit} причин(ы), {others} предмет(ов)")

    lines.append("Подробности — в логах Render.")
    return lines
