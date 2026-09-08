"""
Выгрузка лога в файл на GitHub — чтобы его можно было читать не из чата.

Зачем. Бот живёт на Render, его диск эфемерен и снаружи недоступен. /logs
решает это наполовину: файл приходит в чат, но дальше его надо кому-то
переслать. Если положить лог в репозиторий, читать его можно напрямую.

ГЛАВНОЕ ПРЕДУПРЕЖДЕНИЕ, И ОНО ЖЕ ПРИЧИНА ЗАЩИТЫ НИЖЕ.

Репозиторий на GitHub — это публикация. В логах бота лежит: id чата, весь
вотчлист (то есть стратегия целиком), хосты и порты прокси, трейсбеки с
внутренним состоянием. Секреты вычищаются (logsetup.ScrubbingFormatter), но
чистка знает только про известные виды — пароль в URL, токен, куки. Любая
строка log.info, дописанная завтра, поедет в файл как есть.

И отдельно: история git хранит ВСЕ версии. Удалить файл потом недостаточно —
каждая прошлая выгрузка останется доступной по хешу коммита навсегда.

Поэтому в ПУБЛИЧНЫЙ репозиторий модуль писать отказывается. Снять запрет
можно (LOG_GITHUB_ALLOW_PUBLIC=1), но это должно быть осознанным действием,
а не значением по умолчанию.

ПОЧЕМУ ОТДЕЛЬНЫЙ РЕПОЗИТОРИЙ, А НЕ ВЕТКА В ЭТОМ.

Render передеплоивает сервис на пуш в отслеживаемую ветку. Лог, уезжающий в
неё, перезапустил бы бота, который выгрузил бы лог, который перезапустил бы
бота. Отдельный приватный репозиторий убирает и эту петлю, и публичность
разом, поэтому LOG_GITHUB_REPO задаётся полностью (owner/repo) и по умолчанию
никуда не указывает.

Пишем через Contents API одним PUT — git на Render не нужен, клона нет,
хватает aiohttp, который и так в зависимостях.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import envcfg
import os

import aiohttp

log = logging.getLogger("steam_bot.logship")

API = "https://api.github.com"

def _normalize_repo(raw: str) -> str:
    """
    Привести LOG_GITHUB_REPO к виду owner/repo.

    Терпим то, что реально вставляют вместо голого owner/repo: полную ссылку
    со страницы репозитория, хвост .git от clone-адреса, лишние слэши. Это не
    угождение неряшливости — значение задают на телефоне в поле Render, где
    скопировать URL целиком проще, чем вырезать из него два слова.

    Пробел вместо слэша НЕ чиним молча: это опечатка, и подставить за человека
    слэш значит угадать. Такое значение отвергает validate_repo с прямым
    указанием, что не так.
    """
    value = raw.strip().strip("/")
    for prefix in ("https://github.com/", "http://github.com/", "git@github.com:"):
        if value.lower().startswith(prefix):
            value = value[len(prefix):]
    if value.lower().endswith(".git"):
        value = value[:-4]
    return value.strip("/")


TOKEN = os.environ.get("LOG_GITHUB_TOKEN", "").strip()
REPO = _normalize_repo(os.environ.get("LOG_GITHUB_REPO", ""))  # owner/repo
BRANCH = os.environ.get("LOG_GITHUB_BRANCH", "main").strip()
PATH = os.environ.get("LOG_GITHUB_PATH", "dextrade.log").strip()
INTERVAL_MINUTES = envcfg.env_float("LOG_SHIP_MINUTES", 30)
ALLOW_PUBLIC = os.environ.get("LOG_GITHUB_ALLOW_PUBLIC", "").strip() in ("1", "true", "yes", "да")

# Потолок НАКОПЛЕННОГО файла на GitHub. По достижении режем с начала: файл
# накапливается через редеплои (диск Render эфемерен, а удалённый файл — нет),
# и без потолка он рос бы бесконечно.
#
# Режем именно с начала, а не стираем целиком: «очистить» на десятом мегабайте
# означало бы выбросить и свежие строки тоже, ровно в тот момент, когда их
# больше всего.
# Имя своё, не LOG_MAX_MB: та переменная задаёт размер ЛОКАЛЬНОГО файла
# (logsetup), и одно имя на две разные величины однажды сведёт их вместе
# в самый неподходящий момент.
MAX_BYTES = envcfg.env_int("LOG_GITHUB_MAX_MB", 10) * 1024 * 1024

# Не чаще одного коммита в столько секунд, даже если строки идут потоком.
# Выгрузка теперь по событию, и без этой паузы шумный прогон дал бы коммит на
# каждую строку.
MIN_GAP_SECONDS = envcfg.env_float("LOG_SHIP_MIN_GAP", 60)

# Хеш последней выгруженной версии: если ничего не изменилось, коммит не нужен.
_last_digest: str | None = None


def enabled() -> bool:
    return bool(TOKEN and REPO)


# Имена, которые мы читаем. Нужны, чтобы находить опечатки: переменную задают
# руками в поле Render, и промах в одной букве выглядит как «ничего не
# настроено» — сообщение, по которому не догадаешься, что искать.
_KNOWN_VARS = (
    "LOG_GITHUB_TOKEN", "LOG_GITHUB_REPO", "LOG_GITHUB_BRANCH",
    "LOG_GITHUB_PATH", "LOG_GITHUB_ALLOW_PUBLIC",
    "LOG_SHIP_MINUTES", "LOG_SHIP_MAX_KB", "LOG_GITHUB_MAX_MB", "LOG_SHIP_MIN_GAP",
    "LOG_FILE", "LOG_RING_LINES", "LOG_MAX_MB", "LOG_BACKUPS",
)


def _suspicious_vars() -> list[str]:
    """
    Переменные окружения, похожие на наши, но названные не так.

    Смотрим на всё, что начинается с LOG_ или содержит GITHUB, и вычитаем
    известные. Опечатка в ИМЕНИ переменной иначе неотличима от её отсутствия.
    """
    out = []
    for name in os.environ:
        upper = name.upper()
        if upper in _KNOWN_VARS:
            continue
        if upper.startswith("LOG") or "GITHUB" in upper or "GITUB" in upper:
            out.append(name)
    return sorted(out)


def status() -> str:
    """
    Что именно видно из настроек. Токен наружу не показываем никогда — только
    факт наличия и длину: этого хватает, чтобы отличить «не задан» от
    «задан, но обрезан при копировании», и не хватает, чтобы им воспользоваться.
    """
    if TOKEN and REPO:
        return f"{REPO}, ветка {BRANCH}, файл {PATH}, раз в {INTERVAL_MINUTES:g} мин"

    parts = [
        f"• LOG_GITHUB_REPO — {'«' + REPO + '»' if REPO else 'НЕ ЗАДАН'}",
        f"• LOG_GITHUB_TOKEN — {'задан, ' + str(len(TOKEN)) + ' символов' if TOKEN else 'НЕ ЗАДАН'}",
    ]
    text = "Выгрузка на GitHub не работает, вот что вижу:\n" + "\n".join(parts)

    odd = _suspicious_vars()
    if odd:
        text += (
            "\n\nПохожие переменные с другими именами — возможно, опечатка:\n"
            + "\n".join(f"• {n}" for n in odd)
        )
    text += (
        "\n\nИмена читаются ровно так, посимвольно: LOG_GITHUB_REPO и "
        "LOG_GITHUB_TOKEN. После правки Render перезапустит сервис сам."
    )
    return text


def file_url() -> str:
    return f"https://github.com/{REPO}/blob/{BRANCH}/{PATH}"


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "Dextrade-logship",
    }


class LogShipError(RuntimeError):
    """Выгрузка не удалась. Текст уходит человеку, поэтому он человеческий."""


def validate_repo(value: str) -> str | None:
    """
    Что не так с LOG_GITHUB_REPO. None — всё в порядке.

    Значение показываем в кавычках: пробел вместо слэша в логе выглядит
    неотличимо от слэша, и именно на этом уже потеряли время.
    """
    if not value:
        return "LOG_GITHUB_REPO пуст — нужен вид owner/repo."
    if "/" not in value:
        hint = ""
        if " " in value:
            hint = " Похоже, вместо слэша пробел."
        return (
            f"LOG_GITHUB_REPO = «{value}» — не похоже на owner/repo.{hint}\n"
            f"Нужно ровно так: ngimrngimr23-ctrl/Dextrade-logs"
        )
    if value.count("/") > 1:
        return (
            f"LOG_GITHUB_REPO = «{value}» — лишние слэши. "
            f"Нужны только владелец и имя: owner/repo."
        )
    owner, _, name = value.partition("/")
    if not owner or not name:
        return f"LOG_GITHUB_REPO = «{value}» — пустой владелец или имя репозитория."
    if " " in value:
        return f"LOG_GITHUB_REPO = «{value}» — внутри пробел, так имя не найдётся."
    return None


async def check(session: aiohttp.ClientSession | None = None) -> str:
    """
    Проверить настройку до первой выгрузки: жив ли токен, есть ли репозиторий
    и — главное — не публичный ли он.

    Возвращает человеческое описание. Бросает LogShipError, если писать нельзя.
    """
    if not enabled():
        return status()

    # Формат проверяем ДО сети. Иначе кривое значение уезжает в URL и
    # возвращается как 404 «репозиторий не найден» — сообщение, которое
    # обвиняет токен и уводит от настоящей причины. Ровно так и вышло:
    # в переменную попал пробел вместо слэша, а бот сказал про права токена.
    problem = validate_repo(REPO)
    if problem:
        raise LogShipError(problem)

    own = session is None
    session = session or aiohttp.ClientSession()
    try:
        async with session.get(f"{API}/repos/{REPO}", headers=_headers()) as resp:
            if resp.status == 401:
                raise LogShipError(
                    "GitHub не принял токен (401). Проверь LOG_GITHUB_TOKEN — "
                    "возможно, он истёк или скопирован не полностью."
                )
            if resp.status == 404:
                # Формат уже проверен выше, значит имя как минимум осмысленное.
                # Для приватного репозитория GitHub отвечает 404, а не 403,
                # когда у токена нет к нему доступа — то есть «не найден» здесь
                # почти всегда означает «токен его не видит».
                raise LogShipError(
                    f"Репозиторий «{REPO}» не найден (404).\n"
                    f"Для приватного репозитория GitHub отвечает 404 и когда его "
                    f"просто нет, и когда токен его не видит — это одно и то же "
                    f"сообщение. Проверь по порядку:\n"
                    f"1) в токене выбран именно этот репозиторий "
                    f"(Only select repositories);\n"
                    f"2) в Permissions добавлено Contents со значением "
                    f"Read and write, и счётчик Repositories показывает 1, а не 0;\n"
                    f"3) имя написано точно: {REPO}"
                )
            if resp.status != 200:
                raise LogShipError(f"GitHub ответил {resp.status} на проверку {REPO}.")
            data = await resp.json()

        if not data.get("private") and not ALLOW_PUBLIC:
            raise LogShipError(
                f"❌ {REPO} — ПУБЛИЧНЫЙ репозиторий, отказываюсь писать в него лог.\n\n"
                f"В логах лежат id чата, весь вотчлист, хосты прокси и трейсбеки. "
                f"В публичном репозитории это читает кто угодно, а история git "
                f"хранит все версии навсегда — удалить файл потом будет уже поздно.\n\n"
                f"Заведи ОТДЕЛЬНЫЙ ПРИВАТНЫЙ репозиторий (например Dextrade-logs) и "
                f"укажи его в LOG_GITHUB_REPO. Если всё-таки уверен — "
                f"LOG_GITHUB_ALLOW_PUBLIC=1."
            )

        kind = "приватный" if data.get("private") else "ПУБЛИЧНЫЙ (разрешено явно)"
        return f"выгрузка настроена: {REPO} ({kind}), ветка {BRANCH}, файл {PATH}"
    finally:
        if own:
            await session.close()


async def _current_sha(session: aiohttp.ClientSession) -> str | None:
    """sha существующего файла — Contents API требует его, чтобы перезаписать."""
    params = {"ref": BRANCH}
    async with session.get(
        f"{API}/repos/{REPO}/contents/{PATH}", headers=_headers(), params=params
    ) as resp:
        if resp.status == 404:
            return None          # файла ещё нет, создадим
        if resp.status != 200:
            raise LogShipError(f"GitHub ответил {resp.status} при чтении {PATH}.")
        data = await resp.json()
        return data.get("sha")


async def _current_content(session: aiohttp.ClientSession) -> bytes:
    """
    Что сейчас лежит в удалённом файле.

    Просим сырое содержимое (Accept: …raw) намеренно: JSON-ответ Contents API
    отдаёт base64 только для файлов меньше мегабайта, а накопленный лог этот
    порог перейдёт на второй же день. С raw ограничение — сто мегабайт.
    """
    headers = {**_headers(), "Accept": "application/vnd.github.raw"}
    async with session.get(
        f"{API}/repos/{REPO}/contents/{PATH}", headers=headers, params={"ref": BRANCH}
    ) as resp:
        if resp.status == 404:
            return b""
        if resp.status != 200:
            raise LogShipError(f"GitHub ответил {resp.status} при чтении {PATH}.")
        return await resp.read()


async def append(new_text: str, *, note: str = "") -> str:
    """
    Дописать новые строки к накопленному файлу на GitHub.

    Почему дописываем, а не перезаписываем. Диск Render эфемерен: при каждом
    редеплое локальный лог начинается с нуля, и перезапись затирала бы всё
    предыдущее — как раз то, что нужнее всего, когда разбираешь «а что было
    до перезапуска». Удалённый файл переживает редеплои и служит архивом.

    Возвращает человеческий отчёт.
    """
    if not enabled():
        raise LogShipError(status())
    if not new_text.strip():
        return "новых строк нет — коммит не нужен"

    async with aiohttp.ClientSession() as session:
        await check(session)
        sha = await _current_sha(session)
        current = await _current_content(session) if sha else b""

        merged = current + new_text.encode("utf-8")
        trimmed = False
        if len(merged) > MAX_BYTES:
            merged = "…(начало обрезано, потолок {} МБ)…\n".format(
                MAX_BYTES // (1024 * 1024)
            ).encode("utf-8") + merged[-MAX_BYTES:]
            trimmed = True

        body = {
            "message": f"лог бота{': ' + note if note else ''}",
            "content": base64.b64encode(merged).decode("ascii"),
            "branch": BRANCH,
        }
        if sha:
            body["sha"] = sha

        async with session.put(
            f"{API}/repos/{REPO}/contents/{PATH}", headers=_headers(), json=body
        ) as resp:
            if resp.status not in (200, 201):
                text = (await resp.text())[:300]
                raise LogShipError(f"GitHub ответил {resp.status} при записи: {text}")

    return (
        f"дописано {len(new_text.encode('utf-8')) / 1024:.1f} КБ, "
        f"в файле {len(merged) / 1024 / 1024:.2f} МБ"
        + (" (начало обрезано по потолку)" if trimmed else "")
    )


async def ship(payload: bytes, *, note: str = "") -> str:
    """
    Выгрузить лог. Возвращает человеческий отчёт.

    Идемпотентна по содержимому: если с прошлого раза ничего не изменилось,
    коммит не делается. Иначе репозиторий пух бы одинаковыми версиями каждые
    полчаса, а история перестала бы что-либо значить.
    """
    global _last_digest

    if not enabled():
        raise LogShipError(status())

    if len(payload) > MAX_BYTES:
        # Режем с начала: интересен всегда хвост.
        payload = "...(начало обрезано)...\n".encode("utf-8") + payload[-MAX_BYTES:]

    digest = hashlib.sha256(payload).hexdigest()
    if digest == _last_digest:
        return "лог не изменился с прошлой выгрузки — коммит не нужен"

    async with aiohttp.ClientSession() as session:
        await check(session)
        sha = await _current_sha(session)

        body = {
            "message": f"лог бота{': ' + note if note else ''}",
            "content": base64.b64encode(payload).decode("ascii"),
            "branch": BRANCH,
        }
        if sha:
            body["sha"] = sha

        async with session.put(
            f"{API}/repos/{REPO}/contents/{PATH}", headers=_headers(), json=body
        ) as resp:
            if resp.status not in (200, 201):
                text = (await resp.text())[:300]
                raise LogShipError(f"GitHub ответил {resp.status} при записи: {text}")

    _last_digest = digest
    return f"лог выгружен ({len(payload) / 1024:.0f} КБ): {file_url()}"
