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
import os

import aiohttp

log = logging.getLogger("steam_bot.logship")

API = "https://api.github.com"

TOKEN = os.environ.get("LOG_GITHUB_TOKEN", "").strip()
REPO = os.environ.get("LOG_GITHUB_REPO", "").strip()          # owner/repo
BRANCH = os.environ.get("LOG_GITHUB_BRANCH", "main").strip()
PATH = os.environ.get("LOG_GITHUB_PATH", "dextrade.log").strip()
INTERVAL_MINUTES = float(os.environ.get("LOG_SHIP_MINUTES", "30"))
ALLOW_PUBLIC = os.environ.get("LOG_GITHUB_ALLOW_PUBLIC", "").strip() in ("1", "true", "yes", "да")

# Потолок выгрузки. Не про лимит GitHub (там мегабайты), а про здравый смысл:
# каждая выгрузка это коммит, и репозиторий с историей многомегабайтных
# текстов становится неудобным очень быстро.
MAX_BYTES = int(os.environ.get("LOG_SHIP_MAX_KB", "512")) * 1024

# Хеш последней выгруженной версии: если ничего не изменилось, коммит не нужен.
_last_digest: str | None = None


def enabled() -> bool:
    return bool(TOKEN and REPO)


def status() -> str:
    """Одной строкой — что настроено. Токен наружу не показываем никогда."""
    if not TOKEN and not REPO:
        return "выгрузка лога на GitHub не настроена"
    if not TOKEN:
        return f"репозиторий {REPO} задан, но нет LOG_GITHUB_TOKEN"
    if not REPO:
        return "токен задан, но нет LOG_GITHUB_REPO (нужен вид owner/repo)"
    return f"{REPO}, ветка {BRANCH}, файл {PATH}, раз в {INTERVAL_MINUTES:g} мин"


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


async def check(session: aiohttp.ClientSession | None = None) -> str:
    """
    Проверить настройку до первой выгрузки: жив ли токен, есть ли репозиторий
    и — главное — не публичный ли он.

    Возвращает человеческое описание. Бросает LogShipError, если писать нельзя.
    """
    if not enabled():
        return status()

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
                raise LogShipError(
                    f"Репозиторий {REPO} не найден (404). Либо имя не то, либо у "
                    f"токена нет к нему доступа — для приватного репозитория "
                    f"нужен fine-grained токен с правом Contents: Read and write "
                    f"именно на этот репозиторий."
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
