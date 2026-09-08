"""
Логи в файл: ротация на диске, кольцевой буфер в памяти, чистка секретов.

Зачем это понадобилось. До сих пор бот писал только в stdout, то есть в панель
Render. Чтобы разобрать любой сбой, приходилось открывать панель, искать нужный
кусок глазами и копировать его руками — а панель показывает хвост и режет
длинные строки. Файл решает это: /logs отдаёт его целиком одним касанием.

ТРИ МЕСТА, А НЕ ОДНО, И У КАЖДОГО СВОЯ ПРИЧИНА.

  stdout      — как было. Панель Render остаётся рабочей, ничего не отнимаем.

  файл        — то, что уходит в /logs. С ротацией: без неё лог растёт, пока
                не кончится диск, а на Render это означает падение процесса.

  память      — кольцо последних строк. Нужно, потому что файловая система
                Render эфемерна и вдобавок может быть переполнена: если запись
                на диск не удалась, хвост лога всё равно останется доступен.
                Терять диагностику ровно в тот момент, когда что-то сломалось,
                нельзя — а ломается обычно всё сразу.

ЧИСТКА СЕКРЕТОВ ОБЯЗАТЕЛЬНА, И ВОТ ПОЧЕМУ. Файл уходит в чат, то есть наружу.
В логи попадают адреса прокси вместе с паролями (они лежат прямо в URL) и,
если кто-то поднимет уровень httpx до INFO, полный URL запроса к Telegram —
а токен бота часть этого URL. В bot.py про это есть отдельный комментарий:
там httpx намеренно приглушён именно по этой причине. Полагаться на то, что
никто никогда не вернёт уровень обратно, нельзя, поэтому чистим на выходе.
"""

from __future__ import annotations

import collections
import io
import logging
import logging.handlers
import envcfg
import os
from pathlib import Path

import scan_errors

# Сколько строк держать в памяти. Пять тысяч — это примерно один полный прогон
# вотчлиста со всей диагностикой, то есть ровно тот объём, который нужен, чтобы
# разобрать «что случилось в прошлый раз».
RING_LINES = envcfg.env_int("LOG_RING_LINES", 5000)

LOG_PATH = Path(os.environ.get("LOG_FILE", Path(__file__).parent / "dextrade.log"))
MAX_BYTES = envcfg.env_int("LOG_MAX_MB", 8) * 1024 * 1024
BACKUPS = envcfg.env_int("LOG_BACKUPS", 1)

FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"


class ScrubbingFormatter(logging.Formatter):
    """
    Форматтер, который вычищает секреты из готовой строки.

    Именно из ГОТОВОЙ, а не из аргументов: пароль может приехать и в тексте
    исключения, и в подставленном аргументе, и в самом сообщении. Чистить надо
    один раз в самом конце — тогда мимо не проскочит ничего.
    """

    def format(self, record: logging.LogRecord) -> str:
        return scan_errors.scrub(super().format(record))


class RingHandler(logging.Handler):
    """Последние RING_LINES строк в памяти — на случай, если диска нет."""

    def __init__(self, capacity: int = RING_LINES):
        super().__init__()
        self.buffer: collections.deque[str] = collections.deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.buffer.append(self.format(record))
        except Exception:
            # Обработчик логов не имеет права ронять то, что он логирует.
            pass

    def tail(self, lines: int | None = None) -> list[str]:
        data = list(self.buffer)
        return data if lines is None else data[-lines:]


class DirtyHandler(logging.Handler):
    """
    Поднимает флаг «появились новые строки». На нём держится выгрузка по
    событию вместо выгрузки по таймеру.

    ЗАПИСИ САМОЙ ВЫГРУЗКИ ИГНОРИРУЮТСЯ, и это не мелочь, а условие работы.
    Выгрузка пишет в лог «лог выгружен …» — то есть меняет файл. Если бы эта
    строка тоже поднимала флаг, получился бы вечный двигатель: выгрузили ->
    записали об этом -> файл изменился -> выгрузили снова, и так до конца
    времён с периодом в одну паузу.
    """

    IGNORED_PREFIX = "steam_bot.logship"

    def __init__(self):
        super().__init__()
        self.dirty = False

    def emit(self, record: logging.LogRecord) -> None:
        if record.name.startswith(self.IGNORED_PREFIX):
            return
        self.dirty = True

    def take(self) -> bool:
        """Прочитать и сбросить флаг разом — чтобы не потерять запись, пришедшую между."""
        was, self.dirty = self.dirty, False
        return was


_ring: RingHandler | None = None
_file_handler: logging.handlers.RotatingFileHandler | None = None
_dirty: DirtyHandler | None = None

# Сколько байт локального файла уже уехало на GitHub. Нужно, чтобы дописывать
# только новое, а не слать файл целиком каждый раз.
_shipped_offset = 0


def setup(level: int = logging.INFO) -> None:
    """
    Поднять логирование. Зовётся один раз при старте, повторный вызов не вредит.

    basicConfig здесь не годится: он ставит ровно один обработчик и молча
    ничего не делает, если обработчики уже есть.
    """
    global _ring, _file_handler

    root = logging.getLogger()
    root.setLevel(level)

    if _ring is not None:
        return

    formatter = ScrubbingFormatter(FORMAT, datefmt=DATEFMT)

    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    root.addHandler(stream)

    _ring = RingHandler()
    _ring.setFormatter(formatter)
    root.addHandler(_ring)

    global _dirty
    _dirty = DirtyHandler()
    root.addHandler(_dirty)

    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        _file_handler = logging.handlers.RotatingFileHandler(
            LOG_PATH, maxBytes=MAX_BYTES, backupCount=BACKUPS, encoding="utf-8",
        )
        _file_handler.setFormatter(formatter)
        root.addHandler(_file_handler)
    except Exception:
        # Диск может быть переполнен или недоступен — это не повод не
        # запускаться. Кольцо в памяти работает и без файла, а сказать об
        # этом надо через тот же лог, который уже стоит.
        logging.getLogger("steam_bot").exception(
            "не смог открыть файл лога %s — работаю только с памятью", LOG_PATH
        )


def available() -> bool:
    return _ring is not None


def has_new_records() -> bool:
    """Появились ли строки с прошлой проверки. Флаг сбрасывается чтением."""
    return _dirty.take() if _dirty is not None else False


def unshipped() -> str:
    """
    Строки, которых ещё нет в выгруженном файле.

    Отдаём хвост локального файла после _shipped_offset. Если файл стал
    короче отметки, значит его подрезала ротация или процесс перезапустился с
    чистым диском — начинаем читать сначала, иначе потеряли бы всё новое.
    """
    global _shipped_offset
    if _file_handler is None:
        return "\n".join(_ring.tail()) + "\n" if _ring and _ring.buffer else ""

    try:
        size = LOG_PATH.stat().st_size
        if size < _shipped_offset:
            _shipped_offset = 0
        if size == _shipped_offset:
            return ""
        with io.open(LOG_PATH, encoding="utf-8", errors="replace") as f:
            f.seek(_shipped_offset)
            return f.read()
    except Exception:
        return ""


def mark_shipped() -> None:
    """Запомнить, что локальный файл выгружен до текущей длины."""
    global _shipped_offset
    _shipped_offset = file_size()


def file_size() -> int:
    """Размер файла лога в байтах. Ноль — файла нет."""
    try:
        return LOG_PATH.stat().st_size
    except Exception:
        return 0


def dump(lines: int | None = None, min_level: int | None = None) -> bytes:
    """
    Лог в виде готового файла.

    Источник — файл, если он есть и в нём что-то лежит; иначе кольцо в памяти.
    Файл полнее (переживает ротацию), кольцо надёжнее (не зависит от диска),
    поэтому берём лучшее из доступного, а не одно раз и навсегда.

    min_level — отдать только строки этого уровня и выше. Фильтруем по тексту
    уже отформатированных строк: разбирать их обратно в LogRecord было бы
    и дороже, и хрупче, а формат мы задаём сами и знаем его.
    """
    text: list[str] = []

    if _file_handler is not None and file_size() > 0:
        try:
            with io.open(LOG_PATH, encoding="utf-8", errors="replace") as f:
                text = f.read().splitlines()
        except Exception:
            text = []
    if not text and _ring is not None:
        text = _ring.tail()

    if min_level is not None:
        names = [
            logging.getLevelName(lv)
            for lv in (logging.CRITICAL, logging.ERROR, logging.WARNING,
                       logging.INFO, logging.DEBUG)
            if lv >= min_level
        ]
        # Строка продолжения (трейсбек) уровня не несёт — тащим её за той
        # строкой, к которой она относится, иначе от исключения останется
        # только заголовок, а он без стека почти бесполезен.
        kept: list[str] = []
        keeping = False
        for line in text:
            # Начало записи узнаём по дате: формат задаём мы сами (FORMAT),
            # и год в начале строки — самый дешёвый признак. Всё, что без
            # даты, — продолжение предыдущей записи.
            if len(line) > 20 and line[:4].isdigit():
                keeping = any(f" {n} " in line for n in names)
            if keeping:
                kept.append(line)
        text = kept

    if lines is not None:
        text = text[-lines:]

    return ("\n".join(text) + "\n").encode("utf-8")


def stats() -> dict:
    """Что сказать пользователю про состояние логов."""
    return {
        "file": str(LOG_PATH),
        "file_bytes": file_size(),
        "ring_lines": len(_ring.buffer) if _ring else 0,
        "ring_capacity": RING_LINES,
    }
