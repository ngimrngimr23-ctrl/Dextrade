"""
Локальный (без единого сетевого запроса) декодер inspect-ссылок CS2.

С марта 2026 Valve поменяла формат: данные предмета (флоат, paintseed,
paintindex и т.д.) закодированы прямо в самой inspect-ссылке — раньше
за ними надо было идти на Steam Game Coordinator (живой запрос через бота,
залогиненного в игру), теперь всё уже внутри строки. Формат — protobuf
(сообщение CEconItemPreviewDataBlock), обёрнутый в 1 байт-маркер + CRC32
контрольную сумму, всё это в hex после "csgo_econ_action_preview ".

Портировано с эталонной реализации CSFloat (TypeScript, MIT):
https://github.com/csfloat/cs-inspect-serializer

ВАЖНО: не все ссылки уже в новом формате — часть по-прежнему старого стиля
(S<id>A<id>D<id>M<id>, без хекс-блока), для них тут просто вернётся None:
без живого запроса к Steam GC или стороннему сервису вроде CSFloat API их
не раскодировать, а этот модуль сознательно не делает сетевых запросов.
"""

from __future__ import annotations

import re
import struct
import urllib.parse
import zlib

# Поля CEconItemPreviewDataBlock, которые нас интересуют (varint, wire type 0).
# Полная схема: https://github.com/csfloat/cs-inspect-serializer/blob/master/src/econ.ts
_FIELD_DEFINDEX = 3
_FIELD_PAINTINDEX = 4
_FIELD_PAINTWEAR = 7
_FIELD_PAINTSEED = 8

_HEX_RE = re.compile(
    r"csgo_econ_action_preview\s+([0-9A-Fa-f]+)\s*$"
)


class InspectDecodeError(ValueError):
    """Ссылка не в новом самодостаточном формате (или повреждена) — раскодировать локально нельзя."""


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise InspectDecodeError("varint обрывается за концом буфера")
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7


def _decode_varint_fields(payload: bytes, wanted: set[int]) -> dict[int, int]:
    """
    Проходит по protobuf-сообщению и достаёт только нужные varint-поля,
    корректно ПРОПУСКАЯ остальные (в т.ч. length-delimited вроде stickers) —
    иначе после первого же неизвестного поля разбор поедет вбок.
    """
    fields: dict[int, int] = {}
    pos = 0
    while pos < len(payload):
        tag, pos = _read_varint(payload, pos)
        field_number = tag >> 3
        wire_type = tag & 0x7
        if wire_type == 0:  # varint
            value, pos = _read_varint(payload, pos)
            if field_number in wanted:
                fields[field_number] = value
        elif wire_type == 1:  # 64-bit fixed
            pos += 8
        elif wire_type == 2:  # length-delimited (строки, вложенные сообщения, repeated)
            length, pos = _read_varint(payload, pos)
            pos += length
        elif wire_type == 5:  # 32-bit fixed
            pos += 4
        else:
            raise InspectDecodeError(f"неизвестный wire type {wire_type}")
    return fields


def _checksum(payload: bytes) -> int:
    """CRC32-контрольная сумма payload'а — та же формула, что в cs-inspect-serializer."""
    crc = zlib.crc32(b"\x00" + payload) & 0xFFFFFFFF
    x_crc = (crc & 0xFFFF) ^ ((len(payload) * crc) & 0xFFFFFFFF)
    return x_crc & 0xFFFFFFFF


def _bytes_to_float(uint_value: int) -> float:
    """paintwear хранится как raw-биты float32 внутри uint32 (little-endian)."""
    return struct.unpack("<f", struct.pack("<I", uint_value & 0xFFFFFFFF))[0]


def _xor_mask(buffer: bytes, key: int) -> bytes:
    return bytes(b ^ key for b in buffer)


def _decode_wrapped(buffer: bytes) -> dict[int, int]:
    if len(buffer) < 5 or buffer[0] != 0:
        raise InspectDecodeError("не 'wrapped' формат")
    payload = buffer[1:-4]
    expected_checksum = struct.unpack(">I", buffer[-4:])[0]
    if _checksum(payload) != expected_checksum:
        raise InspectDecodeError("контрольная сумма не сошлась")
    return _decode_varint_fields(payload, {_FIELD_DEFINDEX, _FIELD_PAINTINDEX, _FIELD_PAINTWEAR, _FIELD_PAINTSEED})


def _decode_masked(buffer: bytes) -> dict[int, int]:
    if len(buffer) < 5:
        raise InspectDecodeError("буфер слишком короткий")
    unmasked = _xor_mask(buffer, buffer[0])
    if unmasked[0] != 0:
        raise InspectDecodeError("не 'masked' формат")
    payload = unmasked[1:-4]
    expected_checksum = struct.unpack(">I", unmasked[-4:])[0]
    if _checksum(payload) != expected_checksum:
        raise InspectDecodeError("контрольная сумма не сошлась (masked)")
    return _decode_varint_fields(payload, {_FIELD_DEFINDEX, _FIELD_PAINTINDEX, _FIELD_PAINTWEAR, _FIELD_PAINTSEED})


def decode_hex(hex_str: str) -> dict:
    hex_str = hex_str.strip()
    if not hex_str or len(hex_str) % 2 != 0:
        raise InspectDecodeError("нечётная длина hex-строки")
    try:
        buffer = bytes.fromhex(hex_str)
    except ValueError as e:
        raise InspectDecodeError(f"невалидный hex: {e}") from e

    if buffer and buffer[0] == 0:
        fields = _decode_wrapped(buffer)
    else:
        fields = _decode_masked(buffer)

    if _FIELD_PAINTWEAR not in fields:
        raise InspectDecodeError("в сообщении нет поля paintwear")

    return {
        "floatvalue": _bytes_to_float(fields[_FIELD_PAINTWEAR]),
        "defindex": fields.get(_FIELD_DEFINDEX),
        "paintindex": fields.get(_FIELD_PAINTINDEX),
        "paintseed": fields.get(_FIELD_PAINTSEED),
    }


def _normalize_link(link: str) -> str:
    """
    В HTML от Steam пробел перед hex-блоком закодирован как %20, а иногда
    ссылка приходит закодированной целиком — поэтому СНАЧАЛА раскодируем
    проценты, и только потом ищем hex. Эталонная реализация CSFloat делает
    ровно то же самое первым шагом (decodeURIComponentSafely); без этого
    регулярка с \\s+ не совпадёт никогда и любая, даже валидная новая
    ссылка будет молча выглядеть как "старый формат".
    """
    try:
        return urllib.parse.unquote(link.strip())
    except Exception:
        return link.strip()


def decode_inspect_link(link: str) -> tuple[dict | None, str]:
    """
    Пытается раскодировать inspect-ссылку ЛОКАЛЬНО, без единого сетевого запроса.

    Возвращает (данные, причина):
      ({'floatvalue':..., 'defindex':..., 'paintindex':..., 'paintseed':...}, "ok")
      (None, "no_link")      — ссылки нет вообще
      (None, "legacy_link")  — старый формат (S/A/D/M-параметры, без hex-блока):
                               флоат в самой ссылке не записан, локально взять
                               его неоткуда, нужен Steam GC или сторонний сервис
      (None, "decode_error") — hex-блок есть, но раскодировать не вышло

    Причину возвращаем отдельно, чтобы вызывающий код мог честно отличить
    "нечего декодировать" от "декодер сломан" — раньше оба случая молча
    сливались в None и диагностика вводила в заблуждение.
    """
    if not link:
        return None, "no_link"

    match = _HEX_RE.search(_normalize_link(link))
    if not match:
        return None, "legacy_link"

    try:
        return decode_hex(match.group(1)), "ok"
    except InspectDecodeError:
        return None, "decode_error"
