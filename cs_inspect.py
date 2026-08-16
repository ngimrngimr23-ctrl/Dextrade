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


def _decode_varint_fields(payload: bytes) -> tuple[dict[int, int], set[int]]:
    """
    Проходит по protobuf-сообщению и достаёт varint-поля, корректно ПРОПУСКАЯ
    остальные (в т.ч. length-delimited вроде stickers) — иначе после первого же
    неизвестного поля разбор поедет вбок.
    Возвращает (varint-поля, номера ВСЕХ встреченных полей) — второе нужно для
    проверок валидности payload'а (см. _has_inspect_payload / _is_masked_payload).
    """
    fields: dict[int, int] = {}
    present: set[int] = set()
    pos = 0
    while pos < len(payload):
        tag, pos = _read_varint(payload, pos)
        field_number = tag >> 3
        wire_type = tag & 0x7
        present.add(field_number)
        if wire_type == 0:  # varint
            value, pos = _read_varint(payload, pos)
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
        if pos > len(payload):
            raise InspectDecodeError("поле выходит за границы payload'а")
    return fields, present


# Проверки "похоже ли это на осмысленный payload" — ровно как в эталоне
# (hasDecodedInspectPayload / isDecodedMaskedInspectPayload). Нужны потому,
# что masked-формат контрольной суммой не проверяется, и единственный способ
# убедиться, что мы сняли маску правильным ключом — увидеть осмысленные поля.
_FIELD_ITEMID = 2
_FIELD_INVENTORY = 13
_FIELD_ORIGIN = 14
_FIELD_STICKERS = 12
_FIELD_KEYCHAINS = 20
_FIELD_VARIATIONS = 22


def _has_inspect_payload(present: set[int]) -> bool:
    return bool(present & {
        _FIELD_ITEMID, _FIELD_DEFINDEX, _FIELD_PAINTINDEX, _FIELD_PAINTSEED,
        _FIELD_STICKERS, _FIELD_KEYCHAINS, _FIELD_VARIATIONS,
    })


def _is_masked_payload(present: set[int]) -> bool:
    return {
        _FIELD_ITEMID, _FIELD_DEFINDEX, _FIELD_PAINTINDEX,
        _FIELD_INVENTORY, _FIELD_ORIGIN,
    } <= present


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
    """Формат с ведущим нулевым байтом — здесь контрольная сумма ЕСТЬ и проверяется."""
    if len(buffer) < 5 or buffer[0] != 0:
        raise InspectDecodeError("не 'wrapped' формат")
    payload = buffer[1:-4]
    expected_checksum = struct.unpack(">I", buffer[-4:])[0]
    if _checksum(payload) != expected_checksum:
        raise InspectDecodeError("контрольная сумма не сошлась")
    fields, present = _decode_varint_fields(payload)
    if not _has_inspect_payload(present):
        raise InspectDecodeError("payload разобрался, но в нём нет ожидаемых полей предмета")
    return fields


def _decode_masked(buffer: bytes) -> dict[int, int]:
    """
    Формат с XOR-маской (ключ = первый байт). ВАЖНО: контрольная сумма здесь
    НЕ проверяется — эталонная реализация CSFloat её тоже не проверяет, и это
    не случайность: раньше я добавил сюда проверку "по аналогии" с wrapped, и
    она отсекала АБСОЛЮТНО ВСЕ реальные ссылки со Steam (в логах — decode_error
    на всех 25 из 25). Вместо суммы корректность снятия маски подтверждается
    тем, что payload разобрался и в нём есть осмысленный набор полей.
    """
    if len(buffer) < 5:
        raise InspectDecodeError("буфер слишком короткий")
    unmasked = _xor_mask(buffer, buffer[0])
    if unmasked[0] != 0:
        raise InspectDecodeError("не 'masked' формат")
    fields, present = _decode_varint_fields(unmasked[1:-4])
    if not _is_masked_payload(present):
        raise InspectDecodeError("payload разобрался, но набор полей не похож на masked-предмет")
    return fields


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
        # Предмет разобрался, но флоата у него нет (бывает у предметов без
        # износа — кейсы, наклейки, брелоки). Это не ошибка разбора.
        raise InspectDecodeError("у предмета нет поля paintwear (предмет без износа)")

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
      (None, "no_link")           — ссылки нет вообще
      (None, "legacy_link")       — старый формат (S/A/D/M-параметры, без hex-блока):
                                    флоат в самой ссылке не записан, локально взять
                                    его неоткуда, нужен Steam GC или сторонний сервис
      (None, "decode_error: ...") — hex-блок есть, но раскодировать не вышло;
                                    после двоеточия — конкретная причина

    Причину возвращаем отдельно и с подробностью, чтобы вызывающий код мог
    честно отличить "нечего декодировать" от "декодер сломан" и показать в
    логе, ЧТО именно не сошлось — раньше оба случая молча сливались в None
    и диагностика вводила в заблуждение.
    """
    if not link:
        return None, "no_link"

    match = _HEX_RE.search(_normalize_link(link))
    if not match:
        return None, "legacy_link"

    try:
        return decode_hex(match.group(1)), "ok"
    except InspectDecodeError as e:
        return None, f"decode_error: {e}"
