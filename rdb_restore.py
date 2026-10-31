"""
Перенос базы Upstash из резервной копии (.rdb) — присылают боту в Telegram.

2026-10-31 база упёрлась в месячный лимит запросов бесплатного тарифа, и
прочитать её командами стало нельзя ни боту, ни скрипту. Консоль Upstash
резервную копию отдаёт файлом RDB; бот разбирает его сам и пишет данные в
базу, к которой подключён, — токен новой базы никуда передавать не нужно.

Разбор минимальный: строки, множества, хэши, списки — то, что пишет бот.
Восстанавливает только в пустую базу (см. bot._restore_from_rdb).
"""
import struct


class RdbError(ValueError):
    """Файл похож на RDB, но разобрать его не вышло."""

def lzf_decompress(data, out_len):
    out = bytearray(); i = 0
    while i < len(data):
        ctrl = data[i]; i += 1
        if ctrl < 32:
            out += data[i:i + ctrl + 1]; i += ctrl + 1
        else:
            length = ctrl >> 5
            if length == 7:
                length += data[i]; i += 1
            ref = len(out) - ((ctrl & 0x1f) << 8) - data[i] - 1; i += 1
            for _ in range(length + 2):
                out.append(out[ref]); ref += 1
    if len(out) != out_len:
        raise RdbError("сжатая строка повреждена")
    return bytes(out)

class R:
    def __init__(self, b): self.b = b; self.p = 0
    def read(self, n): v = self.b[self.p:self.p + n]; self.p += n; return v
    def byte(self): v = self.b[self.p]; self.p += 1; return v
    def length(self):
        f = self.byte(); t = f >> 6
        if t == 0: return f & 0x3f, False
        if t == 1: return ((f & 0x3f) << 8) | self.byte(), False
        if t == 2:
            if f == 0x80: return struct.unpack(">I", self.read(4))[0], False
            if f == 0x81: return struct.unpack(">Q", self.read(8))[0], False
        return f & 0x3f, True
    def string(self):
        n, enc = self.length()
        if not enc: return self.read(n)
        if n == 0: return str(struct.unpack("b", self.read(1))[0]).encode()
        if n == 1: return str(struct.unpack("<h", self.read(2))[0]).encode()
        if n == 2: return str(struct.unpack("<i", self.read(4))[0]).encode()
        if n == 3:
            clen, _ = self.length(); ulen, _ = self.length()
            return lzf_decompress(self.read(clen), ulen)
        raise RdbError(f"неизвестная кодировка строки {n}")

def listpack(b):
    p = 6; out = []
    while b[p] != 0xFF:
        e = b[p]
        if e & 0x80 == 0: v = str(e & 0x7f).encode(); size = 1
        elif e & 0xC0 == 0x80: n = e & 0x3f; v = b[p+1:p+1+n]; size = 1 + n
        elif e & 0xE0 == 0xC0:
            x = ((e & 0x1f) << 8) | b[p+1]
            if x >= 1 << 12: x -= 1 << 13
            v = str(x).encode(); size = 2
        elif e & 0xF0 == 0xE0: n = ((e & 0x0f) << 8) | b[p+1]; v = b[p+2:p+2+n]; size = 2 + n
        elif e == 0xF0: n = struct.unpack("<I", b[p+1:p+5])[0]; v = b[p+5:p+5+n]; size = 5 + n
        elif e == 0xF1: v = str(struct.unpack("<h", b[p+1:p+3])[0]).encode(); size = 3
        elif e == 0xF2: x = int.from_bytes(b[p+1:p+4], "little", signed=True); v = str(x).encode(); size = 4
        elif e == 0xF3: v = str(struct.unpack("<i", b[p+1:p+5])[0]).encode(); size = 5
        elif e == 0xF4: v = str(struct.unpack("<q", b[p+1:p+9])[0]).encode(); size = 9
        else: raise RdbError(f"неизвестный элемент listpack {e:#x}")
        out.append(v); p += size
        bl = size; p += 1 if bl < 128 else 2 if bl < 16384 else 3 if bl < 2097152 else 4 if bl < 268435456 else 5
    return out

def intset(b):
    enc, n = struct.unpack("<II", b[:8]); fmt = {2: "h", 4: "i", 8: "q"}[enc]
    return [str(x).encode() for x in struct.unpack("<%d%s" % (n, fmt), b[8:8 + n * enc])]

def parse(b: bytes) -> dict:
    r = R(b)
    if r.read(5) != b"REDIS":
        raise RdbError("это не файл RDB")
    r.read(4)
    out = {}; expire = None
    while True:
        op = r.byte()
        if op == 0xFF: break
        if op == 0xFA: r.string(); r.string(); continue
        if op == 0xFE: r.length(); continue
        if op == 0xFB: r.length(); r.length(); continue
        if op == 0xFC: expire = struct.unpack("<Q", r.read(8))[0]; continue
        if op == 0xFD: expire = struct.unpack("<I", r.read(4))[0] * 1000; continue
        if op == 0xF8: r.byte(); continue
        if op == 0xF9: r.length(); continue
        if op == 0xF4: r.length(); r.length(); r.length(); continue  # slot info (Redis 7.4+/8)
        t = op; key = r.string()
        if t == 0: val = ("string", r.string())
        elif t == 2:
            n, _ = r.length(); val = ("set", [r.string() for _ in range(n)])
        elif t == 11: val = ("set", intset(r.string()))
        elif t == 20: val = ("set", listpack(r.string()))
        elif t == 4:
            n, _ = r.length(); val = ("hash", {r.string(): r.string() for _ in range(n)})
        elif t == 16:
            items = listpack(r.string()); val = ("hash", dict(zip(items[::2], items[1::2])))
        elif t == 18:
            n, _ = r.length(); items = []
            for _ in range(n):
                cont, _ = r.length(); items += listpack(r.string())
            val = ("list", items)
        elif t == 17:
            items = listpack(r.string()); val = ("zset", list(zip(items[::2], items[1::2])))
        else:
            raise RdbError(f"тип {t} у ключа {key[:60]!r} не поддерживается")
        out[key] = (val[0], val[1], expire); expire = None
    return out


# Кэш цен стикеров (тысячи ключей) не переносим: он больше не нужен (см.
# prewarm.py), а замки и кулдауны относятся к старому процессу.
SKIP_PREFIXES = (b"stickerprice", b"lock:", b"steam_cooldown:")


def restorable(data: dict) -> dict:
    """Что из копии писать в новую базу: всё, кроме кэша и служебного."""
    return {k: v for k, v in data.items() if not k.startswith(SKIP_PREFIXES)}
