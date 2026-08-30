"""
Замер прогона вотчлиста: куда уходит время и сколько стоит один предмет.

Зачем отдельным модулем. Ускорять по ощущениям бессмысленно: у /scanall есть
общая очередь к Steam с паузой в несколько секунд на запрос, и если время
уходит в неё, то ни воркеры, ни батчи, ни кэши не дадут ничего. Отличить этот
случай от «тормозит разбор» можно только замером.

Два разных вопроса, и путать их нельзя:

  СКОЛЬКО ВРЕМЕНИ  — суммы по фазам. При параллельной обработке они БОЛЬШЕ
                     реального времени прогона, и это не ошибка: разница и
                     показывает, сколько удалось наложить друг на друга.

  СКОЛЬКО ОПЕРАЦИЙ — счётчики внешних вызовов. Вот они складываются честно, и
                     именно они отвечают на вопрос «во сколько запросов
                     обходится один предмет». Число операций на предмет
                     оптимизируется, а время — почти нет: время в основном
                     задано паузой троттлинга, то есть числом запросов.

Счётчики внешних вызовов снимаются разностью монотонных счётчиков в
steam_client и storage, а не прокидываются через стек вызовов: точек, где
ходят в сеть, много, и протаскивать профайлер в каждую значило бы менять
половину кода ради замера.
"""

from __future__ import annotations

import time

# Фазы в порядке, в котором они идут по одному предмету. Порядок важен: отчёт
# читают сверху вниз и ждут, что он совпадает с ходом работы.
PHASES: tuple[tuple[str, str], ...] = (
    ("steam", "Листинги Steam"),
    ("stickers", "Цены наклеек"),
    ("analysis", "Отбор офферов"),
    ("dedup", "Дедуп (Upstash)"),
    ("send", "Telegram"),
)

# Счётчики воронки — сколько осталось после каждого сужения.
FUNNEL: tuple[tuple[str, str], ...] = (
    ("listings", "лотов получено"),
    ("with_stickers", "из них с наклейками"),
    ("offers", "прошли отбор"),
    ("fresh", "новых (не слали раньше)"),
    ("sent", "отправлено в чат"),
)


class ScanProfile:
    """
    Разбивка прогона по фазам и счётчикам.

    Имена steam/compute/send сохранены с прежнего _ScanStats: на них завязан
    итоговый лог прогона, и переименование ничего бы не улучшило.
    """

    __slots__ = (
        "steam", "stickers", "analysis", "dedup", "send",
        "items", "failed",
        "listings", "with_stickers", "offers", "fresh", "sent",
        "steam_requests", "redis_calls", "sticker_requests",
    )

    def __init__(self) -> None:
        for name in self.__slots__:
            setattr(self, name, 0.0 if name in _TIME_FIELDS else 0)

    # --- накопление --------------------------------------------------------

    def add(self, phase: str, seconds: float) -> None:
        setattr(self, phase, getattr(self, phase) + seconds)

    def count(self, field: str, n: int = 1) -> None:
        setattr(self, field, getattr(self, field) + n)

    @property
    def compute(self) -> float:
        """Всё между получением листингов и отправкой — как в прежнем логе."""
        return self.stickers + self.analysis + self.dedup

    # --- отчёт -------------------------------------------------------------

    def render(
        self, *, wall: float, workers: int, interval: float, throttle: float,
    ) -> str:
        """
        Текстовый отчёт. wall — реальное время прогона, throttle — сколько из
        него простояли в паузе троттлинга.
        """
        phases = [(label, getattr(self, key)) for key, label in PHASES]
        total = sum(v for _, v in phases) or 1e-9

        lines = ["<b>Профиль прогона</b>", ""]
        lines.append(f"Время по факту: <b>{wall:.1f} с</b> на {self.items} предмет(ов)")
        if self.items:
            lines.append(f"На предмет: {wall / self.items:.2f} с")
        lines.append("")

        lines.append("<b>Фазы</b> <i>(суммы по всем предметам, идут внахлёст —")
        lines.append(f"поэтому больше {wall:.1f} с реального времени)</i>")
        for label, value in phases:
            lines.append(f"  {label}: {value:.1f} с ({value / total * 100:.0f}%)")
        lines.append(f"  <b>всего работы: {total:.1f} с</b>")
        lines.append("")

        # Главное число отчёта. Если пауза съедает большую часть — оптимизация
        # разбора, кэшей и параллельности не даст ничего: прогон упирается не в
        # вычисления, а в собственное расписание запросов к Steam.
        share = throttle / wall * 100 if wall else 0.0
        lines.append("<b>Где узкое место</b>")
        lines.append(
            f"  пауза троттлинга: {throttle:.1f} с — {share:.0f}% реального времени"
        )
        lines.append(f"  воркеров: {workers}, пауза между запросами: {interval:g} с")
        lines.append(f"  {_verdict(share)}")
        lines.append("")

        lines.append("<b>Внешние операции</b>")
        lines.append(f"  запросов к Steam за листингами: {self.steam_requests}")
        lines.append(f"  запросов к Steam за ценами наклеек: {self.sticker_requests}")
        lines.append(f"  обращений к Upstash: {self.redis_calls}")
        if self.items:
            per_item = (self.steam_requests + self.sticker_requests + self.redis_calls) / self.items
            lines.append(f"  <b>на один предмет: {per_item:.1f}</b>")
        lines.append("")

        lines.append("<b>Воронка</b>")
        for key, label in FUNNEL:
            lines.append(f"  {label}: {getattr(self, key)}")
        if self.failed:
            lines.append(f"  предметов с ошибкой: {self.failed}")
        return "\n".join(lines)


_TIME_FIELDS = frozenset({"steam", "stickers", "analysis", "dedup", "send"})


def _verdict(throttle_share: float) -> str:
    """
    Вывод одной строкой. Намеренно не «оптимизируйте X»: отчёт должен сказать,
    что мерить дальше, а не назначить виноватого.
    """
    if throttle_share >= 60:
        return (
            "→ прогон упирается в собственную паузу к Steam. Ускорит только "
            "МЕНЬШЕ запросов или короче пауза; воркеры и кэши не дадут ничего."
        )
    if throttle_share >= 25:
        return (
            "→ пауза заметна, но не одна: смотри самую дорогую фазу выше — "
            "выигрыш поделится между ней и паузой."
        )
    return (
        "→ пауза не главная. Время уходит в фазу с наибольшим процентом — "
        "её и разбирать."
    )


class Phase:
    """
    Замер одной фазы: `with Phase(profile, "stickers"): ...`

    Синхронный контекст поверх await внутри работает верно — засекается
    время от входа до выхода, включая ожидание.
    """

    __slots__ = ("_profile", "_phase", "_t0")

    def __init__(self, profile: ScanProfile | None, phase: str) -> None:
        self._profile = profile
        self._phase = phase
        self._t0 = 0.0

    def __enter__(self) -> "Phase":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc) -> None:
        if self._profile is not None:
            self._profile.add(self._phase, time.perf_counter() - self._t0)
        return None
