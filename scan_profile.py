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
                     обходится один предмет». Обычно это и самый крупный рычаг:
                     первый живой замер показал 144 запроса на 100 предметов,
                     то есть треть работы Steam ушла в повторы после 429.

Доли считаются от ёмкости воркеров (workers × wall), а не от реального
времени — см. render(). На этом уже один раз обожглись: деление суммы по
воркерам на реальное время дало «пауза = 99%» там, где на самом деле было 25%,
и вывод получился противоположным.

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
        lanes: int = 0, retries: dict[str, int] | None = None,
    ) -> str:
        """
        Текстовый отчёт. wall — реальное время прогона, throttle — суммарное
        время в паузе троттлинга по ВСЕМ воркерам, lanes — сколько исходящих
        адресов участвовало, retries — повторы по причинам.

        Про знаменатель. Доли считаются от ЁМКОСТИ ВОРКЕРОВ (workers × wall), а
        не от реального времени. Первая версия делила сумму по воркерам на
        реальное время и на живом прогоне выдала «пауза = 99% времени», хотя на
        деле она занимала четверть: 206 секунд ожидания четырёх воркеров это
        четверть от 830 воркеро-секунд, а не 99% от 207. Вывод из-за этого
        получался прямо противоположный правде.
        """
        retries = retries or {}
        phases = [(label, getattr(self, key)) for key, label in PHASES]
        capacity = (wall * workers) or 1e-9

        lines = ["<b>Профиль прогона</b>", ""]
        lines.append(f"Время по факту: <b>{wall:.1f} с</b> на {self.items} предмет(ов)")
        if self.items:
            lines.append(f"На предмет: {wall / self.items:.2f} с")
        lines.append(
            f"Ёмкость: {capacity:.0f} воркеро-секунд ({workers} × {wall:.0f} с) — "
            f"проценты ниже от неё"
        )
        lines.append("")

        lines.append("<b>Фазы</b>")
        for label, value in phases:
            lines.append(f"  {label}: {value:.1f} с ({value / capacity * 100:.0f}%)")
        lines.append("")

        # Сеть и пауза требуют разных действий, поэтому и печатаются отдельно:
        # паузу лечит расписание, сеть — маршрут и число одновременных запросов.
        network = max(self.steam - throttle, 0.0)
        lines.append("<b>Где узкое место</b>")
        lines.append(f"  ожидание сети: {network:.1f} с = {network / capacity * 100:.0f}% ёмкости")
        lines.append(f"  пауза троттлинга: {throttle:.1f} с = {throttle / capacity * 100:.0f}% ёмкости")
        busy = (self.steam + self.stickers) / capacity * 100
        lines.append(f"  воркеры заняты: {busy:.0f}% времени")
        lines.append(
            f"  воркеров {workers}, пауза {interval:g} с"
            + (f", исходящих адресов {lanes}" if lanes else "")
        )
        if self.steam_requests:
            lines.append(f"  на запрос: сеть {network / self.steam_requests:.2f} с "
                         f"+ пауза {throttle / self.steam_requests:.2f} с")
        lines.append(f"  {_verdict(network, throttle, busy)}")
        lines.append("")

        lines.append("<b>Внешние операции</b>")
        lines.append(f"  запросов к Steam за листингами: {self.steam_requests}")
        lines.append(f"  запросов к Steam за ценами наклеек: {self.sticker_requests}")
        lines.append(f"  обращений к Upstash: {self.redis_calls}")
        if self.items:
            per_item = (self.steam_requests + self.sticker_requests + self.redis_calls) / self.items
            lines.append(f"  <b>на один предмет: {per_item:.1f}</b>")

        # Повторы — самая дорогая строка отчёта, потому что каждый 429 не только
        # тратит запрос, но и выбивает адрес из пула, из-за чего у оставшихся
        # растёт нагрузка и вероятность следующего 429.
        total_retries = sum(retries.values())
        if total_retries:
            detail = ", ".join(f"{k} {v}" for k, v in sorted(retries.items()) if v)
            share = total_retries / self.steam_requests * 100 if self.steam_requests else 0
            lines.append(f"  ⚠️ из них повторов: {total_retries} ({detail}) — {share:.0f}% запросов впустую")
            if retries.get("429"):
                lines.append(
                    f"     каждый 429 выбивает адрес из пула — оставшимся достаётся "
                    f"больше нагрузки, и следующий 429 вероятнее"
                )
        lines.append("")

        lines.append("<b>Воронка</b>")
        for key, label in FUNNEL:
            lines.append(f"  {label}: {getattr(self, key)}")
        if self.failed:
            lines.append(f"  предметов с ошибкой: {self.failed}")
        return "\n".join(lines)


_TIME_FIELDS = frozenset({"steam", "stickers", "analysis", "dedup", "send"})


def _verdict(network: float, throttle: float, busy_pct: float) -> str:
    """
    Вывод одной строкой. Намеренно не «оптимизируйте X»: отчёт должен сказать,
    что мерить дальше, а не назначить виноватого.
    """
    if throttle > network:
        return (
            "→ времени больше уходит в собственную паузу, чем в сеть. Ускорит "
            "МЕНЬШЕ запросов или больше исходящих адресов; воркеры не помогут."
        )
    if busy_pct >= 80:
        return (
            "→ воркеры насыщены ожиданием сети, а не паузой. Помогут: убрать "
            "лишние запросы, добавить воркеров, ускорить маршрут."
        )
    return (
        "→ воркеры простаивают, и пауза не главная. Смотри самую дорогую фазу "
        "выше — время уходит туда."
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
