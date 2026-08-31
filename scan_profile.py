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

import math
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
        lock_wait: float = 0.0, lanes: int = 0,
        retries: dict[str, int] | None = None,
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

        # Три слагаемых фазы Steam, и все лечатся разным:
        #   очередь на полосу — полос меньше, чем воркеров;
        #   пауза            — наше собственное расписание;
        #   сеть             — маршрут и скорость ответа Steam.
        # Пока очередь считалась «сетью», отчёт показывал 11.8 с сетевой
        # задержки там, где большую часть занимало стояние за другим воркером.
        network = max(self.steam - throttle - lock_wait, 0.0)
        lines.append("<b>Где узкое место</b>")
        lines.append(f"  очередь на полосу: {lock_wait:.1f} с = {lock_wait / capacity * 100:.0f}% ёмкости")
        lines.append(f"  пауза троттлинга: {throttle:.1f} с = {throttle / capacity * 100:.0f}% ёмкости")
        lines.append(f"  ожидание сети: {network:.1f} с = {network / capacity * 100:.0f}% ёмкости")

        reqs = self.steam_requests
        if reqs:
            lines.append(
                f"  на запрос: очередь {lock_wait / reqs:.2f} с + пауза "
                f"{throttle / reqs:.2f} с + сеть {network / reqs:.2f} с"
            )
            # Сколько воркеров нужно, чтобы сеть полностью пряталась за паузой.
            # Больше этого числа не даёт ничего: полоса всё равно выпускает
            # один запрос в interval секунд.
            need = math.ceil((network / reqs) / interval) if interval else workers
            lines.append(
                f"  воркеров {workers}, чтобы спрятать сеть хватает {need}"
                + (" — лишние только стоят в очереди" if workers > need else "")
            )
        lines.append(
            f"  пауза {interval:g} с"
            + (f", полос задействовано {lanes}" if lanes else "")
        )

        # Сколько полос РАБОТАЛО на самом деле — не по числу адресов, а по
        # пропускной способности: одна полоса выпускает запрос раз в interval
        # секунд, значит (запросы × интервал) / время прогона и есть число
        # полос, поделивших между собой поток.
        #
        #   541 × 4 / 2172 = 1.00  → одна полоса, прогон ею и связан
        #   144 × 2 / 207  = 1.39  → полос было больше одной
        #
        # Считать «полосой» адрес, по которому ушёл один запрос из пятисот,
        # нельзя — он ничего не разгрузил.
        effective = (reqs * interval / wall) if wall else 0.0
        lines.append(f"  {_verdict(effective, throttle, network, lock_wait)}")
        if reqs and wall:
            lines.append(
                f"  <i>запросы × пауза ÷ время = {effective:.2f} — столько полос "
                f"реально делили поток</i>"
            )
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
            share = total_retries / reqs * 100 if reqs else 0
            lines.append(f"  ⚠️ из них повторов: {total_retries} ({detail}) — {share:.0f}% запросов впустую")
            if retries.get("429"):
                lines.append(
                    "     каждый 429 выбивает адрес из пула — оставшимся достаётся "
                    "больше нагрузки, и следующий 429 вероятнее"
                )
        lines.append("")

        lines.append("<b>Воронка</b>")
        for key, label in FUNNEL:
            lines.append(f"  {label}: {getattr(self, key)}")
        if self.failed:
            lines.append(f"  предметов с ошибкой: {self.failed}")
        return "\n".join(lines)


_TIME_FIELDS = frozenset({"steam", "stickers", "analysis", "dedup", "send"})


def _verdict(effective_lanes: float, throttle: float,
             network: float, lock_wait: float) -> str:
    """
    Вывод одной строкой. Намеренно не «оптимизируйте X»: отчёт должен сказать,
    что мерить дальше, а не назначить виноватого.

    Первая проверка — насыщение единственной полосы, и это не эвристика, а
    тождество: если (запросы × интервал) равно времени прогона, значит запросы
    шли строго друг за другом по одной полосе, и никакая параллельность этого
    не изменит.

    Две прежние версии ошибались здесь по-разному. Первая проверяла занятость
    воркеров и на живом прогоне посоветовала добавить воркеров там, где их и так
    было больше нужного. Вторая сравнивала «запросы × интервал ≥ время × 0.9» и
    принимала за насыщение случай 288 ≥ 207 — а превышение как раз ДОКАЗЫВАЕТ,
    что полос было несколько.
    """
    # Именно ОКРЕСТНОСТЬ единицы, а не «меньше единицы»: значение сильно ниже
    # означает обратное — полоса простаивала, время ушло куда-то ещё.
    if 0.9 <= effective_lanes <= 1.1:
        return (
            "→ УПЁРЛИСЬ В ОДНУ ПОЛОСУ: время прогона = запросы × пауза. Помогут "
            "только меньше запросов, короче пауза или больше полос. Воркеры, "
            "кэши и батчи не дадут ничего."
        )
    if lock_wait > throttle and lock_wait > network:
        return (
            "→ больше всего стоим в ОЧЕРЕДИ на полосу: воркеров больше, чем полос. "
            "Помогут полосы, а не воркеры."
        )
    if throttle > network:
        return (
            "→ времени больше уходит в собственную паузу, чем в сеть. Ускорит "
            "МЕНЬШЕ запросов или больше полос."
        )
    return (
        "→ время уходит в сеть, а не в расписание. Помогут: убрать лишние "
        "запросы, ускорить маршрут, добавить воркеров до нужного числа выше."
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
