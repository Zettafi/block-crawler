import time
from contextlib import contextmanager
from typing import Dict, List


class StatsService:
    def __init__(self) -> None:
        self.__counters: Dict[str, int] = dict()
        self.__timers: Dict[str, List[int]] = dict()

    def increment(self, stat: str, quantity: int = 1):
        if stat in self.__counters:
            self.__counters[stat] += quantity
        else:
            self.__counters[stat] = quantity

    def get_count(self, stat: str):
        if stat in self.__counters:
            count = self.__counters[stat]
        else:
            count = 0
        return count

    @contextmanager
    def timer(self, stat: str):
        start = time.perf_counter_ns()
        try:
            yield None
        finally:
            end = time.perf_counter_ns()
            duration = end - start
            if stat not in self.__timers:
                self.__timers[stat] = list()
            self.__timers[stat].append(duration)

    def get_timings(self, stat):
        if stat in self.__timers:
            timings = self.__timers[stat]
        else:
            timings = list()
        return timings

    def reset(self):
        self.__counters.clear()
        self.__timers.clear()
