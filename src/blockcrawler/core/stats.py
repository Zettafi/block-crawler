"""Class for tracking performance statistics"""
import asyncio
import logging

import time
from asyncio import CancelledError
from contextlib import contextmanager
from typing import Dict, List, Callable

from blockcrawler import LOGGER_NAME


class StatsService:
    """Service for tracking and retrieving statistics stored in memory"""

    def __init__(self) -> None:
        self.__counters: Dict[str, int] = {}
        self.__timers: Dict[str, List[int]] = {}
        self.__point_in_time_counters: Dict[str, int] = {}

    def increment(self, stat: str, quantity: int = 1) -> None:
        """
        Increment a counter `stat` statistic by a `quantity`

        :param stat: Counter stat name to be incremented
        :param quantity: Value for which the stat will be incremented
        """
        if stat in self.__counters:
            self.__counters[stat] += quantity
        else:
            self.__counters[stat] = quantity

    def get_count(self, stat: str) -> int:
        """
        Get the current count of a counter `stat`

        :param stat: Counter stat name to retrieve

        """
        if stat in self.__counters:
            count = self.__counters[stat]
        else:
            count = 0
        return count

    @contextmanager
    def ms_counter(self, stat: str):
        """
        Return a context manager that will determine the number of milliseconds
        spent within the context and add that value to the counter `stat`. This method
        differs from the `timer()` method as it will not store a record for each time
        it is called and can be utilized for long-running operations and high frequency
        code execution without concern for memory usage.

        :param stat: Counter stat name to which the number of milliseconds will be added

        Example::

            with stats_service.ms_counter("timer"):
                sleep(1.0)

        The above example should add 100 to the counter stat "timer".
        """
        start = time.perf_counter_ns()
        try:
            yield None
        finally:
            end = time.perf_counter_ns()
            duration = int((end - start) / 1_000_000)
            self.increment(stat, duration)

    @contextmanager
    def timer(self, stat: str):
        """
        Return a context manager that will determine the number of nanoseconds
        spent within the context and add an item to a list of timings associated with
        `stat`. This method differs from the `ms_counter()` method as it will store an
        item for each time it is called which can raise memory concerns for high-volume
        and long-running processes.

        :param stat: Counter stat name to which the number of nanoseconds will be
            recorded

        Example::
            with stats_service.ms_counter("timer"):
                sleep(0.001)

        The above example should add an item with teh value 1,000,000 to the timing stat
        "timer".
        """
        start = time.perf_counter_ns()
        try:
            yield None
        finally:
            end = time.perf_counter_ns()
            duration = end - start
            if stat not in self.__timers:
                self.__timers[stat] = []
            self.__timers[stat].append(duration)

    def get_timings(self, stat: str) -> List[int]:
        """
        Get the recorded timings for the provided `stat`

        :param stat: Name of stat for which you wish to retrieve the timings

        :returns:  A list the number of nanoseconds recorded for the stat

        """
        if stat in self.__timers:
            timings = self.__timers[stat]
        else:
            timings = []
        return timings

    def reset(self) -> None:
        """
        Reset all counter and timer stats to 0 and an empty list respectively.
        """
        self.__counters.clear()
        self.__timers.clear()
        self.__point_in_time_counters.clear()


def _safe_average(count: int, total: int) -> float:
    return 0.0 if count == 0 else total / count


class StatsWriter:
    def __init__(
        self, stats_service: StatsService, get_line_function: Callable[[StatsService], str]
    ) -> None:
        self.__stats_service = stats_service
        self.__logger = logging.getLogger(LOGGER_NAME)
        self.__get_line_function = get_line_function

    def write_line(self):
        logging.getLogger(LOGGER_NAME).info(self.__get_line_function(self.__stats_service))

    async def write_at_interval(self, interval: int):
        try:
            while True:
                await asyncio.sleep(interval)
                self.write_line()
        except CancelledError:
            pass
