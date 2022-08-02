import unittest
import random

from chainconductor.blockcrawler.stats import StatsService


class TestStats(unittest.TestCase):
    def setUp(self) -> None:
        self.__stats_service = StatsService()

    def test_increment_increases_count(self):
        stats = random.randint(1, 1_000_000)
        for _ in range(stats):
            self.__stats_service.increment("stat")
        self.assertEqual(stats, self.__stats_service.get_count("stat"))

    def test_get_count_is_zero_for_non_incremented_stat(self):
        self.assertEqual(0, self.__stats_service.get_count("stat"))

    def test_timer_adds_timing_for_stat(self):
        stats = random.randint(1, 1_000_000)
        for _ in range(stats):
            with self.__stats_service.timer("stat"):
                pass

        timings = self.__stats_service.get_timings("stat")
        self.assertEqual(stats, len(timings))
        self.assertIsInstance(timings[0], int)

    def test_timer_adds_integer_timing_for_stat(self):
        with self.__stats_service.timer("stat"):
            pass

        timings = self.__stats_service.get_timings("stat")
        self.assertIsInstance(timings[0], int)

    def test_get_timings_is_empty_for_untimed_stat(self):
        timings = self.__stats_service.get_timings("stat")
        self.assertEqual(0, len(timings), "Expected zero timing events")
