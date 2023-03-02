from time import sleep

from blockcrawler.core.stats import StatsService


def run():
    stats_service = StatsService()

    with stats_service.timer("timer"):
        sleep(0.1)
        stats_service.increment("counter")

    with stats_service.ms_counter("ms_counter"):
        sleep(0.1)
        stats_service.increment("counter")

    print("timer:", stats_service.get_timings("timer"))
    print("ms_counter:", stats_service.get_count("ms_counter"))
    print("counter:", stats_service.get_count("counter"))


if __name__ == "__main__":
    run()
