import csv
import pathlib
from typing import List, Tuple


class BlockTimeCacheManager:
    def __init__(self, cache_file: pathlib.Path) -> None:
        self.__cache_file = cache_file

    def get_block_times_from_cache(self) -> List[Tuple[int, int]]:
        block_times: List[Tuple[int, int]] = []
        try:
            with open(self.__cache_file, "r") as file:
                for block_id, timestamp in csv.reader(file):
                    block_times.append((int(block_id), int(timestamp)))
        except FileNotFoundError:
            pass
        return block_times

    def write_block_times_to_cache(self, block_timestamps: List[Tuple[int, int]]):
        with open(self.__cache_file, "w+") as file:
            csv_writer = csv.writer(file)
            for row in block_timestamps:
                csv_writer.writerow(row)
