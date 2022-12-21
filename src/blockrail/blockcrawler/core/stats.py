import asyncio
import datetime
import os
import time
from asyncio import Task
from contextlib import contextmanager
from typing import Dict, List, ContextManager, Optional

import click
import psutil

from blockrail.blockcrawler.core.bus import Consumer, DataPackage
from blockrail.blockcrawler.evm.data_packages import EvmBlockDataPackage
from blockrail.blockcrawler.nft.data_packages import (
    TokenTransferDataPackage,
    CollectionDataPackage,
    TokenMetadataUriUpdatedDataPackage,
)
from blockrail.blockcrawler.nft.entities import TokenTransactionType


class StatsService:
    def __init__(self) -> None:
        self.__counters: Dict[str, int] = dict()
        self.__timers: Dict[str, List[int]] = dict()
        self.__point_in_time_counters: Dict[str, int] = dict()

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
    def ms_counter(self, stat: str):
        start = time.perf_counter_ns()
        try:
            yield None
        finally:
            end = time.perf_counter_ns()
            duration = int((end - start) / 1_000_000)
            self.increment(stat, duration)

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

    def set_point_in_time_counter(self, stat: str, count: int):
        self.__point_in_time_counters[stat] = count

    def get_point_in_time_counter(self, stat: str):
        self.__point_in_time_counters.get(stat, 0)

    def reset(self):
        self.__counters.clear()
        self.__timers.clear()
        self.__point_in_time_counters.clear()


class StatsPrintingConsumer(Consumer, ContextManager):
    def __init__(self, starting_block: int, ending_block: int, print_interval=60) -> None:
        self.__starting_block = starting_block
        self.__ending_block = ending_block
        self.__print_interval = print_interval
        self.__collections = 0
        self.__mints = 0
        self.__transfers = 0
        self.__burns = 0
        self.__metadata_updates = 0
        self.__blocks = 0
        self.__in_flight = 0
        self.__progress_bar = click.progressbar(
            length=abs(starting_block - ending_block), item_show_func=self.__item_show_func
        )
        self.__print_status_task: Optional[Task] = None

    def __enter__(self):
        self.__progress_bar.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.__progress_bar.__exit__(exc_type, exc_value, tb)
        if self.__print_status_task:
            self.__print_status_task.cancel()
        if self.__progress_bar.is_hidden:
            self.__print_status()

    def __item_show_func(self, item):
        memory = psutil.Process(os.getpid()).memory_info().rss / 1024**2
        current = "N/A" if item is None else f"{item:,}"
        return (
            f"{current} - "
            f"B: {self.blocks:,} "
            f"C: {self.collections:,} "
            f"T: {self.mints:,}/{self.burns:,}/{self.transfers:,} "
            f"M: {self.metadata_updates:,} "
            f"I: {self.in_flight:,} [{memory:0,.2F}MB]"
        )

    async def receive(self, data_package: DataPackage):
        if isinstance(data_package, TokenTransferDataPackage):
            if data_package.token_transfer.transaction_type == TokenTransactionType.TRANSFER:
                self.__transfers += 1
            elif data_package.token_transfer.transaction_type == TokenTransactionType.MINT:
                self.__mints += 1
            elif data_package.token_transfer.transaction_type == TokenTransactionType.BURN:
                self.__burns += 1
        elif isinstance(data_package, CollectionDataPackage):
            self.__collections += 1
        elif isinstance(data_package, TokenMetadataUriUpdatedDataPackage):
            self.__metadata_updates += 1
        elif isinstance(data_package, EvmBlockDataPackage):
            self.__blocks += 1
            self.__progress_bar.update(1, data_package.block.number.int_value)

        if self.__progress_bar.is_hidden and self.__print_status_task is None:
            self.__print_status_task = asyncio.create_task(
                self.__print_status_loop(self.__print_interval)
            )

    async def __print_status_loop(self, interval: int):
        while True:
            self.__print_status()
            await asyncio.sleep(interval)

    def __print_status(self):
        click.echo(
            f"{datetime.datetime.utcnow():%Y-%m-%dT%H:%M:%S+00:00} "
            f"{self.__progress_bar.format_pct()} "
            f"{self.__progress_bar.format_eta()} "
            f"{self.__item_show_func(self.__progress_bar.current_item)} "
        )

    @property
    def blocks(self) -> int:
        return self.__blocks

    @property
    def collections(self) -> int:
        return self.__collections

    @property
    def transfers(self) -> int:
        return self.__transfers

    @property
    def mints(self) -> int:
        return self.__mints

    @property
    def burns(self) -> int:
        return self.__burns

    @property
    def metadata_updates(self) -> int:
        return self.__metadata_updates

    @property
    def in_flight(self):
        return self.__in_flight
