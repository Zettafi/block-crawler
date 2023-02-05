from dataclasses import dataclass
from logging import Logger
from typing import Any
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock

from blockcrawler.core.bus import (
    ParallelDataBus,
    DataPackage,
    Consumer,
)


@dataclass
class TestDataPackage(DataPackage):
    data: Any


class ParallelDataBusTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__logger = Mock(Logger)
        self.__data_bus = ParallelDataBus(self.__logger)
        self.__consumer = AsyncMock(Consumer)

    async def test_passes_sent_data_to_registered_consumers(self):
        expected = TestDataPackage("Expected Data")
        consumers = list()
        for _ in range(10):
            consumer = AsyncMock(Consumer)
            await self.__data_bus.register(consumer)
            consumers.append(consumer)
        async with self.__data_bus:
            await self.__data_bus.send(expected)
        for consumer in consumers:
            consumer.receive.assert_awaited_once_with(expected)

    async def test_logs_errors_from_consumers(self):
        self.__consumer.receive.side_effect = Exception
        await self.__data_bus.register(self.__consumer)
        async with self.__data_bus:
            await self.__data_bus.send(TestDataPackage("Anything"))
        self.__logger.exception.assert_called_once()

    async def test_consumption_errors_do_not_prevent_consumption_by_other_consumers(self):
        self.__consumer.receive.side_effect = Exception
        await self.__data_bus.register(self.__consumer)
        consumer_2 = AsyncMock(Consumer)
        await self.__data_bus.register(consumer_2)
        async with self.__data_bus:
            await self.__data_bus.send(TestDataPackage("Anything"))
        consumer_2.receive.assert_awaited_once()

    async def test_debug_logs_registering_consumer(self):
        await self.__data_bus.register(self.__consumer)
        self.__logger.debug.assert_any_call(
            "REGISTERED CONSUMER", extra=dict(consumer=self.__consumer)
        )

    async def test_debug_logs_data_received_by_send_function(self):
        data_package = TestDataPackage(data="Hello")
        await self.__data_bus.send(data_package)
        self.__logger.debug.assert_any_call(
            "RECEIVED DATA PACKAGE", extra=dict(data_package=data_package)
        )

    async def test_debug_logs_providing_data_to_consumer(self):
        data_package = TestDataPackage(data="Hello")
        await self.__data_bus.register(self.__consumer)
        await self.__data_bus.send(data_package)
        self.__logger.debug.assert_any_call(
            "SENDING DATA PACKAGE TO CONSUMER",
            extra=dict(consumer=self.__consumer, data_package=data_package),
        )
