import asyncio
import logging
import signal
import unittest
from dataclasses import dataclass
from typing import Any
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch, ANY, call

import blockcrawler
from blockcrawler.core.bus import (
    ParallelDataBus,
    DataPackage,
    Consumer,
    SignalManager,
)


@dataclass
class TestDataPackage(DataPackage):
    data: Any


class ParallelDataBusTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = ParallelDataBus()
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

    async def test_logs_errors_from_consumers_by_default(self):
        self.__consumer.receive.side_effect = Exception
        await self.__data_bus.register(self.__consumer)
        with self.assertLogs(blockcrawler.LOGGER_NAME, logging.ERROR):
            async with self.__data_bus:
                await self.__data_bus.send(TestDataPackage("Anything"))

    async def test_raises_exceptions_from_send_when_raise_on_exception_is_true(self):
        self.__consumer.receive.side_effect = Exception
        data_bus = ParallelDataBus(raise_on_exception=True)
        await data_bus.register(self.__consumer)
        with self.assertRaises(Exception):
            async with data_bus:
                await data_bus.send(TestDataPackage("Anything"))

    async def test_raises_exceptions_from_consumer_result_when_raise_on_exception_is_true(self):
        future = asyncio.get_running_loop().create_future()
        self.__consumer.receive.return_value = future
        future.set_exception(Exception())
        data_bus = ParallelDataBus(raise_on_exception=True)
        await data_bus.register(self.__consumer)
        with self.assertRaises(Exception):
            async with data_bus:
                await data_bus.send(TestDataPackage("Anything"))
                future.set_exception(Exception())

    async def test_consumption_errors_do_not_prevent_consumption_by_other_consumers(self):
        self.__consumer.receive.side_effect = Exception
        await self.__data_bus.register(self.__consumer)
        consumer_2 = AsyncMock(Consumer)
        await self.__data_bus.register(consumer_2)
        async with self.__data_bus:
            await self.__data_bus.send(TestDataPackage("Anything"))
        consumer_2.receive.assert_awaited_once()

    async def test_debug_logs_registering_consumer(self):
        with self.assertLogs(blockcrawler.LOGGER_NAME, logging.DEBUG) as cm:
            await self.__data_bus.register(self.__consumer)
            self.assertIn(f"DEBUG:{blockcrawler.LOGGER_NAME}:REGISTERED CONSUMER", cm.output)

    async def test_debug_logs_data_received_by_send_function(self):
        with self.assertLogs(blockcrawler.LOGGER_NAME, logging.DEBUG) as cm:
            data_package = TestDataPackage(data="Hello")
            await self.__data_bus.send(data_package)
            self.assertIn(f"DEBUG:{blockcrawler.LOGGER_NAME}:RECEIVED DATA PACKAGE", cm.output)

    async def test_debug_logs_providing_data_to_consumer(self):
        with self.assertLogs(blockcrawler.LOGGER_NAME, logging.DEBUG) as cm:
            data_package = TestDataPackage(data="Hello")
            await self.__data_bus.register(self.__consumer)
            await self.__data_bus.send(data_package)
            self.assertIn(
                f"DEBUG:{blockcrawler.LOGGER_NAME}:SENDING DATA PACKAGE TO CONSUMER", cm.output
            )


class SignalManagerTestCase(unittest.TestCase):
    @patch("blockcrawler.core.bus.signal")
    def test_entering_context_manager_registers_handler_for_all_signals(self, signal_patch):
        expected_calls = []
        for a in ["SIGABRT", "SIGBREAK", "SIGHUP", "SIGINT", "SIGQUIT", "SIGTERM"]:
            expected_calls.append(call(getattr(signal_patch, a), ANY))

        with SignalManager():
            pass

        signal_patch.signal.assert_has_calls(expected_calls)

    def test_exiting_context_manager_registers_original_handler_for_all_signals(self):
        original_handlers = []
        expected_handlers = []
        signals = ["SIGABRT", "SIGBREAK", "SIGHUP", "SIGINT", "SIGQUIT", "SIGTERM"]
        for sig_name in signals:
            try:
                sig = getattr(signal, sig_name)
                handler = Mock()
                orig_handler = signal.signal(sig, handler)
                original_handlers.append((sig, orig_handler))
                expected_handlers.append((sig, handler))
            except AttributeError:  # signal not support by OS
                pass

        with SignalManager():
            pass

        for sig, handler in expected_handlers:
            self.assertEqual(handler, signal.getsignal(sig))

        for sig, handler in original_handlers:
            signal.signal(sig, handler)

    def test_handles_signals_properly(self):
        try:
            sig = signal.SIGINT
        except AttributeError:
            # SIGINT is not available on Windows but SIGBREAK is
            sig = signal.SIGBREAK

        with SignalManager() as sm:
            signal.raise_signal(sig)

            self.assertTrue(sm.interrupted)
            self.assertEqual(sig.name, sm.interrupting_signal)
