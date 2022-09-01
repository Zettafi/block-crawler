import time
import unittest
from asyncio import QueueEmpty
from unittest.mock import AsyncMock, call
from unittest.mock import MagicMock

from chainconductor.blockcrawler.processors.queued_batch import (
    QueuedAcquisitionStrategy,
    DevNullDispositionStrategy,
    QueuedDispositionStrategy,
    TypeQueuedDispositionStrategy,
)


class QueuedAcquisitionStrategyTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__max_batch_size = 10
        self.__max_batch_wait = 999
        self.__queue = MagicMock(side_effect=iter(range(self.__max_batch_size)))
        self.__strategy = QueuedAcquisitionStrategy(
            self.__queue, self.__max_batch_size, self.__max_batch_wait
        )

    async def test_calls_get_on_queue_until_max_batch_size_reached(self):
        await self.__strategy()
        self.assertEqual(10, self.__queue.get_nowait.call_count)

    async def test_stops_and_returns_items_gathered_when_max_wait_exceeded(self):
        items = [1, 2, None, 3]
        start = time.perf_counter_ns()
        max_batch_wait = 0.01

        def __time_tester():
            item = items.pop(0)
            if item is None:
                if time.perf_counter_ns() - start < 1_000_000_000 * max_batch_wait:
                    items.insert(0, None)
                    raise QueueEmpty()
            return item

        self.__queue.get_nowait.side_effect = __time_tester
        strategy = QueuedAcquisitionStrategy(self.__queue, 999, max_batch_wait)

        actual = await strategy()
        self.assertEqual([1, 2], actual)

    async def test_returns_chunks_of_batch_size(self):
        self.__queue.get_nowait.side_effect = iter(range(self.__max_batch_size * 3))
        self.assertEqual(
            self.__max_batch_size, len(await self.__strategy()), "First batch is unexpected size"
        )
        self.assertEqual(
            self.__max_batch_size, len(await self.__strategy()), "Second batch is unexpected size"
        )
        self.assertEqual(
            self.__max_batch_size, len(await self.__strategy()), "Third batch is unexpected size"
        )


class DevNullDispositionStrategyTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_does_nothing_with_batch(self):
        self.assertIsNone(await DevNullDispositionStrategy()([1, 2, 3]))


class QueuedDispositionStrategyTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__queue_1 = AsyncMock()
        self.__queue_2 = AsyncMock()
        self.__strategy = QueuedDispositionStrategy(self.__queue_1, self.__queue_2)

    async def test_sends_items_to_queues(self):
        await self.__strategy([1, 2, 3])
        expected = [call(1), call(2), call(3)]
        self.__queue_1.put.assert_has_awaits(expected)
        self.__queue_2.put.assert_has_awaits(expected)

    async def test_sends_non_along(self):
        await self.__strategy(None)
        self.__queue_1.put.assert_awaited_once_with(None)
        self.__queue_2.put.assert_awaited_once_with(None)


class TypeQueuedDispositionStrategyTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__int_queue = AsyncMock()
        self.__str_queue = AsyncMock()
        self.__strategy = TypeQueuedDispositionStrategy(
            (int, self.__int_queue),
            (str, self.__str_queue),
        )

    async def test_sends_type_values_to_typed_queues(self):
        await self.__strategy([1, "two", 3.0])
        self.__int_queue.put.assert_awaited_once_with(1)
        self.__str_queue.put.assert_awaited_once_with("two")

    async def test_sends_none_values_to_all_queues(self):
        await self.__strategy(None)
        self.__int_queue.put.assert_awaited_once_with(None)
        self.__str_queue.put.assert_awaited_once_with(None)
