from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, call

from chainconductor.blockcrawler.processors.direct_batch import (
    DirectBatchProcessor,
    DirectDispositionStrategy,
    TypedDirectDispositionStrategy,
)


class DirectBatchProcessorTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__disposition_strategy = AsyncMock()
        self.__batch_processor = AsyncMock()
        self.__processor = DirectBatchProcessor(
            self.__batch_processor, self.__disposition_strategy, 2, 5
        )

    async def test_call_sends_input_to_batch_processor(self):
        expected = ["expected"]
        await self.__processor(expected[:])
        self.__batch_processor.assert_awaited_once_with(expected)

    async def test_call_sends_input_to_batch_processor_in_batches(self):
        input_ = ["b1-1", "b1-2", "b2-1", "b2-2"]
        calls = [call(["b1-1", "b1-2"]), call(["b2-1", "b2-2"])]
        await self.__processor(input_)
        self.__batch_processor.assert_has_awaits(calls)

    async def test_call_sends_batch_processor_result_to_disposition_strategy(self):
        input_ = ["b1-1", "b1-2", "b2-1", "b2-2"]
        expected = ["b1-result", "b2-result"]
        self.__batch_processor.side_effect = [["b1-result"], ["b2-result"]]
        await self.__processor(input_)
        self.__disposition_strategy.assert_awaited_once_with(expected)


class DirectDispositionStrategyTestCaste(IsolatedAsyncioTestCase):
    async def test_call_sends_to_one_processor(self):
        processor = AsyncMock()
        strategy = DirectDispositionStrategy(processor)
        expected = ["expected", "result"]
        await strategy(expected[:])
        processor.assert_awaited_once_with(expected)

    async def test_call_sends_same_values_but_different_lists_to_multiple_processors(self):
        processor1 = AsyncMock()
        processor2 = AsyncMock()
        processor3 = AsyncMock()
        strategy = DirectDispositionStrategy(processor1, processor2, processor3)
        expected = ["expected", "result"]
        inputed = expected[:]
        await strategy(inputed)
        processor1.assert_awaited_once_with(expected)
        self.assertIsNot(processor1.call_args[0][0], inputed)
        processor2.assert_awaited_once_with(expected)
        self.assertIsNot(processor2.call_args[0][0], inputed)
        processor3.assert_awaited_once_with(expected)
        self.assertIsNot(processor3.call_args[0][0], inputed)


class TypedDirectDispositionStrategyTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__int_processor = AsyncMock()
        self.__str_processor = AsyncMock()
        self.__strategy = TypedDirectDispositionStrategy(
            (int, self.__int_processor),
            (str, self.__str_processor),
        )

    async def test_sends_type_values_to_typed_queues(self):
        await self.__strategy([1, "two", 3.0, 4, "five", 6.0])
        self.__int_processor.assert_awaited_once_with([1, 4])
        self.__str_processor.assert_awaited_once_with(["two", "five"])
