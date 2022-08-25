import asyncio
from asyncio import Queue, Task, QueueEmpty
from datetime import timedelta, datetime
from logging import Logger
from typing import List, Tuple, Union, Callable, Coroutine, Any, Type

from chainconductor.blockcrawler.processors import TokenTransportObject

END_OF_RUN_MARKER = None


class QueuedBatchProcessor:
    """
    Batch processor that processes batch from an incoming queue that is passed down
    to a batch processor and send the processed results to one or more queues based on
    response object type.
    """

    def __init__(
        self,
        acquisition_strategy: Callable[[], Coroutine[Any, Any, List]],
        batch_processor: Callable[[List], Coroutine[Any, Any, List]],
        disposition_strategy: Callable[
            [Union[List, type(END_OF_RUN_MARKER)]], Coroutine[Any, Any, type(END_OF_RUN_MARKER)]
        ],
        logger: Logger,
        max_processors: int,
    ) -> None:
        self.__acquisition_strategy = acquisition_strategy
        self.__disposition_strategy = disposition_strategy
        self.__batch_processor = batch_processor
        self.__logger = logger
        self.__max_processors: int = max_processors
        self.__queued_batches = list()
        self.__running_tasks: List[Task] = list()
        self.__running: bool = False
        self.__stopping = False

    async def run(self):
        self.__running = True
        while self.__running:
            if len(self.__running_tasks) < self.__max_processors:
                batch_items = await self.__get_batch_items()
                if batch_items:
                    await self.__process_batch(batch_items)
            await self.__process_completed_batches()

        # We are no longer processing the queue,
        # so wait for all running tasks to complete
        while self.__running_tasks:
            await self.__process_completed_batches()
        await self.__send_end_of_run_marker_to_queues()

    async def __get_batch_from_acquisition_strategy(self):
        items = await self.__acquisition_strategy()
        try:
            end_marker_index = items.index(END_OF_RUN_MARKER)
            batch = items[:end_marker_index]
            self.__running = False
        except ValueError:
            # No end marker found
            batch = items
        return batch

    async def __process_batch(self, batch_items: List):
        loop = asyncio.get_running_loop()
        task = loop.create_task(self.__batch_processor(batch_items))
        self.__running_tasks.append(task)

    async def __process_completed_batches(self):
        await asyncio.sleep(0)  # Allow tasks to complete if I/O completed
        for i, task in enumerate(self.__running_tasks):
            if task.done():
                await self.__process_completed_task(task)
                del self.__running_tasks[i]

    async def __process_completed_task(self, task):
        # noinspection PyBroadException
        try:
            results = task.result()
            await self.__process_batch_processor_results(results)
        except Exception:
            # All error processing should have been handled by the batch processor
            # We are in a dire situation, drain the inbound data from the upstream
            # processes to end the process naturally
            self.__logger.exception("An error occurred processing the batch. Halting!")
            await self.__drain_acquisition_strategy()

    async def __process_batch_processor_results(self, batch_results: list):
        await self.__disposition_strategy(batch_results)

    async def __send_end_of_run_marker_to_queues(self):
        await self.__disposition_strategy(END_OF_RUN_MARKER)

    def __get_batch_items(self):
        if self.__queued_batches:
            batch_items = self.__queued_batches.pop(0)
        else:
            batch_items = self.__get_batch_from_acquisition_strategy()
        return batch_items

    async def __drain_acquisition_strategy(self):
        while self.__running:
            await self.__get_batch_from_acquisition_strategy()


class DevNullDispositionStrategy:
    async def __call__(self, batch_results: List):
        pass


class QueuedDispositionStrategy:
    def __init__(self, *queues: Queue) -> None:
        self.__queues: Tuple[Queue] = queues

    async def __call__(self, batch_results: Union[List, type(END_OF_RUN_MARKER)]):
        for queue in self.__queues:
            if batch_results is END_OF_RUN_MARKER:
                batch_results = [END_OF_RUN_MARKER]
            for batch_result in batch_results:
                await queue.put(batch_result)


class TypeQueuedDispositionStrategy:
    def __init__(self, *type_queues: Tuple[Type, Queue]) -> None:
        self.__type_queues: Tuple[Tuple[Type, Queue]] = type_queues[:]

    async def __call__(self, batch_results: Union[List, type(END_OF_RUN_MARKER)]):
        if batch_results == END_OF_RUN_MARKER:
            for _, queue in self.__type_queues:
                await queue.put(END_OF_RUN_MARKER)
        else:
            for batch_result in batch_results:
                for type, queue in self.__type_queues:
                    if isinstance(batch_result, type):
                        await queue.put(batch_result)


class MetadataQueuedDispositionStrategy:
    def __init__(self, storage_queue: Queue, capture_queue) -> None:
        self.__storage_queue = storage_queue
        self.__capture_queue = capture_queue

    async def __call__(
        self, batch_results: Union[List[TokenTransportObject], type(END_OF_RUN_MARKER)]
    ):
        if batch_results == END_OF_RUN_MARKER:
            await self.__capture_queue.put(END_OF_RUN_MARKER)
            # We're not sending end of run to the storage queue as it would stop
            # the storage processor before metadata capture was complete
        else:
            for batch_result in batch_results:
                if (
                    batch_result.token.metadata_uri is not None
                    and len(batch_result.token.metadata_uri) > 0
                ):
                    await self.__capture_queue.put(batch_result)
                else:
                    await self.__storage_queue.put(batch_result)


class QueuedAcquisitionStrategy:
    def __init__(self, queue: Queue, max_batch_size: int, max_batch_wait: int) -> None:
        self.__inbound_queue = queue
        self.__max_batch_size = max_batch_size
        self.__max_batch_wait = timedelta(seconds=max_batch_wait)

    async def __call__(self):
        last_batch_time = datetime.now()
        items = list()
        while True:
            try:
                item = self.__inbound_queue.get_nowait()
                items.append(item)
            except QueueEmpty:
                await asyncio.sleep(0.01)
            now = datetime.now()
            if (
                len(items) >= self.__max_batch_size
                or now - last_batch_time > self.__max_batch_wait
                or END_OF_RUN_MARKER in items
            ):
                if items:
                    break
                else:
                    last_batch_time = datetime.now()
        return items
