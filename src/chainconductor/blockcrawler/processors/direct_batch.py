import asyncio
from asyncio import Task
from typing import List, Callable, Coroutine, Any, Tuple, Type

from chainconductor.blockcrawler.processors import TokenTransportObject


class DirectBatchProcessor:
    def __init__(
        self,
        batch_processor: Callable[[List], Coroutine[Any, Any, List]],
        disposition_strategy: Callable[[List], Coroutine[Any, Any, None]],
        batch_size: int,
        max_batch_processor_instances: int,
    ) -> None:
        self.__batch_processor = batch_processor
        self.__disposition_strategy = disposition_strategy
        self.__batch_size = batch_size
        self.__max_batch_processor_instances = max_batch_processor_instances
        self.__event_loop = None

    def __get_event_loop(self):
        if self.__event_loop is None:
            self.__event_loop = asyncio.get_running_loop()
        return self.__event_loop

    async def __call__(self, items: List):
        batch_processor_instances: List[Task] = list()
        results = list()
        while items:
            items_list = list()
            for i in range(self.__batch_size):
                try:
                    item = items.pop(0)
                    items_list.append(item)
                except IndexError:
                    break

            while len(batch_processor_instances) > self.__max_batch_processor_instances:
                await self.__process_completed_batch_processor_instances(
                    batch_processor_instances, results
                )

            instance = self.__get_event_loop().create_task(self.__batch_processor(items_list))
            batch_processor_instances.append(instance)

        await asyncio.gather(*batch_processor_instances)

        while batch_processor_instances:
            await self.__process_completed_batch_processor_instances(
                batch_processor_instances, results
            )
        await self.__disposition_strategy(results)

    @staticmethod
    async def __process_completed_batch_processor_instances(
        batch_processor_instances: List[Task], results: List
    ):
        for i, batch_processor_instance in enumerate(batch_processor_instances):
            await asyncio.sleep(0)
            if batch_processor_instance.done():
                batch_results = batch_processor_instances.pop(i).result()
                if batch_results:
                    results.extend(batch_results)


class DirectDispositionStrategy:
    def __init__(self, *processors: Callable[[List], Coroutine[Any, Any, None]]) -> None:
        self.__processors = processors

    async def __call__(self, batch_results: List):
        for processor in self.__processors:
            await processor(batch_results)


class TypedDirectDispositionStrategy:
    def __init__(
        self, *type_processors: Tuple[Type, Callable[[List], Coroutine[Any, Any, None]]]
    ) -> None:
        self.__type_processors = type_processors[:]

    async def __call__(self, batch_results: List):
        for type_, processor in self.__type_processors:
            items = list()
            for batch_result in batch_results:
                if isinstance(batch_result, type_):
                    items.append(batch_result)
            await processor(items)


class MetadataDirectDispositionStrategy:
    def __init__(
        self,
        persistence_processor: Callable[[List], Coroutine[Any, Any, Any]],
        capture_processor: Callable[[List], Coroutine[Any, Any, Any]],
    ) -> None:
        self.__persistence_processor = persistence_processor
        self.__capture_processor = capture_processor

    async def __call__(self, batch_results: List[TokenTransportObject]):
        capture_items = list()
        persistence_items = list()
        for batch_result in batch_results:
            if (
                batch_result.token.metadata_uri is not None
                and len(batch_result.token.metadata_uri) > 0
            ):
                capture_items.append(batch_result)
            else:
                persistence_items.append(batch_result)

        if capture_items:
            await self.__capture_processor(capture_items)

        if persistence_items:
            await self.__persistence_processor(persistence_items)
