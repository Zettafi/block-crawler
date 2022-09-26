import asyncio
from abc import ABC
from asyncio import Task
from datetime import datetime
from logging import Logger
from typing import List, Optional, Coroutine


class ConsumerError(Exception):
    pass


class DataPackage:
    pass


class Consumer:
    """
    Data bus consumer interface
    """

    async def receive(self, data_package: DataPackage):
        raise NotImplementedError


class DataBus:
    """
    Data bus interface
    """

    async def send(self, data_package: DataPackage):
        """
        Send data to the data bus. The data will be received from the data bus by all of
        the consumers.
        """
        raise NotImplementedError

    async def register(self, consumer: Consumer):
        """
        Register a consumer with the data bus. The data bus will send data to the
        consumer that is send to the data bus.

        """
        raise NotImplementedError

    async def __aenter__(self):
        """
        Asynchronous context manager entrypoint. This should be used to set up any
        asynchronous components in the data bus.
        """
        raise NotImplementedError

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Asynchronous context manager exit point. This should be used to tear down any
        components in the data bus.
        """
        raise NotImplementedError


class Producer:
    """
    Data bus producer interface
    """

    async def __call__(self, data_bus: DataBus):
        raise NotImplementedError


class Transformer(Consumer, ABC):
    """
    A data bus consumer that places data on the data bus based on the data
    transformations it makes.
    """

    def __init__(self, data_bus: DataBus) -> None:
        self.__data_bus = data_bus

    def _get_data_bus(self) -> DataBus:
        """
        Get the data bus instance which will receive transformed data
        """
        return self.__data_bus


class ParallelDataBus(DataBus):
    """
    Data bus which will send the data packages to consumers in parallel.
    """

    def __init__(self, logger: Logger) -> None:
        self.__tasks: List[Task] = list()
        self.__logger: Logger = logger
        self.__consumers: List[Consumer] = list()
        self.__task_watcher_task: Optional[Task] = None

    async def __aenter__(self):
        self.__logger.debug("STARTING TASK WATCHER")
        self.__task_watcher_task = asyncio.get_running_loop().create_task(self.__task_watcher())

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.__logger.debug("WAITING FOR CONSUMERS TO COMPLETE")

        while True:
            if self.__tasks:
                # Let tasks process
                await asyncio.sleep(0)
            else:
                # All tasks have completed. Stop waiting.
                break

        self.__logger.debug("CONSUMERS COMPLETE")

        self.__logger.debug("STOPPING TASK WATCHER")
        if self.__task_watcher_task is not None:
            self.__task_watcher_task.cancel()

    async def send(self, data_package: DataPackage):
        self.__logger.debug("RECEIVED DATA PACKAGE", extra=dict(data_package=data_package))
        for consumer in self.__consumers:
            try:
                self.__logger.debug(
                    "SENDING DATA PACKAGE TO CONSUMER",
                    extra=dict(consumer=consumer, data_package=data_package),
                )
                await self.__add_task(consumer.receive(data_package))
                await asyncio.sleep(0)  # Allow task to start before continuing
            except Exception as e:
                if isinstance(e, ConsumerError):
                    self.__logger.error(e)
                else:
                    self.__logger.exception(
                        "Error sending data package to consumer", exc_info=False
                    )

    async def register(self, consumer: Consumer):
        self.__consumers.append(consumer)
        self.__logger.debug("REGISTERED CONSUMER", extra=dict(consumer=consumer))

    async def __add_task(self, coroutine: Coroutine):
        self.__tasks.append(asyncio.get_running_loop().create_task(coroutine))

    async def __task_watcher(self):
        while True:
            for index, task in enumerate(self.__tasks):
                if task.done():
                    if exc_info := task.exception():
                        self.__logger.exception(
                            "Error sending data package to consumer", exc_info=exc_info
                        )
                    self.__tasks.pop(index)
            await asyncio.sleep(0)


class DebugConsumer(Consumer):
    def __init__(self, filter_function=lambda _: True) -> None:
        self.__filter_function = filter_function

    async def receive(self, data_package: DataPackage):
        if self.__filter_function(data_package):
            print(f"{datetime.now():%Y-%m-%dT%H:%M:%S%z} - {data_package}")
