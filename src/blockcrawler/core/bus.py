"""Classes and exceptions needed to operate a Data Bus"""

import abc
import asyncio
import logging
import signal
from abc import ABC
from asyncio import Task
from datetime import datetime
from logging import Logger
from types import TracebackType
from typing import List, Optional, Coroutine, Dict, Callable, Type, ContextManager, Union, Any

import blockcrawler


class ConsumerError(Exception):
    """The default base error class for a data bus."""

    pass


class DataPackage(ABC):
    """The base data package for a data bus to send and a consumer to receive."""

    pass


class Consumer(ABC):
    """
    Data bus consumer ABC
    """

    @abc.abstractmethod
    async def receive(self, data_package: DataPackage):
        raise NotImplementedError


class DataBus(ABC):
    """
    Data bus abstract base class
    """

    @abc.abstractmethod
    async def send(self, data_package: DataPackage):
        """
        Send data to the data bus. The data will be received from the data bus by
        consumers.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def register(self, consumer: Consumer):
        """
        Register a consumer with the data bus. The data bus will send data to the
        consumer that is sent to the data bus.

        """
        raise NotImplementedError

    @abc.abstractmethod
    async def __aenter__(self):
        """
        Asynchronous context manager entrypoint. This should be used to set up any
        asynchronous components in the data bus.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Asynchronous context manager exit point. This should be used to tear down any
        components in the data bus.
        """
        raise NotImplementedError


class Producer(ABC):
    """
    Data Producer abstract base class
    """

    @abc.abstractmethod
    async def __call__(self, data_bus: DataBus):
        """
        Implementations should place Data Packages on the Data Bus
        """
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

        :meta public:
        """
        return self.__data_bus


class ParallelDataBus(DataBus):
    """
    Data Bus implementation which will send the data packages to consumers in parallel.
    """

    def __init__(self, logger: Logger) -> None:
        self.__tasks: List[Task] = []
        self.__logger: Logger = logger
        self.__consumers: List[Consumer] = []
        self.__task_watcher_task: Optional[Task] = None

    async def __aenter__(self):
        self.__logger.debug("STARTING TASK WATCHER")
        self.__task_watcher_task = asyncio.get_running_loop().create_task(self.__task_watcher())
        return self

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
        self.__logger.debug("RECEIVED DATA PACKAGE", extra={"data_package": data_package})
        for consumer in self.__consumers:
            try:
                self.__logger.debug(
                    "SENDING DATA PACKAGE TO CONSUMER",
                    extra={"consumer": consumer, "data_package": data_package},
                )
                await self.__add_task(consumer.receive(data_package))
                await asyncio.sleep(0)  # Allow task to start before continuing
            except Exception as e:
                self.__logger.exception("Error sending data package to consumer", exc_info=e)

    async def register(self, consumer: Consumer):
        self.__consumers.append(consumer)
        self.__logger.debug("REGISTERED CONSUMER", extra={"consumer": consumer})

    async def __add_task(self, coroutine: Coroutine):
        self.__tasks.append(asyncio.get_running_loop().create_task(coroutine))

    async def __task_watcher(self):
        while True:
            for index, task in enumerate(self.__tasks):
                if task.done():
                    if exc_info := task.exception():
                        if isinstance(exc_info, ConsumerError):
                            self.__logger.error(exc_info)
                        else:
                            self.__logger.exception(
                                "Error sending data package to consumer", exc_info=exc_info
                            )
                    self.__tasks.pop(index)
            await asyncio.sleep(0)


class SignalManager(ContextManager):
    """
    Context manager to manage signals and allow for graceful shutdown. Instantiating the
    SignalManager will register its own internal signal handler for all signals in
    the SIG_NAMES attribute tht are supported by the operating system. Entering the
    context will register the internal signal handler, exiting the manager will restore
    the original handler if the internal handler is still registered. The internal
    handler will alter the state of the manager such that `interrupted` will be True
    and `interrupting_signal` will return the name of hte interrupting signal.

    Usage::

        with SignalManager() as signal_manager:
            i: int = 0
            while not signal_manager.interrupted:
                i += 1
                print(i)
                sleep(1.0)
            print(signal_manager.interrupting_signal)

    The above code will print an incrementing value every second until a signal occurs.
    Once the signal occurs, it will exit the while loop and print which signal it was.
    """

    SIG_NAMES = [
        "SIGABRT",  # Linux abort signal
        "SIGBREAK",  # Windows keyboard interrupt (CTRL + BREAK)
        "SIGHUP",  # Linux terminal hangup
        "SIGINT",  # *nix keyboard interrupt (CTRL + C)
        "SIGQUIT",  # Quit
        "SIGTERM",  # Termination signal
    ]

    def __init__(self) -> None:
        self.__logger = logging.getLogger(blockcrawler.LOGGER_NAME)
        self.__interrupting_signal = None
        self.__original_handlers: Dict[str, Union[Callable, int, Any]] = dict()

    def __enter__(self) -> "SignalManager":
        for sig_name in self.SIG_NAMES:
            if hasattr(signal, sig_name):
                self.__original_handlers[sig_name] = signal.signal(
                    getattr(signal, sig_name), self.signal_handler
                )
        return self

    def __exit__(
        self,
        __exc_type: Optional[Type[BaseException]],
        __exc_value: Optional[BaseException],
        __traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        for sig_name, original_handler in self.__original_handlers.items():
            sig = getattr(signal, sig_name)
            if signal.getsignal(sig) == self.signal_handler:
                signal.signal(sig, original_handler)
        return None

    def signal_handler(self, signum, _frame):
        self.__interrupting_signal = signum
        self.__logger.info(f"{self.interrupting_signal} signal received.")

    @property
    def interrupted(self):
        return self.__interrupting_signal is not None

    @property
    def interrupting_signal(self):
        return (
            None
            if self.__interrupting_signal is None
            else signal.Signals(self.__interrupting_signal).name
        )


class DebugConsumer(Consumer):
    """
    Consumer to use for testing your composed crawler. It will print all data received
    for with the filter function returns True.

    This consumer can help understand which data is being passed around asynchronously
    on the data bus when standard debugging becomes too cumbersome.
    """

    def __init__(self, filter_function: Callable[[DataPackage], bool] = lambda _: True) -> None:
        """
        :filter_function: Callable to filter which DataPackage items to print. If the result
            of the filter is not truthy, it will not be printed. The default filter will
            print all DataPackage items received
        """
        self.__filter_function = filter_function

    async def receive(self, data_package: DataPackage):
        if self.__filter_function(data_package):
            print(f"{datetime.now():%Y-%m-%dT%H:%M:%S%z} - {data_package}")
