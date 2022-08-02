from typing import Coroutine
from typing import Callable, Dict, List


class EventBus:
    def __init__(self) -> None:
        self.__registrations: Dict[str, List[Callable]] = dict()

    async def register(self, event: str, callback: Callable):
        if event not in self.__registrations:
            self.__registrations[event] = list()
        self.__registrations[event].append(callback)

    async def trigger(self, event: str, args=tuple()):
        if event in self.__registrations:
            for callee in self.__registrations[event]:
                result = callee(*args)
                if isinstance(result, Coroutine):
                    await result
