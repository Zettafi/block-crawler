import unittest
from unittest.mock import MagicMock, AsyncMock

from chainconductor.blockcrawler.events import EventBus


class EventsTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__event_bus = EventBus()

    async def test_trigger_calls_registered_events_with_async_callbacks(self):
        callback = AsyncMock()
        await self.__event_bus.register("test", callback)
        await self.__event_bus.trigger("test", ("arg1", "arg2"))
        callback.assert_called_once_with("arg1", "arg2")

    async def test_trigger_calls_registered_events_with_sync_callbacks(self):
        callback = MagicMock()
        await self.__event_bus.register("test", callback)
        await self.__event_bus.trigger("test", ("arg1", "arg2"))
        callback.assert_called_once_with("arg1", "arg2")

    async def test_trigger_calls_nothing_for_unregistered_events(self):
        callback = MagicMock()
        await self.__event_bus.register("test", callback)
        await self.__event_bus.trigger("other", ("arg1", "arg2"))
        callback.assert_not_called()
