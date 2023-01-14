import unittest
from unittest.mock import MagicMock, AsyncMock

from blockrail.blockcrawler.core.entities import HexInt
from blockrail.blockcrawler.core.services import (
    BlockTimeService,
    BlockTimeCache,
    MemoryBlockTimeCache,
)
from blockrail.blockcrawler.evm.rpc import EvmRpcClient
from blockrail.blockcrawler.evm.types import EvmBlock


class TestMemoryBlockTimeService(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__cache = MagicMock(BlockTimeCache)
        self.__cache.get.return_value = None
        self.__rpc_client = AsyncMock(EvmRpcClient)
        self.__block_time_service = BlockTimeService(self.__cache, self.__rpc_client)

    async def test_set_timestamp_for_block_sets_cache_value(self):
        await self.__block_time_service.set_block_timestamp(HexInt(0x1), HexInt(0x11))
        self.__cache.set.assert_awaited_once_with(0x1, 0x11)

    async def test_get_timestamp_for_block_returns_cache_value_when_not_none(self):
        self.__cache.get.return_value = 0x11
        actual = await self.__block_time_service.get_block_timestamp(HexInt(0x1))
        self.assertEqual([], self.__rpc_client.mock_calls, "Expected no interaction with RPC")
        self.assertEqual(HexInt(0x11), actual)

    async def test_get_timestamp_for_block_returns_rpc_value_when_none(self):
        expected = HexInt(0x11)
        block = MagicMock(EvmBlock)
        block.timestamp = expected
        self.__rpc_client.get_block.return_value = block
        actual = await self.__block_time_service.get_block_timestamp(HexInt(0x1))
        self.assertEqual(expected, actual)

    async def test_get_timestamp_for_block_returns_sets_rpc_value_in_cache(self):
        expected = HexInt(0x11)
        block = MagicMock(EvmBlock)
        block.timestamp = expected
        self.__rpc_client.get_block.return_value = block
        await self.__block_time_service.get_block_timestamp(HexInt(0x1))
        self.__cache.set.assert_awaited_once_with(0x1, 0x11)


class TestMemoryBlockTimeCache(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__cache = MemoryBlockTimeCache()

    async def test_block_time_not_set_returns_none(self):
        self.assertIsNone(await self.__cache.get(0x1))

    async def test_block_time_set_returns_set_value(self):
        await self.__cache.set(0x1, 0x11)
        self.assertEqual(0x11, await self.__cache.get(0x1))

    async def test_block_time_set_sync_returns_set_value(self):
        self.__cache.set_sync(0x1, 0x11)
        self.assertEqual(0x11, await self.__cache.get(0x1))
