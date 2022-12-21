import unittest
from unittest.mock import MagicMock, AsyncMock

from blockrail.blockcrawler.core.entities import HexInt
from blockrail.blockcrawler.core.services import BlockTimeService
from blockrail.blockcrawler.evm.rpc import EvmRpcClient
from blockrail.blockcrawler.evm.types import EvmBlock


class TestBlockTimeService(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock(EvmRpcClient)
        self.__block_time_service = BlockTimeService(self.__rpc_client)

    async def test_get_timestamp_for_block_set_returns_set_value_and_deos_not_call_rpc(self):
        await self.__block_time_service.set_block_timestamp(HexInt(0x1), HexInt(0x11))
        actual = await self.__block_time_service.get_block_timestamp(HexInt(0x1))
        self.assertEqual([], self.__rpc_client.mock_calls, "Expected jo interaction with RPC")
        self.assertEqual(HexInt(0x11), actual)

    async def test_get_timestamp_gets_block_time_from_block_on_first_request_only(self):
        expected = HexInt(0x11)
        block = MagicMock(EvmBlock)
        block.timestamp = expected
        self.__rpc_client.get_block.return_value = block

        block_number = HexInt(0x1)
        first = await self.__block_time_service.get_block_timestamp(block_number)
        self.assertEqual(expected, first, "Incorrect response in first get!")
        second = await self.__block_time_service.get_block_timestamp(block_number)
        self.assertEqual(expected, second, "Incorrect response in second get!")
        self.__rpc_client.get_block.assert_awaited_once_with(HexInt(0x1))
