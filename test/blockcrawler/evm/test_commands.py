import unittest
from unittest.mock import patch, AsyncMock, MagicMock

from blockcrawler.core.stats import StatsService
from blockcrawler.core.types import HexInt
from blockcrawler.evm.bin import get_block
from blockcrawler.evm.rpc import EvmRpcClient


class GetBlockCommandTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        patcher = patch("blockcrawler.evm.bin.EvmRpcClient", spec=EvmRpcClient)
        self.addCleanup(patcher.stop)
        self.__rpc_client = patcher.start()
        self.__rpc_client.return_value.get_block_number = AsyncMock()
        self.__stats_service = MagicMock(StatsService)

    async def test_provides_node_uri_to_rpc_client_init(self):
        expected = "URI"
        await get_block(expected[:], self.__stats_service)
        self.__rpc_client.assert_called_once_with(expected, self.__stats_service)

    async def test_calls_rpc_get_block_number(self):
        await get_block(None, self.__stats_service)
        self.__rpc_client.return_value.get_block_number.assert_called_once()

    async def test_returns_result_of_rpc_block_number(self):
        expected = 101
        self.__rpc_client.return_value.get_block_number.return_value = HexInt(hex(expected))
        actual = await get_block(None, self.__stats_service)
        self.assertEqual(expected, actual)
