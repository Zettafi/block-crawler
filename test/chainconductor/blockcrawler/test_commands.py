import unittest
from unittest.mock import patch, AsyncMock

from chainconductor.blockcrawler.commands import get_block
from chainconductor.web3.rpc import RPCClient
from chainconductor.web3.types import HexInt


class GetBlockCommandTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        patcher = patch("chainconductor.blockcrawler.commands.RPCClient", spec=RPCClient)
        self.addCleanup(patcher.stop)
        self.__rpc_client = patcher.start()
        self.__rpc_client.return_value.get_block_number = AsyncMock()

    async def test_provides_node_uri_to_rpc_client_init(self):
        expected = "URI"
        await get_block(expected[:])
        self.__rpc_client.assert_called_once_with(expected)

    async def test_calls_rpc_get_block_number(self):
        await get_block(None)
        self.__rpc_client.return_value.get_block_number.assert_called_once()

    async def test_returns_result_of_rpc_block_number(self):
        expected = 101
        self.__rpc_client.return_value.get_block_number.return_value = HexInt(hex(expected))
        actual = await get_block(None)
        self.assertEqual(expected, actual)


if __name__ == "__main__":
    unittest.main()
