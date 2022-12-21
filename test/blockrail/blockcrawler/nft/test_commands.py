import unittest
from unittest.mock import patch, AsyncMock, MagicMock

from botocore.exceptions import ClientError

from blockrail.blockcrawler.core.entities import BlockChain
from blockrail.blockcrawler.core.stats import StatsService
from blockrail.blockcrawler.nft.commands import get_block, get_data_version
from blockrail.blockcrawler.evm.rpc import RpcClient
from blockrail.blockcrawler.evm.types import HexInt


class GetBlockCommandTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        patcher = patch("blockrail.blockcrawler.nft.commands.EvmRpcClient", spec=RpcClient)
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


class GetDataVersionTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__dynamodb = AsyncMock()

    async def test_not_increment_gets_data_from_correct_dynamodb_table(self):
        await get_data_version(self.__dynamodb, BlockChain.ETHEREUM_MAINNET, False, "prefix-")
        self.__dynamodb.Table.assert_awaited_once_with("prefix-crawler_config")

    async def test_not_increment_data_version_gets_current_data_version_from_db(self):
        await get_data_version(self.__dynamodb, BlockChain.ETHEREUM_MAINNET, False, table_prefix="")
        self.__dynamodb.Table.return_value.get_item.assert_awaited_once_with(
            Key={"blockchain": BlockChain.ETHEREUM_MAINNET.value},
        )

    async def test_not_increment_data_version_returns_current_data_version_from_db(self):
        expected = 1
        self.__dynamodb.Table.return_value.get_item.return_value = {
            "Item": {"data_version": expected}
        }
        actual = await get_data_version(
            self.__dynamodb, BlockChain.ETHEREUM_MAINNET, False, table_prefix=""
        )
        self.assertEqual(expected, actual)

    async def test_increment_gets_data_from_correct_dynamodb_table(self):
        await get_data_version(self.__dynamodb, BlockChain.ETHEREUM_MAINNET, True, table_prefix="")
        self.__dynamodb.Table.assert_awaited_once_with("crawler_config")

    async def test_increment_data_version_creates_incremented_data_version_in_db(self):
        await get_data_version(self.__dynamodb, BlockChain.ETHEREUM_MAINNET, True, table_prefix="")
        self.__dynamodb.Table.return_value.update_item.assert_awaited_once_with(
            Key={"blockchain": BlockChain.ETHEREUM_MAINNET.value},
            UpdateExpression="SET data_version = data_version + :inc",
            ExpressionAttributeValues={":inc": 1},
            ReturnValues="UPDATED_NEW",
        )

    async def test_increment_data_version_returns_incremented_version(self):
        expected = 1
        self.__dynamodb.Table.return_value.update_item.return_value = {
            "Attributes": {"data_version": expected}
        }
        actual = await get_data_version(
            self.__dynamodb, BlockChain.ETHEREUM_MAINNET, True, table_prefix=""
        )
        self.assertEqual(expected, actual)

    async def test_increment_data_version_updates_returns_version_one_when_no_version_exists(self):
        self.__dynamodb.Table.return_value.update_item.side_effect = [
            ClientError({"Error": {"Code": "ValidationException"}}, "Operation Name"),
            {"Attributes": {"data_version": 1}},
        ]
        actual = await get_data_version(
            self.__dynamodb, BlockChain.ETHEREUM_MAINNET, True, table_prefix=""
        )
        self.assertEqual(1, actual)

    async def test_increment_data_version_updates_to_version_one_when_no_version_exists(self):
        self.__dynamodb.Table.return_value.update_item.side_effect = [
            ClientError({"Error": {"Code": "ValidationException"}}, "Operation Name"),
            {"Attributes": {"data_version": 1}},
        ]
        await get_data_version(self.__dynamodb, BlockChain.ETHEREUM_MAINNET, True, table_prefix="")
        self.__dynamodb.Table.return_value.update_item.assert_awaited_with(
            Key={"blockchain": BlockChain.ETHEREUM_MAINNET.value},
            UpdateExpression="SET data_version = :version",
            ExpressionAttributeValues={":version": 1},
            ReturnValues="UPDATED_NEW",
        )


if __name__ == "__main__":
    unittest.main()
