import unittest
from unittest.mock import AsyncMock

from botocore.exceptions import ClientError

from blockcrawler.core.entities import BlockChain
from blockcrawler.nft.bin.shared import _get_data_version


class GetDataVersionTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__dynamodb = AsyncMock()

    async def test_not_increment_gets_data_from_correct_dynamodb_table(self):
        await _get_data_version(self.__dynamodb, BlockChain.ETHEREUM_MAINNET, False, "prefix-")
        self.__dynamodb.Table.assert_awaited_once_with("prefix-crawler_config")

    async def test_not_increment_data_version_gets_current_data_version_from_db(self):
        await _get_data_version(
            self.__dynamodb, BlockChain.ETHEREUM_MAINNET, False, table_prefix=""
        )
        self.__dynamodb.Table.return_value.get_item.assert_awaited_once_with(
            Key={"blockchain": BlockChain.ETHEREUM_MAINNET.value},
        )

    async def test_not_increment_data_version_returns_current_data_version_from_db(self):
        expected = 1
        self.__dynamodb.Table.return_value.get_item.return_value = {
            "Item": {"data_version": expected}
        }
        actual = await _get_data_version(
            self.__dynamodb, BlockChain.ETHEREUM_MAINNET, False, table_prefix=""
        )
        self.assertEqual(expected, actual)

    async def test_increment_gets_data_from_correct_dynamodb_table(self):
        await _get_data_version(self.__dynamodb, BlockChain.ETHEREUM_MAINNET, True, table_prefix="")
        self.__dynamodb.Table.assert_awaited_once_with("crawler_config")

    async def test_increment_data_version_creates_incremented_data_version_in_db(self):
        await _get_data_version(self.__dynamodb, BlockChain.ETHEREUM_MAINNET, True, table_prefix="")
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
        actual = await _get_data_version(
            self.__dynamodb, BlockChain.ETHEREUM_MAINNET, True, table_prefix=""
        )
        self.assertEqual(expected, actual)

    async def test_increment_data_version_updates_returns_version_one_when_no_version_exists(self):
        self.__dynamodb.Table.return_value.update_item.side_effect = [
            ClientError({"Error": {"Code": "ValidationException"}}, "Operation Name"),
            {"Attributes": {"data_version": 1}},
        ]
        actual = await _get_data_version(
            self.__dynamodb, BlockChain.ETHEREUM_MAINNET, True, table_prefix=""
        )
        self.assertEqual(1, actual)

    async def test_increment_data_version_updates_to_version_one_when_no_version_exists(self):
        self.__dynamodb.Table.return_value.update_item.side_effect = [
            ClientError({"Error": {"Code": "ValidationException"}}, "Operation Name"),
            {"Attributes": {"data_version": 1}},
        ]
        await _get_data_version(self.__dynamodb, BlockChain.ETHEREUM_MAINNET, True, table_prefix="")
        self.__dynamodb.Table.return_value.update_item.assert_awaited_with(
            Key={"blockchain": BlockChain.ETHEREUM_MAINNET.value},
            UpdateExpression="SET data_version = :version",
            ExpressionAttributeValues={":version": 1},
            ReturnValues="UPDATED_NEW",
        )


if __name__ == "__main__":
    unittest.main()
