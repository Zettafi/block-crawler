import logging
import unittest
from unittest.mock import Mock, AsyncMock, ANY, MagicMock, call

import ddt
from boto3.dynamodb.conditions import Attr
from hexbytes import HexBytes

import blockcrawler
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.stats import StatsService
from blockcrawler.core.types import Address, HexInt
from blockcrawler.nft import data_services
from blockcrawler.nft.data_services import (
    DataVersionTooOldException,
)
from blockcrawler.nft.data_services.dynamodb import DynamoDbDataService
from blockcrawler.nft.entities import (
    Collection,
    CollectionType,
    EthereumCollectionType,
    Token,
    TokenTransfer,
    TokenTransactionType,
    TokenOwner,
)


@ddt.ddt
class DynamoDbDataServiceTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__dynamodb = AsyncMock()
        self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException = Exception
        self.__table_resource = self.__dynamodb.Table.return_value
        self.__table_resource.batch_writer = Mock(return_value=AsyncMock())
        self.__batch_writer = (
            self.__table_resource.batch_writer.return_value.__aenter__.return_value
        )  # noqa: E501
        self.__table_prefix = "pre"
        self.__stats_service = MagicMock(StatsService)

        self.__data_service = DynamoDbDataService(
            self.__dynamodb, self.__stats_service, self.__table_prefix
        )

    async def test_write_collection_uses_collection_table_with_prefix_prepended(self):
        collection = Collection(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection Address"),
            block_created=HexInt("0x1"),
            date_created=HexInt("0x1234"),
            creator=Address("Creator"),
            owner=Address("Owner"),
            name="Name",
            symbol="Symbol",
            total_supply=HexInt("0x100"),
            specification=EthereumCollectionType.ERC721,
            data_version=999,
        )
        await self.__data_service.write_collection(collection)
        self.__dynamodb.Table.assert_awaited_once_with("precollection")

    async def test_write_collection_writes_expected_data(self):
        blockchain = Mock(BlockChain)
        blockchain.value = "Expected Blockchain"
        collection_type = "Expected Collection Type"
        collection = Collection(
            blockchain=blockchain,
            collection_id=Address("Collection Address"),
            block_created=HexInt("0x1"),
            date_created=HexInt("0x1234"),
            creator=Address("Creator"),
            owner=Address("Owner"),
            name="Name",
            symbol="Symbol",
            total_supply=HexInt("0x100"),
            specification=CollectionType(collection_type),
            data_version=999,
        )
        await self.__data_service.write_collection(collection)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item={
                "blockchain": "Expected Blockchain",
                "collection_id": "Collection Address",
                "block_created": "0x1",
                "creator_account": "Creator",
                "owner_account": "Owner",
                "date_created": 0x1234,
                "collection_name": "Name",
                "name_lower": "name",
                "symbol": "Symbol",
                "total_supply": "0x100",
                "specification": "Expected Collection Type",
                "data_version": 999,
            },
            ConditionExpression=ANY,
        )

    async def test_write_collection_truncates_name_short_to_1024_chars(self):
        blockchain = Mock(BlockChain)
        blockchain.value = "Expected Blockchain"
        collection_type = "Expected Collection Type"
        collection = Collection(
            blockchain=blockchain,
            collection_id=Address("Collection Address"),
            block_created=HexInt("0x1"),
            date_created=HexInt("0x1234"),
            creator=Address("Creator"),
            owner=Address("Owner"),
            name="abcdefgh" * 200,
            symbol="Symbol",
            total_supply=HexInt("0x100"),
            specification=CollectionType(collection_type),
            data_version=999,
        )
        await self.__data_service.write_collection(collection)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item={
                "blockchain": ANY,
                "collection_id": ANY,
                "block_created": ANY,
                "creator_account": ANY,
                "owner_account": ANY,
                "date_created": ANY,
                "collection_name": ANY,
                "name_lower": "abcdefgh" * 128,
                "symbol": ANY,
                "total_supply": ANY,
                "specification": ANY,
                "data_version": ANY,
            },
            ConditionExpression=ANY,
        )

    async def test_write_collection_uses_the_correct_conditional_expression_and_attrs(self):
        collection = Collection(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection Address"),
            block_created=HexInt("0x1"),
            date_created=HexInt("0x1234"),
            creator=Address("Creator"),
            owner=Address("Owner"),
            name="Name",
            symbol="Symbol",
            total_supply=HexInt("0x100"),
            specification=EthereumCollectionType.ERC721,
            data_version=999,
        )

        await self.__data_service.write_collection(collection)
        self.__dynamodb.Table.return_value.put_item.assert_awaited_once_with(
            Item=ANY,
            ConditionExpression=Attr("data_version").not_exists() | Attr("data_version").lte(999),
        )

    async def test_write_collection_raises_exception_for_condition_check_failure_when_saving(
        # noqa: E501
        self,
    ):
        self.__table_resource.put_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        collection = Collection(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection Address"),
            block_created=HexInt("0x1"),
            date_created=HexInt("0x1234"),
            creator=Address("Creator"),
            owner=Address("Owner"),
            name="Name",
            symbol="Symbol",
            total_supply=HexInt("0x100"),
            specification=EthereumCollectionType.ERC721,
            data_version=999,
        )
        with self.assertRaises(DataVersionTooOldException):
            await self.__data_service.write_collection(collection)
            self.__table_resource.put_item.assert_awaited_once()

    @ddt.data("", None)
    async def test_write_collection_does_send_name_or_name_lower_when_name_is_empty(self, name):
        self.__table_resource.put_item.side_effect = (
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException
        )
        collection = Collection(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection Address"),
            block_created=HexInt("0x1"),
            date_created=HexInt("0x1234"),
            creator=Address("Creator"),
            owner=Address("Owner"),
            name=name,
            symbol="Symbol",
            total_supply=HexInt("0x100"),
            specification=EthereumCollectionType.ERC721,
            data_version=999,
        )
        await self.__data_service.write_collection(collection)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item={
                "blockchain": ANY,
                "collection_id": ANY,
                "block_created": ANY,
                "creator_account": ANY,
                "owner_account": ANY,
                "date_created": ANY,
                "symbol": ANY,
                "total_supply": ANY,
                "specification": ANY,
                "data_version": ANY,
            },
            ConditionExpression=ANY,
        )

    async def test_write_token_uses_token_table_with_prefix_prepended(self):
        token = Token(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            data_version=999,
            collection_id=Address("Collection Address"),
            token_id=HexInt(1),
            mint_date=HexInt(2),
            mint_block=HexInt(4),
            current_owner=Address("Current Owner"),
            original_owner=Address("Original Owner"),
            quantity=HexInt(3),
            metadata_url="Metadata URL",
            attribute_version=HexInt(7),
        )

        await self.__data_service.write_token(token)
        self.__dynamodb.Table.assert_awaited_once_with("pretoken")

    async def test_write_token_stores_correct_data(self):
        blockchain = Mock(BlockChain)
        blockchain.value = "Expected Blockchain"
        token = Token(
            blockchain=blockchain,
            data_version=999,
            collection_id=Address("Collection Address"),
            token_id=HexInt(1),
            mint_date=HexInt(2),
            mint_block=HexInt(4),
            current_owner=Address("Current Owner"),
            original_owner=Address("Original Owner"),
            quantity=HexInt(3),
            metadata_url="Metadata URL",
            attribute_version=HexInt("0x00000000000000000007"),
        )

        await self.__data_service.write_token(token)
        self.__table_resource.update_item.assert_awaited_once_with(
            Key={
                "blockchain_collection_id": "Expected Blockchain::Collection Address",
                "token_id": "0x1",
            },
            UpdateExpression=(
                "SET mint_timestamp = :mint_timestamp, mint_block_id = :mint_block_id, "
                "original_owner_account = :original_owner_account"
            ),
            ConditionExpression=(
                "attribute_not_exists(data_version) OR data_version <= :data_version"
            ),
            ExpressionAttributeValues={
                ":mint_timestamp": 2,
                ":mint_block_id": "0x4",
                ":original_owner_account": "Original Owner",
                ":data_version": 999,
            },
        )

    async def test_write_token_logs_condition_check_fails(self):
        self.__table_resource.update_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        with self.assertLogs(blockcrawler.LOGGER_NAME, logging.DEBUG) as cm:
            token = Token(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                data_version=999,
                collection_id=Address("Collection Address"),
                token_id=HexInt(1),
                mint_date=HexInt(2),
                mint_block=HexInt(4),
                current_owner=Address("Current Owner"),
                original_owner=Address("Original Owner"),
                quantity=HexInt(3),
                metadata_url="Metadata URL",
                attribute_version=HexInt("0x00000000000000000007"),
            )
            await self.__data_service.write_token(token)
            self.assertIn(
                f"DEBUG:{blockcrawler.LOGGER_NAME}:Token for "
                f"ethereum-mainnet:Collection Address:1 not written -- version too old -- "
                f"999 -- {token}",
                cm.output,
            )

    async def test_update_token_url_uses_token_table_with_prefix_prepended(self):
        await self.__data_service.update_token_metadata_url(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            metadata_url="metadata-url",
            metadata_url_version=HexInt(0x100),
            data_version=999,
        )
        self.__dynamodb.Table.assert_awaited_once_with("pretoken")

    async def test_update_token_url_calls_update_item_with_the_correct_values(self):
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        await self.__data_service.update_token_metadata_url(
            blockchain=blockchain,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            metadata_url="metadata-url",
            metadata_url_version=HexInt(0x100),
            data_version=999,
        )
        self.__table_resource.update_item.assert_awaited_once_with(
            Key={
                "blockchain_collection_id": "blockchain::collection-id",
                "token_id": "0x1",
            },
            UpdateExpression=(
                "SET metadata_url = :metadata_url, metadata_url_version = :metadata_url_version"
            ),
            ExpressionAttributeValues={
                ":metadata_url": "metadata-url",
                ":metadata_url_version": "0x100",
                ":data_version": 999,
            },
            ConditionExpression=(
                "attribute_not_exists(data_version)"  # New token
                " OR data_version < :data_version"  # New load for existing token
                " OR (data_version = :data_version"  # Same load for existing item
                " AND (attribute_not_exists(metadata_url_version)"  # No URI
                " OR metadata_url_version < :metadata_url_version)"  # Newer URI"
                ")"
            ),
        )

    async def test_update_token_url_debug_logs_condition_check_fails(self):
        self.__table_resource.update_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        with self.assertLogs(blockcrawler.LOGGER_NAME, logging.DEBUG) as cm:
            await self.__data_service.update_token_metadata_url(
                blockchain=blockchain,
                collection_id=Address("collection-id"),
                token_id=HexInt(0x1),
                metadata_url="metadata-url",
                metadata_url_version=HexInt(0x12345),
                data_version=999,
            )
            self.assertIn(
                f"DEBUG:{blockcrawler.LOGGER_NAME}:Metadata URI for "
                f"blockchain:collection-id:1 not updated -- version too old -- "
                f"data: 999 - URI: 0x12345 -- metadata-url",
                cm.output,
            )

    async def test_update_token_url_hits_stat_for_condition_check_fails(self):
        self.__table_resource.update_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        await self.__data_service.update_token_metadata_url(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            metadata_url="metadata-url",
            metadata_url_version=HexInt(0),
            data_version=999,
        )
        self.__stats_service.increment.assert_has_calls(
            [call(data_services.STAT_TOKEN_URI_UPDATE_DATA_TOO_OLD)]
        )

    async def test_update_token_url_with_2048_char_url_stores_url(self):
        metadata_url = "X" * 2048
        await self.__data_service.update_token_metadata_url(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            metadata_url=metadata_url,
            metadata_url_version=HexInt(0),
            data_version=999,
        )
        self.__table_resource.update_item.assert_awaited_once()

    async def test_update_token_url_with_none_stores_url(self):
        await self.__data_service.update_token_metadata_url(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            metadata_url=None,
            metadata_url_version=HexInt(0),
            data_version=999,
        )
        self.__table_resource.update_item.assert_awaited_once()

    async def test_update_token_url_with_2049_char_url_does_not_store_url_data_and_debug_logs(self):
        metadata_url = "X" * 2049
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        with self.assertLogs(blockcrawler.LOGGER_NAME, logging.DEBUG) as cm:
            await self.__data_service.update_token_metadata_url(
                blockchain=blockchain,
                collection_id=Address("collection-id"),
                token_id=HexInt(0x1),
                metadata_url=metadata_url,
                metadata_url_version=HexInt(0),
                data_version=999,
            )
            self.__table_resource.update_item.assert_not_awaited()
            self.assertIn(
                f"DEBUG:{blockcrawler.LOGGER_NAME}:Metadata URI for "
                f"blockchain:collection-id:1 not updated -- "
                f"too long to store -- {metadata_url}",
                cm.output,
            )

    async def test_update_token_url_increments_count_stat(self):
        await self.__data_service.update_token_metadata_url(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            metadata_url="metadata-url",
            metadata_url_version=HexInt(0),
            data_version=999,
        )
        self.__stats_service.increment.assert_has_calls([call(data_services.STAT_TOKEN_URI_UPDATE)])

    async def test_update_token_url_hits_timer_stat(self):
        await self.__data_service.update_token_metadata_url(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            metadata_url="metadata-url",
            metadata_url_version=HexInt(0),
            data_version=999,
        )
        self.__stats_service.ms_counter.assert_called_once_with(
            data_services.STAT_TOKEN_URI_UPDATE_MS
        )

    async def test_update_token_quantity_uses_token_table_with_prefix_prepended(self):
        await self.__data_service.update_token_quantity(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            quantity=21,
            data_version=999,
        )
        self.__dynamodb.Table.assert_awaited_once_with("pretoken")

    async def test_update_token_quantity_calls_update_item_with_the_correct_values(self):
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        await self.__data_service.update_token_quantity(
            blockchain=blockchain,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            quantity=21,
            data_version=999,
        )
        self.__table_resource.update_item.assert_called_once_with(
            Key={
                "blockchain_collection_id": "blockchain::collection-id",
                "token_id": "0x1",
            },
            UpdateExpression="ADD quantity :q SET data_version = :data_version",
            ExpressionAttributeValues={":q": 21, ":data_version": 999},
            ConditionExpression="attribute_not_exists(data_version) "
            "OR data_version = :data_version",
        )

    async def test_update_token_quantity_updates_item_again_to_increment_when_check_fails(
        self,
    ):
        self.__table_resource.update_item.side_effect = [
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException,
            None,
        ]
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        await self.__data_service.update_token_quantity(
            blockchain=blockchain,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            quantity=21,
            data_version=999,
        )
        self.__table_resource.update_item.assert_has_awaits(
            [
                call(
                    Key={
                        "blockchain_collection_id": "blockchain::collection-id",
                        "token_id": "0x1",
                    },
                    UpdateExpression="SET quantity :q, data_version = :data_version",
                    ExpressionAttributeValues={":q": 21, ":data_version": 999},
                    ConditionExpression="data_version < :data_version",
                )
            ]
        )

    async def test_update_token_quantity_debug_logs_seconds_condition_check_fails(self):
        self.__table_resource.update_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        with self.assertLogs(blockcrawler.LOGGER_NAME, logging.DEBUG) as cm:
            await self.__data_service.update_token_quantity(
                blockchain=blockchain,
                collection_id=Address("collection-id"),
                token_id=HexInt(0x1),
                quantity=21,
                data_version=999,
            )
            self.assertIn(
                f"DEBUG:{blockcrawler.LOGGER_NAME}:Quantity for "
                f"blockchain:collection-id:1 not updated -- version too old -- "
                f"data: 999 -- 21",
                cm.output,
            )

    async def test_update_token_quantity_hits_stat_for_condition_check_fails(self):
        self.__table_resource.update_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        await self.__data_service.update_token_quantity(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            quantity=21,
            data_version=999,
        )
        self.__stats_service.increment.assert_has_calls(
            [call(data_services.STAT_TOKEN_QUANTITY_UPDATE_DATA_TOO_OLD)]
        )

    async def test_update_token_quantity_increments_count_stat(self):
        await self.__data_service.update_token_quantity(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            quantity=21,
            data_version=999,
        )
        self.__stats_service.increment.assert_has_calls(
            [call(data_services.STAT_TOKEN_QUANTITY_UPDATE)]
        )

    async def test_update_token_quantity_hits_timer_stat(self):
        await self.__data_service.update_token_quantity(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            quantity=21,
            data_version=999,
        )
        self.__stats_service.ms_counter.assert_called_once_with(
            data_services.STAT_TOKEN_QUANTITY_UPDATE_MS
        )

    async def test_update_token_current_owner_uses_token_table_with_prefix_prepended(self):
        await self.__data_service.update_token_current_owner(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            owner=Address("Owner"),
            owner_version=HexInt(0x1234),
            data_version=999,
        )
        self.__dynamodb.Table.assert_awaited_once_with("pretoken")

    async def test_update_token_current_owner_calls_update_item_with_the_correct_values(self):
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        await self.__data_service.update_token_current_owner(
            blockchain=blockchain,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            owner=Address("Owner"),
            owner_version=HexInt(0x1234),
            data_version=999,
        )
        self.__table_resource.update_item.assert_awaited_once_with(
            Key={
                "blockchain_collection_id": "blockchain::collection-id",
                "token_id": "0x1",
            },
            UpdateExpression="SET current_owner_account = :current_owner_account, "
            "current_owner_version = :current_owner_version",
            ExpressionAttributeValues={
                ":current_owner_account": "Owner",
                ":current_owner_version": "0x1234",
                ":data_version": 999,
            },
            ConditionExpression=(
                "attribute_not_exists(data_version)"  # New token
                " OR data_version < :data_version"  # New load for existing token
                " OR (data_version = :data_version"  # Same load for existing item
                " AND (attribute_not_exists(current_owner_version)"  # No owner
                " OR current_owner_version < :current_owner_version)"  # Newer owner
                ")"
            ),
        )

    async def test_update_token_current_owner_debug_logs_condition_check_fails(self):
        self.__table_resource.update_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        with self.assertLogs(blockcrawler.LOGGER_NAME, logging.DEBUG) as cm:
            await self.__data_service.update_token_current_owner(
                blockchain=blockchain,
                collection_id=Address("collection-id"),
                token_id=HexInt(0x1),
                owner=Address("Owner"),
                owner_version=HexInt(0x1234),
                data_version=999,
            )
            self.assertIn(
                f"DEBUG:{blockcrawler.LOGGER_NAME}:Current owner for "
                f"blockchain:collection-id:1 not updated -- version too old -- "
                f"data: 999 - owner: 0x1234 -- Owner",
                cm.output,
            )

    async def test_update_token_current_owner_hits_stat_for_condition_check_fails(self):
        self.__table_resource.update_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        await self.__data_service.update_token_current_owner(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            owner=Address("Owner"),
            owner_version=HexInt(0x1234),
            data_version=999,
        )
        self.__stats_service.increment.assert_has_calls(
            [call(data_services.STAT_TOKEN_CURRENT_OWNER_UPDATE_DATA_TOO_OLD)]
        )

    async def test_update_token_current_owner_increments_count_stat(self):
        await self.__data_service.update_token_current_owner(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            owner=Address("Owner"),
            owner_version=HexInt(0x1234),
            data_version=999,
        )
        self.__stats_service.increment.assert_has_calls(
            [call(data_services.STAT_TOKEN_CURRENT_OWNER_UPDATE)]
        )

    async def test_update_token_current_owner_hits_timer_stat(self):
        await self.__data_service.update_token_current_owner(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("collection-id"),
            token_id=HexInt(0x1),
            owner=Address("Owner"),
            owner_version=HexInt(0x1234),
            data_version=999,
        )
        self.__stats_service.ms_counter.assert_called_once_with(
            data_services.STAT_TOKEN_CURRENT_OWNER_UPDATE_MS
        )

    async def test_write_token_batch_uses_token_table_with_prefix_prepended(self):
        token = Token(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            data_version=999,
            collection_id=Address("Collection Address"),
            token_id=HexInt(1),
            mint_date=HexInt(2),
            mint_block=HexInt(4),
            current_owner=Address("Current Owner"),
            original_owner=Address("Original Owner"),
            quantity=HexInt(3),
            metadata_url="Metadata URL",
            attribute_version=HexInt(7),
        )

        await self.__data_service.write_token_batch([token])
        self.__dynamodb.Table.assert_awaited_once_with("pretoken")

    async def test_write_token_batch_stores_correct_data(self):
        blockchain = Mock(BlockChain)
        blockchain.value = "Expected Blockchain"
        token = Token(
            blockchain=blockchain,
            data_version=999,
            collection_id=Address("Collection Address"),
            token_id=HexInt(1),
            mint_date=HexInt(2),
            mint_block=HexInt(4),
            current_owner=Address("Current Owner"),
            original_owner=Address("Original Owner"),
            quantity=HexInt(3),
            metadata_url="Metadata URL",
            attribute_version=HexInt("0x00000000000000000007"),
        )

        await self.__data_service.write_token_batch([token])
        self.__batch_writer.put_item.assert_awaited_once_with(
            Item={
                "blockchain_collection_id": "Expected Blockchain::Collection Address",
                "token_id": "0x1",
                "mint_timestamp": 2,
                "mint_block_id": "0x4",
                "original_owner_account": "Original Owner",
                "current_owner_account": "Current Owner",
                "current_owner_version": "0x00000000000000000007",
                "quantity": 3,
                "metadata_url": "Metadata URL",
                "metadata_url_version": "0x00000000000000000007",
                "data_version": 999,
            },
        )

    async def test_write_token_batch_with_2048_char_url_stores_url(self):
        blockchain = Mock(BlockChain)
        blockchain.value = "Expected Blockchain"
        token = Token(
            blockchain=blockchain,
            data_version=999,
            collection_id=Address("Collection Address"),
            token_id=HexInt(1),
            mint_date=HexInt(2),
            mint_block=HexInt(4),
            current_owner=Address("Current Owner"),
            original_owner=Address("Original Owner"),
            quantity=HexInt(3),
            metadata_url="X" * 2048,
            attribute_version=HexInt("0x00000000000000000007"),
        )

        await self.__data_service.write_token_batch([token])
        self.__batch_writer.put_item.assert_awaited_once_with(
            Item={
                "blockchain_collection_id": ANY,
                "token_id": ANY,
                "mint_timestamp": ANY,
                "mint_block_id": ANY,
                "original_owner_account": ANY,
                "current_owner_account": ANY,
                "current_owner_version": ANY,
                "quantity": ANY,
                "metadata_url": "X" * 2048,
                "metadata_url_version": ANY,
                "data_version": ANY,
            },
        )

    async def test_write_token_batch_with_2049_char_url_does_not_store_url_data(self):
        blockchain = Mock(BlockChain)
        blockchain.value = "Expected Blockchain"
        token = Token(
            blockchain=blockchain,
            data_version=999,
            collection_id=Address("Collection Address"),
            token_id=HexInt(1),
            mint_date=HexInt(2),
            mint_block=HexInt(4),
            current_owner=Address("Current Owner"),
            original_owner=Address("Original Owner"),
            quantity=HexInt(3),
            metadata_url="X" * 2049,
            attribute_version=HexInt("0x00000000000000000007"),
        )

        await self.__data_service.write_token_batch([token])
        self.__batch_writer.put_item.assert_awaited_once_with(
            Item={
                "blockchain_collection_id": ANY,
                "token_id": ANY,
                "mint_timestamp": ANY,
                "mint_block_id": ANY,
                "original_owner_account": ANY,
                "current_owner_account": ANY,
                "current_owner_version": ANY,
                "quantity": ANY,
                "data_version": ANY,
            },
        )

    async def test_write_token_batch_with_none_url_does_not_store_url_data(self):
        blockchain = Mock(BlockChain)
        blockchain.value = "Expected Blockchain"
        token = Token(
            blockchain=blockchain,
            data_version=999,
            collection_id=Address("Collection Address"),
            token_id=HexInt(1),
            mint_date=HexInt(2),
            mint_block=HexInt(4),
            current_owner=Address("Current Owner"),
            original_owner=Address("Original Owner"),
            quantity=HexInt(3),
            metadata_url=None,
            attribute_version=HexInt("0x00000000000000000007"),
        )

        await self.__data_service.write_token_batch([token])
        self.__batch_writer.put_item.assert_awaited_once_with(
            Item={
                "blockchain_collection_id": ANY,
                "token_id": ANY,
                "mint_timestamp": ANY,
                "mint_block_id": ANY,
                "original_owner_account": ANY,
                "current_owner_account": ANY,
                "current_owner_version": ANY,
                "quantity": ANY,
                "data_version": ANY,
            },
        )

    async def test_write_token_transfer_uses_token_transfer_table_with_prefix_prepended(self):
        token_transfer = TokenTransfer(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            collection_type=EthereumCollectionType.ERC721,
            token_id=HexInt("0x10"),
            timestamp=HexInt("0x12345"),
            transaction_type=TokenTransactionType.TRANSFER,
            from_=Address("From"),
            to_=Address("To"),
            quantity=HexInt("0x1"),
            data_version=11,
            block_id=HexInt("0x80"),
            transaction_hash=HexBytes("0x9999"),
            transaction_index=HexInt("0x9"),
            log_index=HexInt("0x0"),
            attribute_version=HexInt("0x99"),
        )

        await self.__data_service.write_token_transfer(token_transfer)
        self.__dynamodb.Table.assert_awaited_once_with("pretokentransfers")

    async def test_write_token_transfer_stores_correct_data(self):
        token_transfer = TokenTransfer(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            collection_type=EthereumCollectionType.ERC721,
            token_id=HexInt("0x10"),
            timestamp=HexInt("0x12345"),
            transaction_type=TokenTransactionType.TRANSFER,
            from_=Address("From"),
            to_=Address("To"),
            quantity=HexInt("0x1"),
            data_version=11,
            block_id=HexInt("0x80"),
            transaction_hash=HexBytes("0x9999"),
            transaction_index=HexInt("0x9"),
            log_index=HexInt("0x0"),
            attribute_version=HexInt("0x99"),
        )

        await self.__data_service.write_token_transfer(token_transfer)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item={
                "blockchain_collection_id": f"{BlockChain.ETHEREUM_MAINNET.value}::Collection ID",
                "transaction_log_index_hash": "0x99",
                "collection_id": "Collection ID",
                "token_id": "0x10",
                "transaction_timestamp": 74565,
                "block_id": "0x00000080",
                "transaction_type": TokenTransactionType.TRANSFER.value,
                "from_account": "From",
                "to_account": "To",
                "quantity": "0x1",
                "transaction_hash": "0x9999",
                "transaction_index": "0x9",
                "log_index": "0x0",
                "data_version": 11,
            },
            ConditionExpression=ANY,
        )

    async def test_write_token_transfer_uses_the_correct_conditional_expression_and_attrs(self):
        token_transfer = TokenTransfer(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            data_version=999,
            collection_type=EthereumCollectionType.ERC721,
            collection_id=Address("Collection Address"),
            token_id=HexInt(1),
            timestamp=HexInt(2),
            from_=Address("From"),
            to_=Address("To"),
            quantity=HexInt(3),
            block_id=HexInt(4),
            transaction_hash=HexBytes("0xaabbccdd"),
            transaction_index=HexInt(5),
            log_index=HexInt(6),
            transaction_type=TokenTransactionType.MINT,
            attribute_version=HexInt(7),
        )

        await self.__data_service.write_token_transfer(token_transfer)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item=ANY,
            ConditionExpression=Attr("data_version").not_exists() | Attr("data_version").lte(999),
        )

    async def test_write_token_transfer_logs_debug_check_failure_when_saving(
        # noqa: E501
        self,
    ):
        token_transfer = TokenTransfer(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            data_version=999,
            collection_type=EthereumCollectionType.ERC721,
            collection_id=Address("Collection Address"),
            token_id=HexInt(1),
            timestamp=HexInt(2),
            from_=Address("From"),
            to_=Address("To"),
            quantity=HexInt(3),
            block_id=HexInt(4),
            transaction_hash=HexBytes("0xaabbccdd"),
            transaction_index=HexInt(5),
            log_index=HexInt(6),
            transaction_type=TokenTransactionType.MINT,
            attribute_version=HexInt(7),
        )

        self.__table_resource.put_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        with self.assertLogs(blockcrawler.LOGGER_NAME, logging.DEBUG) as cm:
            await self.__data_service.write_token_transfer(token_transfer)
            self.assertIn(
                f"DEBUG:{blockcrawler.LOGGER_NAME}:Token Transfer for "
                f"ethereum-mainnet:Collection Address not written -- version too old -- "
                f"999 -- {token_transfer}",
                cm.output,
            )

    async def test_write_token_transfer_batch_uses_token_transfer_table_with_prefix_prepended(self):
        token_transfer = TokenTransfer(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            collection_type=EthereumCollectionType.ERC721,
            token_id=HexInt("0x10"),
            timestamp=HexInt("0x12345"),
            transaction_type=TokenTransactionType.TRANSFER,
            from_=Address("From"),
            to_=Address("To"),
            quantity=HexInt("0x1"),
            data_version=11,
            block_id=HexInt("0x80"),
            transaction_hash=HexBytes("0x9999"),
            transaction_index=HexInt("0x9"),
            log_index=HexInt("0x0"),
            attribute_version=HexInt("0x99"),
        )

        await self.__data_service.write_token_transfer_batch([token_transfer])
        self.__dynamodb.Table.assert_awaited_once_with("pretokentransfers")

    async def test_write_token_transfer_batch_stores_correct_data(self):
        token_transfer = TokenTransfer(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            collection_type=EthereumCollectionType.ERC721,
            token_id=HexInt("0x10"),
            timestamp=HexInt("0x12345"),
            transaction_type=TokenTransactionType.TRANSFER,
            from_=Address("From"),
            to_=Address("To"),
            quantity=HexInt("0x1"),
            data_version=11,
            block_id=HexInt("0x80"),
            transaction_hash=HexBytes("0x9999"),
            transaction_index=HexInt("0x9"),
            log_index=HexInt("0x0"),
            attribute_version=HexInt("0x99"),
        )

        await self.__data_service.write_token_transfer_batch([token_transfer])
        self.__batch_writer.put_item.assert_awaited_once_with(
            Item={
                "blockchain_collection_id": f"{BlockChain.ETHEREUM_MAINNET.value}::Collection ID",
                "transaction_log_index_hash": "0x99",
                "collection_id": "Collection ID",
                "token_id": "0x10",
                "transaction_timestamp": 74565,
                "block_id": "0x00000080",
                "transaction_type": TokenTransactionType.TRANSFER.value,
                "from_account": "From",
                "to_account": "To",
                "quantity": "0x1",
                "transaction_hash": "0x9999",
                "transaction_index": "0x9",
                "log_index": "0x0",
                "data_version": 11,
            },
        )

    async def test_update_token_owner_uses_owner_table_with_prefix_prepended(self):
        token_owner = TokenOwner(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
            quantity=HexInt("0x1"),
            data_version=11,
        )

        await self.__data_service.update_token_owner(token_owner)
        self.__dynamodb.Table.assert_awaited_once_with("preowner")

    async def test_update_token_owner_stores_correct_data(self):
        await self.__data_service.update_token_owner(
            TokenOwner(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                collection_id=Address("Collection ID"),
                token_id=HexInt("0x10"),
                account=Address("Account"),
                quantity=HexInt("0x3"),
                data_version=11,
            )
        )
        self.__table_resource.update_item.assert_awaited_once_with(
            Key=dict(
                blockchain_account="ethereum-mainnet::Account",
                collection_id_token_id="Collection ID::0x10",
            ),
            UpdateExpression="SET collection_id = :collection_id,"
            "token_id = :token_id,"
            "account = :account,"
            "data_version = :data_version "
            "ADD quantity :quantity",
            ConditionExpression=(
                "attribute_not_exists(data_version) OR data_version = :data_version"
            ),
            ExpressionAttributeValues={
                ":collection_id": "Collection ID",
                ":token_id": "0x10",
                ":account": "Account",
                ":quantity": 3,
                ":data_version": 11,
            },
        )

    async def test_update_token_owner_updates_item_again_to_increment_when_check_fails(
        self,
    ):
        self.__table_resource.update_item.side_effect = [
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException,
            None,
        ]
        blockchain = MagicMock(BlockChain)
        blockchain.value = "blockchain"
        await self.__data_service.update_token_owner(
            TokenOwner(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                collection_id=Address("Collection ID"),
                token_id=HexInt("0x10"),
                account=Address("Account"),
                quantity=HexInt("0x3"),
                data_version=11,
            )
        )
        self.__table_resource.update_item.assert_has_awaits(
            [
                call(
                    Key=dict(
                        blockchain_account="ethereum-mainnet::Account",
                        collection_id_token_id="Collection ID::0x10",
                    ),
                    UpdateExpression="SET collection_id = :collection_id"
                    ",token_id = :token_id"
                    ",account = :account"
                    ",data_version = :data_version"
                    ",quantity = :quantity",
                    ConditionExpression="data_version < :data_version",
                    ExpressionAttributeValues={
                        ":collection_id": "Collection ID",
                        ":token_id": "0x10",
                        ":account": "Account",
                        ":quantity": 3,
                        ":data_version": 11,
                    },
                )
            ]
        )

    async def test_update_token_owner_debug_logs_seconds_condition_check_fails(self):
        self.__table_resource.update_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        with self.assertLogs(blockcrawler.LOGGER_NAME, logging.DEBUG) as cm:
            token_owner = TokenOwner(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                collection_id=Address("Collection ID"),
                token_id=HexInt("0x10"),
                account=Address("Account"),
                quantity=HexInt("0x3"),
                data_version=11,
            )
            await self.__data_service.update_token_owner(token_owner)
            self.assertIn(
                f"DEBUG:{blockcrawler.LOGGER_NAME}:Owner for "
                f"ethereum-mainnet:Account:Collection ID:0x10 not updated -- version too old -- "
                f"11 -- {token_owner}",
                cm.output,
            )

    async def test_update_token_owner_hits_stat_for_condition_check_fails(self):
        self.__table_resource.update_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        await self.__data_service.update_token_owner(
            TokenOwner(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                collection_id=Address("Collection ID"),
                token_id=HexInt("0x10"),
                account=Address("Account"),
                quantity=HexInt("0x3"),
                data_version=11,
            )
        )
        self.__stats_service.increment.assert_has_calls(
            [call(data_services.STAT_TOKEN_OWNER_UPDATE_DATA_TOO_OLD)]
        )

    async def test_update_token_owner_increments_count_stat(self):
        await self.__data_service.update_token_owner(
            TokenOwner(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                collection_id=Address("Collection ID"),
                token_id=HexInt("0x10"),
                account=Address("Account"),
                quantity=HexInt("0x3"),
                data_version=11,
            )
        )
        self.__stats_service.increment.assert_has_calls(
            [call(data_services.STAT_TOKEN_OWNER_UPDATE)]
        )

    async def test_update_token_owner_hits_timer_stat(self):
        await self.__data_service.update_token_owner(
            TokenOwner(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                collection_id=Address("Collection ID"),
                token_id=HexInt("0x10"),
                account=Address("Account"),
                quantity=HexInt("0x3"),
                data_version=11,
            )
        )
        self.__stats_service.ms_counter.assert_called_once_with(
            data_services.STAT_TOKEN_OWNER_UPDATE_MS
        )

    async def test_delete_zero_quantity_token_owner_uses_owner_table_with_prefix_prepended(self):
        await self.__data_service.delete_token_owner_with_zero_tokens(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
        )
        self.__dynamodb.Table.assert_awaited_once_with("preowner")

    async def test_delete_zero_quantity_token_owner_stores_correct_data(self):
        await self.__data_service.delete_token_owner_with_zero_tokens(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
        )
        self.__table_resource.delete_item.assert_awaited_once_with(
            Key=dict(
                blockchain_account="ethereum-mainnet::Account",
                collection_id_token_id="Collection ID::0x10",
            ),
            ConditionExpression="quantity = :quantity",
            ExpressionAttributeValues={
                ":quantity": 0,
            },
        )

    async def test_delete_zero_quantity_token_owner_ignores_condition_fail(self):
        self.__table_resource.delete_item.side_effect = (
            self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException
        )
        await self.__data_service.delete_token_owner_with_zero_tokens(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
        )

    async def test_delete_zero_quantity_token_owner_increments_count_stat(self):
        await self.__data_service.delete_token_owner_with_zero_tokens(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
        )
        self.__stats_service.increment.assert_has_calls(
            [call(data_services.STAT_TOKEN_OWNER_DELETE_ZERO)]
        )

    async def test_delete_zero_quantity_token_owner_hits_timer_stat(self):
        await self.__data_service.delete_token_owner_with_zero_tokens(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
        )
        self.__stats_service.ms_counter.assert_called_once_with(
            data_services.STAT_TOKEN_OWNER_DELETE_ZERO_MS
        )

    async def test_write_token_owner_batch_uses_owner_table_with_prefix_prepended(self):
        token_owner = TokenOwner(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
            quantity=HexInt("0x1"),
            data_version=11,
        )

        await self.__data_service.write_token_owner_batch([token_owner])
        self.__dynamodb.Table.assert_awaited_once_with("preowner")

    async def test_write_token_owner_batch_stores_correct_data(self):
        token_owner = TokenOwner(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
            quantity=HexInt("0x1"),
            data_version=11,
        )

        await self.__data_service.write_token_owner_batch([token_owner])
        self.__batch_writer.put_item.assert_awaited_once_with(
            Item={
                "blockchain_account": f"{BlockChain.ETHEREUM_MAINNET.value}::Account",
                "collection_id_token_id": "Collection ID::0x10",
                "collection_id": "Collection ID",
                "token_id": "0x10",
                "account": "Account",
                "quantity": 1,
                "data_version": 11,
            },
        )

    async def test_write_batch_with_parallel_batches_write_parallel_batches(self):
        batch_writer_obj_1 = AsyncMock()
        batch_writer_obj_2 = AsyncMock()
        batch_writer_obj_3 = AsyncMock()
        self.__table_resource.batch_writer.side_effect = [
            batch_writer_obj_1,
            batch_writer_obj_2,
            batch_writer_obj_3,
        ]
        batch_writer_1 = batch_writer_obj_1.__aenter__.return_value
        batch_writer_2 = batch_writer_obj_2.__aenter__.return_value
        batch_writer_3 = batch_writer_obj_3.__aenter__.return_value
        self.__data_service = DynamoDbDataService(
            self.__dynamodb, self.__stats_service, self.__table_prefix, 3
        )

        await self.__data_service.write_token_owner_batch(
            [
                TokenOwner(
                    blockchain=BlockChain.ETHEREUM_MAINNET,
                    collection_id=Address("Collection 1"),
                    token_id=HexInt(0x10),
                    account=Address("Account 1"),
                    quantity=HexInt(0x11),
                    data_version=1,
                ),
                TokenOwner(
                    blockchain=BlockChain.ETHEREUM_MAINNET,
                    collection_id=Address("Collection 2"),
                    token_id=HexInt(0x20),
                    account=Address("Account 2"),
                    quantity=HexInt(0x21),
                    data_version=2,
                ),
                TokenOwner(
                    blockchain=BlockChain.ETHEREUM_MAINNET,
                    collection_id=Address("Collection 3"),
                    token_id=HexInt(0x30),
                    account=Address("Account 3"),
                    quantity=HexInt(0x31),
                    data_version=3,
                ),
            ]
        )

        batch_writer_1.put_item.assert_awaited_once_with(
            Item=dict(
                blockchain_account=f"{BlockChain.ETHEREUM_MAINNET.value}::Account 1",
                collection_id_token_id="Collection 1::0x10",
                collection_id=Address("Collection 1"),
                token_id=HexInt(0x10).hex_value,
                account=Address("Account 1"),
                quantity=HexInt(0x11).int_value,
                data_version=1,
            )
        )

        batch_writer_2.put_item.assert_awaited_once_with(
            Item=dict(
                blockchain_account=f"{BlockChain.ETHEREUM_MAINNET.value}::Account 2",
                collection_id_token_id="Collection 2::0x20",
                collection_id=Address("Collection 2"),
                token_id=HexInt(0x20).hex_value,
                account=Address("Account 2"),
                quantity=HexInt(0x21).int_value,
                data_version=2,
            )
        )

        batch_writer_3.put_item.assert_awaited_once_with(
            Item=dict(
                blockchain_account=f"{BlockChain.ETHEREUM_MAINNET.value}::Account 3",
                collection_id_token_id="Collection 3::0x30",
                collection_id=Address("Collection 3"),
                token_id=HexInt(0x30).hex_value,
                account=Address("Account 3"),
                quantity=HexInt(0x31).int_value,
                data_version=3,
            )
        )
