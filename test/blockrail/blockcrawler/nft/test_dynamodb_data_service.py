import unittest
from unittest.mock import Mock, AsyncMock, ANY, MagicMock

import ddt
from boto3.dynamodb.conditions import Attr
from hexbytes import HexBytes

from blockrail.blockcrawler.core.entities import HexInt, BlockChain
from blockrail.blockcrawler.core.stats import StatsService
from blockrail.blockcrawler.nft.data_services.dynamodb import DynamoDbDataService
from blockrail.blockcrawler.nft.data_services import DataVersionTooOldException
from blockrail.blockcrawler.evm.types import Address
from blockrail.blockcrawler.nft.entities import (
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
        self.__dynamodb.exceptions.ConditionalCheckFailedException = Exception
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
                "creator": "Creator",
                "owner": "Owner",
                "date_created": "0x1234",
                "name": "Name",
                "name_lower": "name",
                "symbol": "Symbol",
                "total_supply": "0x100",
                "specification": "Expected Collection Type",
                "data_version": 999,
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

    async def test_write_collection_raises_expected_exception_for_condition_check_failure_when_saving(  # noqa: E501
        self,
    ):
        self.__table_resource.put_item.side_effect = (
            self.__dynamodb.exceptions.ConditionalCheckFailedException
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
                "creator": ANY,
                "owner": ANY,
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
        self.__table_resource.put_item.assert_awaited_once_with(
            Item={
                "blockchain_collection_id": "Expected Blockchain::Collection Address",
                "token_id": "0x1",
                "mint_date": 2,
                "mint_block": "0x4",
                "original_owner": "Original Owner",
                "current_owner": "Current Owner",
                "current_owner_version": "0x00000000000000000007",
                "quantity": 3,
                "metadata_url": "Metadata URL",
                "data_version": 999,
            },
            ConditionExpression=ANY,
        )

    async def test_write_token_uses_the_correct_conditional_expression_and_attrs(self):
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
        self.__table_resource.put_item.assert_awaited_once_with(
            Item=ANY,
            ConditionExpression=Attr("data_version").not_exists() | Attr("data_version").lte(999),
        )

    async def test_write_token_raises_expected_exception_for_condition_check_failure_when_saving(
        self,
    ):
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

        self.__table_resource.put_item.side_effect = (
            self.__dynamodb.exceptions.ConditionalCheckFailedException
        )
        with self.assertRaises(DataVersionTooOldException):
            await self.__data_service.write_token(token)
            self.__table_resource.put_item.assert_awaited_once()

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
                "mint_date": 2,
                "mint_block": "0x4",
                "original_owner": "Original Owner",
                "current_owner": "Current Owner",
                "current_owner_version": "0x00000000000000000007",
                "quantity": 3,
                "metadata_url": "Metadata URL",
                "data_version": 999,
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
                "timestamp": 74565,
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

    async def test_write_token_transfer_raises_expected_exception_for_condition_check_failure_when_saving(  # noqa: E501
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
            self.__dynamodb.exceptions.ConditionalCheckFailedException
        )
        with self.assertRaises(DataVersionTooOldException):
            await self.__data_service.write_token_transfer(token_transfer)
            self.__table_resource.put_item.assert_awaited_once()

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
                "timestamp": 74565,
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

    async def test_write_token_owner_uses_owner_table_with_prefix_prepended(self):
        token_owner = TokenOwner(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
            quantity=HexInt("0x1"),
            data_version=11,
        )

        await self.__data_service.write_token_owner(token_owner)
        self.__dynamodb.Table.assert_awaited_once_with("preowner")

    async def test_write_token_owner_stores_correct_data(self):
        token_owner = TokenOwner(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
            quantity=HexInt("0x1"),
            data_version=11,
        )

        await self.__data_service.write_token_owner(token_owner)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item={
                "blockchain_account": f"{BlockChain.ETHEREUM_MAINNET.value}::Account",
                "collection_id_token_id": "Collection ID::0x10",
                "collection_id": "Collection ID",
                "token_id": "0x10",
                "account": "Account",
                "quantity": "0x1",
                "data_version": 11,
            },
            ConditionExpression=ANY,
        )

    async def test_write_token_owner_uses_the_correct_conditional_expression_and_attrs(self):
        token_owner = TokenOwner(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
            quantity=HexInt("0x1"),
            data_version=11,
        )

        await self.__data_service.write_token_owner(token_owner)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item=ANY,
            ConditionExpression=Attr("data_version").not_exists() | Attr("data_version").lte(11),
        )

    async def test_write_token_owner_raises_expected_exception_for_condition_check_failure_when_saving(  # noqa: E501
        self,
    ):
        token_owner = TokenOwner(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            account=Address("Account"),
            quantity=HexInt("0x1"),
            data_version=11,
        )

        self.__table_resource.put_item.side_effect = (
            self.__dynamodb.exceptions.ConditionalCheckFailedException
        )
        with self.assertRaises(DataVersionTooOldException):
            await self.__data_service.write_token_owner(token_owner)
            self.__table_resource.put_item.assert_awaited_once()

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
                "quantity": "0x1",
                "data_version": 11,
            },
        )
