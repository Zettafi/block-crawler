from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, ANY, MagicMock, call

import ddt
from boto3.dynamodb.conditions import Attr
from hexbytes import HexBytes

from blockrail.blockcrawler.core.bus import DataPackage, ConsumerError
from blockrail.blockcrawler.core.data_clients import (
    HttpDataClient,
    IpfsDataClient,
    ArweaveDataClient,
    DataUriDataClient,
    UnsupportedProtocolError,
)
from blockrail.blockcrawler.core.entities import BlockChain, HexInt
from blockrail.blockcrawler.evm.types import Address
from blockrail.blockcrawler.nft.consumers import (
    NftCollectionPersistenceConsumer,
    NftTokenMintPersistenceConsumer,
    NftTokenTransferPersistenceConsumer,
    NftTokenQuantityUpdatingConsumer,
    NftMetadataUriUpdatingConsumer,
    NftTokenMetadataPersistingConsumer,
    CurrentOwnerPersistingConsumer,
)
from blockrail.blockcrawler.nft.data_packages import (
    NftCollectionDataPackage,
    NftTokenTransferDataPackage,
    NftTokenMetadataUriUpdatedDataPackage,
)
from blockrail.blockcrawler.nft.entities import (
    Collection,
    CollectionType,
    TokenTransfer,
    TokenTransactionType,
    EthereumCollectionType,
)
from blockrail.blockcrawler.nft.evm import LogVersionOracle


@ddt.ddt
class NftCollectionPersistenceBatchConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__table_resource = AsyncMock()
        self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException = Exception
        self.__consumer = NftCollectionPersistenceConsumer(self.__table_resource)

    async def test_stores_correct_data(self):
        blockchain = Mock(BlockChain)
        blockchain.value = "Expected Blockchain"
        collection_type = "Expected Collection Type"
        data_package = NftCollectionDataPackage(
            Collection(
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
        )
        await self.__consumer.receive(data_package)
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

    async def test_uses_the_correct_conditional_expression_and_attrs(self):
        data_package = NftCollectionDataPackage(
            Collection(
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
        )

        await self.__consumer.receive(data_package)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item=ANY,
            ConditionExpression=Attr("data_version").not_exists() | Attr("data_version").lte(999),
        )

    async def test_does_not_react_to_condition_check_failure_when_saving(self):
        self.__table_resource.put_item.side_effect = (
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException
        )
        data_package = NftCollectionDataPackage(
            Collection(
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
        )
        await self.__consumer.receive(data_package)
        self.__table_resource.put_item.assert_awaited_once()


@ddt.ddt
class NftTokenMintPersistenceConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__table_resource = AsyncMock()
        self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException = Exception
        self.__consumer = NftTokenMintPersistenceConsumer(self.__table_resource)

    async def test_does_not_process_non_token_transaction_data_packages(self):
        await self.__consumer.receive(DataPackage())
        self.__table_resource.put_item.assert_not_called()

    @ddt.data(TokenTransactionType.TRANSFER, TokenTransactionType.BURN, Mock(TokenTransactionType))
    async def test_does_not_process_token_transactions_other_than_mint(self, token_transfer_type):
        token_transfer_data_package = Mock(NftTokenTransferDataPackage)
        token_transfer_data_package.token_transfer = Mock(TokenTransfer)
        token_transfer_data_package.token_transfer.transaction_type = token_transfer_type
        await self.__consumer.receive(token_transfer_data_package)
        self.__table_resource.put_item.assert_not_called()

    async def test_stores_correct_data(self):
        blockchain = Mock(BlockChain)
        blockchain.value = "Expected Blockchain"
        transaction_type = TokenTransactionType.MINT
        data_package = NftTokenTransferDataPackage(
            TokenTransfer(
                blockchain=blockchain,
                data_version=999,
                collection_id=Address("Collection Address"),
                token_id=HexInt(1),
                timestamp=HexInt(2),
                from_=Address("From"),
                to_=Address("To"),
                quantity=HexInt(0),
                block_id=HexInt(4),
                transaction_hash=HexBytes("0xaabbccdd"),
                transaction_index=HexInt(5),
                log_index=HexInt(6),
                transaction_type=transaction_type,
                attribute_version=HexInt("0x00000000000000000007"),
            ),
        )

        await self.__consumer.receive(data_package)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item={
                "blockchain_collection_id": "Expected Blockchain::Collection Address",
                "token_id": "0x1",
                "mint_date": 2,
                "mint_block": "0x4",
                "original_owner": "To",
                "current_owner": "To",
                "current_owner_version": "0x00000000000000000007",
                "quantity": 0,
                "data_version": 999,
            },
            ConditionExpression=ANY,
        )

    async def test_uses_the_correct_conditional_expression_and_attrs(self):
        data_package = NftTokenTransferDataPackage(
            TokenTransfer(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                data_version=999,
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
            ),
        )

        await self.__consumer.receive(data_package)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item=ANY,
            ConditionExpression=Attr("data_version").not_exists() | Attr("data_version").lte(999),
        )

    async def test_does_not_react_to_condition_check_failure_when_saving(self):
        data_package = NftTokenTransferDataPackage(
            TokenTransfer(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                data_version=999,
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
            ),
        )

        self.__table_resource.put_item.side_effect = (
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException
        )
        await self.__consumer.receive(data_package)
        self.__table_resource.put_item.assert_awaited_once()


class NftTokenTransferPersistenceConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__table_resource = AsyncMock()
        self.__version_oracle = MagicMock(LogVersionOracle)
        self.__version_oracle.version.return_value = HexInt("0x0006429ed72aff20b9947825a")
        self.__consumer = NftTokenTransferPersistenceConsumer(
            self.__table_resource, self.__version_oracle
        )

    async def test_stores_correct_data(self):
        data_package = NftTokenTransferDataPackage(
            token_transfer=TokenTransfer(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                collection_id=Address("Collection ID"),
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
                attribute_version=HexInt("0x0"),
            )
        )

        await self.__consumer.receive(data_package)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item={
                "blockchain_collection_id": f"{BlockChain.ETHEREUM_MAINNET.value}::Collection ID",
                "transaction_log_index_hash": "0x0006429ed72aff20b9947825a",
                "collection_id": "Collection ID",
                "token_id": "0x10",
                "timestamp": 74565,
                "block_id": "0x80",
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

    async def test_uses_the_correct_conditional_expression_and_attrs(self):
        data_package = NftTokenTransferDataPackage(
            TokenTransfer(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                data_version=999,
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
            ),
        )

        await self.__consumer.receive(data_package)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item=ANY,
            ConditionExpression=Attr("data_version").not_exists() | Attr("data_version").lte(999),
        )

    async def test_does_not_react_to_condition_check_failure_when_saving(self):
        data_package = NftTokenTransferDataPackage(
            TokenTransfer(
                blockchain=BlockChain.ETHEREUM_MAINNET,
                data_version=999,
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
            ),
        )

        self.__table_resource.put_item.side_effect = (
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException
        )
        await self.__consumer.receive(data_package)
        self.__table_resource.put_item.assert_awaited_once()


# noinspection PyDataclass,PyPropertyAccess
@ddt.ddt
class NftTokenQuantityUpdatingConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__table_resource = AsyncMock()
        self.__consumer = NftTokenQuantityUpdatingConsumer(self.__table_resource)
        transfer = Mock(TokenTransfer)
        transfer.blockchain = Mock(BlockChain)
        transfer.blockchain.value = "blockchain"
        transfer.collection_id = "Collection ID"
        transfer.transaction_type = Mock(TokenTransactionType)
        transfer.token_id = Mock(HexInt)
        transfer.quantity = Mock(HexInt)
        transfer.quantity.int_value = 0
        transfer.data_version = 0

        self.__data_package = NftTokenTransferDataPackage(transfer)

    async def test_transfer_does_nothing(self):
        transfer = Mock(TokenTransfer)
        transfer.transaction_type = TokenTransactionType.TRANSFER

        data_package = NftTokenTransferDataPackage(transfer)
        await self.__consumer.receive(data_package)

        self.__table_resource.update_item.assert_not_called()

    @ddt.data(TokenTransactionType.MINT, TokenTransactionType.BURN)
    async def test_mint_and_burn_update_the_correct_token(self, tx_type):
        self.__data_package.token_transfer.transaction_type = tx_type
        self.__data_package.token_transfer.blockchain.value = "blockchain"
        self.__data_package.token_transfer.collection_id = "Collection ID"
        self.__data_package.token_transfer.token_id.hex_value = "Token ID"
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.update_item.assert_called_once_with(
            Key=dict(
                blockchain_collection_id="blockchain::Collection ID",
                token_id="Token ID",
            ),
            UpdateExpression=ANY,
            ExpressionAttributeValues=ANY,
            ConditionExpression=ANY,
        )

    @ddt.data((TokenTransactionType.MINT, 12, 12), (TokenTransactionType.BURN, 13, -13))
    @ddt.unpack
    async def test_modifies_quantity_correctly(self, tx_type, quantity, add_value):
        self.__data_package.token_transfer.transaction_type = tx_type
        self.__data_package.token_transfer.quantity.int_value = abs(quantity)
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.update_item.assert_called_once_with(
            Key=ANY,
            UpdateExpression="ADD quantity :q",
            ExpressionAttributeValues={":q": add_value},
            ConditionExpression=ANY,
        )

    async def test_updates_requires_data_version_is_same(self):
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.MINT
        self.__data_package.token_transfer.data_version = 999
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.update_item.assert_called_once_with(
            Key=ANY,
            UpdateExpression=ANY,
            ExpressionAttributeValues=ANY,
            ConditionExpression=Attr("data_version").eq(999),
        )


# noinspection PyDataclass,PyPropertyAccess
class NftMetadataUriPersistingConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__table_resource = AsyncMock()
        self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException = Exception
        self.__consumer = NftMetadataUriUpdatingConsumer(self.__table_resource)
        transfer = Mock(TokenTransfer)
        transfer.blockchain = Mock(BlockChain)
        transfer.blockchain.value = "blockchain"
        transfer.collection_id = "Collection ID"
        transfer.transaction_type = Mock(TokenTransactionType)
        transfer.token_id = Mock(HexInt)
        transfer.quantity = Mock(HexInt)
        transfer.quantity.int_value = 0
        transfer.data_version = 0

        self.__data_package = Mock(NftTokenMetadataUriUpdatedDataPackage)
        self.__data_package.blockchain = Mock(BlockChain)
        self.__data_package.blockchain.value = "blockchain"
        self.__data_package.collection_id = Address("Collection ID")
        self.__data_package.token_id = Mock(HexInt)
        self.__data_package.token_id.int_value = 0
        self.__data_package.metadata_uri = "Expected URI"
        self.__data_package.metadata_uri_version = Mock(HexInt)
        self.__data_package.metadata_uri_version.hex_value = "0x0"
        self.__data_package.data_version = 0

    async def test_non_uri_update_does_nothing(self):
        transfer = Mock(TokenTransfer)
        transfer.transaction_type = TokenTransactionType.TRANSFER

        data_package = NftTokenTransferDataPackage(transfer)
        await self.__consumer.receive(data_package)

        self.__table_resource.update_item.assert_not_called()

    async def test_updates_metadata_uri_correctly(self):
        self.__data_package.metadata_uri = "Expected"
        self.__data_package.data_version = 99
        self.__data_package.metadata_uri_version.hex_value = "0x100"
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.update_item.assert_called_once_with(
            Key=ANY,
            UpdateExpression="SET metadata_uri = :metadata_uri, "
            "metadata_uri_version = :metadata_uri_version",
            ExpressionAttributeValues={
                ":metadata_uri": "Expected",
                ":data_version": 99,
                ":metadata_uri_version": "0x100",
            },
            ConditionExpression="data_version = :data_version"
            " AND (attribute_not_exists(metadata_uri_version)"
            " OR metadata_uri_version <= :metadata_uri_version)",
        )

    async def test_ignores_error_when_metadata_uri_version_is_older(self):
        self.__table_resource.update_item.side_effect = (
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException
        )
        self.__table_resource.get_item.return_value = dict(
            Item=dict(
                data_version=998,
                metadata_uri_version="0x1",
            ),
        )
        self.__data_package.blockchain.value = "Blockchain"
        self.__data_package.collection_id = Address("Collection")
        self.__data_package.token_id.hex_value = "0x2"
        self.__data_package.data_version = 999
        self.__data_package.metadata_uri_version.hex_value = "0x0"
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.get_item.assert_awaited_once_with(
            Key=dict(
                blockchain_collection_id="Blockchain::Collection",
                token_id="0x2",
            )
        )
        # If there is no Exception raised, success

    async def test_errors_when_data_version_is_not_same_or_lower(self):
        self.__table_resource.update_item.side_effect = (
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException
        )
        self.__table_resource.get_item.return_value = dict(
            Item=dict(
                data_version=999,
                metadata_uri_version="0x0",
            )
        )
        self.__data_package.data_version = 998
        with self.assertRaises(
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException
        ):
            await self.__consumer.receive(self.__data_package)


@ddt.ddt
class NftTokenMetadataPersistingConsumerTestCase(IsolatedAsyncioTestCase):
    URI_TO_MOCK = (
        ("http://metadata/uri", "http_data_client"),
        ("https://metadata/uri", "http_data_client"),
        ("ipfs://metadata/uri", "ipfs_data_client"),
        ("ar://metadata/uri", "arweave_data_client"),
        ("data:,metadata", "data_uri_data_client"),
    )

    def return_client_response(self):
        return self.__client_response

    async def asyncSetUp(self) -> None:
        self.__client_response = None, None
        self.http_data_client = AsyncMock(HttpDataClient)
        self.http_data_client.get.return_value.__aenter__.side_effect = self.return_client_response
        self.http_data_client.get.return_value.__aexit__.return_value = None
        self.ipfs_data_client = AsyncMock(IpfsDataClient)
        self.ipfs_data_client.get.return_value.__aenter__.side_effect = self.return_client_response
        self.ipfs_data_client.get.return_value.__aexit__.return_value = None
        self.arweave_data_client = AsyncMock(ArweaveDataClient)
        self.arweave_data_client.get.return_value.__aenter__.side_effect = (
            self.return_client_response
        )
        self.arweave_data_client.get.return_value.__aexit__.return_value = None
        self.data_uri_data_client = AsyncMock(DataUriDataClient)
        self.data_uri_data_client.get.return_value.__aenter__.side_effect = (
            self.return_client_response
        )
        self.data_uri_data_client.get.return_value.__aexit__.return_value = None
        self.__s3_bucket = AsyncMock()

        self.__consumer = NftTokenMetadataPersistingConsumer(
            http_client=self.http_data_client,
            ipfs_client=self.ipfs_data_client,
            arweave_client=self.arweave_data_client,
            data_uri_client=self.data_uri_data_client,
            s3_bucket=self.__s3_bucket,
        )

        self.__data_package = Mock(NftTokenMetadataUriUpdatedDataPackage)
        self.__data_package.blockchain = Mock(BlockChain)
        self.__data_package.blockchain.value = "blockchain"
        self.__data_package.collection_id = Address("Collection ID")
        self.__data_package.token_id = Mock(HexInt)
        self.__data_package.token_id.hex_value = "0x0"
        self.__data_package.metadata_uri = "URI"
        self.__data_package.metadata_uri_version = Mock(HexInt)
        self.__data_package.metadata_uri_version.hex_value = "0x0"

    @ddt.data(*URI_TO_MOCK)
    @ddt.unpack
    async def test_uses_correct_client_for_uri(self, metadata_uri, mock_attr):
        self.__data_package.metadata_uri = metadata_uri[:]
        await self.__consumer.receive(self.__data_package)
        mock = getattr(self, mock_attr)
        mock.get.assert_called_once_with(metadata_uri)

    @ddt.data(*URI_TO_MOCK)
    @ddt.unpack
    async def test_sends_data_client_response_to_s3(self, metadata_uri, _):
        self.__data_package.metadata_uri = metadata_uri[:]
        self.__client_response = (None, "Expected")
        await self.__consumer.receive(self.__data_package)
        self.__s3_bucket.upload_fileobj.assert_awaited_once_with(
            "Expected",
            Key=ANY,
            ExtraArgs=ANY,
        )

    @ddt.data(*URI_TO_MOCK)
    @ddt.unpack
    async def test_sends_content_type_to_s3(self, metadata_uri, _):
        self.__data_package.metadata_uri = metadata_uri[:]
        self.__client_response = (b"expected content type", None)
        await self.__consumer.receive(self.__data_package)
        self.__s3_bucket.upload_fileobj.assert_awaited_once_with(
            ANY,
            Key=ANY,
            ExtraArgs=dict(
                ContentType=b"expected content type",
            ),
        )

    @ddt.data(*URI_TO_MOCK)
    @ddt.unpack
    async def test_sends_metadata_with_correct_key_s3(self, metadata_uri, _):
        self.__data_package.metadata_uri = metadata_uri[:]
        self.__data_package.blockchain.value = "blockchain"
        self.__data_package.collection_id = Address("0x1")
        self.__data_package.token_id.hex_value = "0x2"
        self.__data_package.metadata_uri_version.hex_value = "0x3"
        expected = "blockchain/0x1/0x2/0x3"
        await self.__consumer.receive(self.__data_package)
        self.__s3_bucket.upload_fileobj.assert_awaited_once_with(
            ANY,
            Key=expected,
            ExtraArgs=ANY,
        )

    async def test_raises_when_protocol_not_supported(self):
        self.__data_package.metadata_uri = "Not Supported"
        with self.assertRaises(UnsupportedProtocolError):
            await self.__consumer.receive(self.__data_package)

    @ddt.data("https://ipfs.io/ipfs/hash", "https://ipfs.infura.io/ipfs/hash")
    async def test_uses_ipfs_for_http_versions_of_ipfs(self, uri):
        self.__data_package.metadata_uri = uri
        await self.__consumer.receive(self.__data_package)
        self.ipfs_data_client.get.assert_called_once_with("ipfs://hash")


@ddt.ddt
class CurrentOwnerPersistingConsumerTestCase(IsolatedAsyncioTestCase):
    class SpecialException(Exception):
        pass

    # noinspection PyDataclass
    async def asyncSetUp(self) -> None:
        self.__table_resource = AsyncMock()
        self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException = (
            self.SpecialException
        )
        self.__table_resource.delete_item.side_effect = (
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException
        )
        self.__consumer = CurrentOwnerPersistingConsumer(
            self.__table_resource,
        )
        self.__data_package = MagicMock(NftTokenTransferDataPackage)
        self.__data_package.token_transfer = MagicMock(TokenTransfer)
        self.__data_package.token_transfer.blockchain = Mock(BlockChain)  #
        self.__data_package.token_transfer.blockchain.value = "blockchain"
        self.__data_package.token_transfer.collection_id = Address("Collection ID")
        self.__data_package.token_transfer.token_id = HexInt("0x01")
        self.__data_package.token_transfer.from_ = Address("From")
        self.__data_package.token_transfer.to_ = Address("To")
        self.__data_package.token_transfer.quantity = HexInt("0x3")
        self.__data_package.token_transfer.data_version = 11
        self.__data_package.token_transfer.attribute_version = HexInt("0x02")

    async def test_ignores_non_token_transfer_data_packages(self):
        await self.__consumer.receive(DataPackage())
        self.__table_resource.update_item.assert_not_called()
        self.__table_resource.get_item.assert_not_called()
        self.__table_resource.delete_item.assert_not_called()

    async def test_adds_quantity_to_to_on_mint(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.MINT
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.update_item.assert_awaited_once_with(
            Key=dict(
                blockchain_account="blockchain::To",
                collection_id_token_id="Collection ID::0x01",
            ),
            UpdateExpression="SET collection_id = :cid,"
            "token_id = :tid,"
            "account = :a,"
            "data_version = :dv "
            "ADD quantity :q",
            ExpressionAttributeValues={
                ":cid": "Collection ID",
                ":tid": "0x01",
                ":a": "To",
                ":q": 3,
                ":dv": 11,
            },
            ConditionExpression=Attr("data_version").not_exists() | Attr("data_version").eq(11),
        )

    async def test_subtracts_quantity_from_from_on_burn(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.BURN
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.update_item.assert_awaited_once_with(
            Key=dict(
                blockchain_account="blockchain::From",
                collection_id_token_id="Collection ID::0x01",
            ),
            UpdateExpression="SET collection_id = :cid,"
            "token_id = :tid,"
            "account = :a,"
            "data_version = :dv "
            "ADD quantity :q",
            ExpressionAttributeValues={
                ":cid": "Collection ID",
                ":tid": "0x01",
                ":a": "From",
                ":q": -3,
                ":dv": 11,
            },
            ConditionExpression=Attr("data_version").not_exists() | Attr("data_version").eq(11),
        )

    async def test_adds_quantity_to_to_and_subtracts_quantity_from_from_on_transfer(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.TRANSFER
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.update_item.assert_has_awaits(
            [
                call(
                    Key=dict(
                        blockchain_account="blockchain::To",
                        collection_id_token_id="Collection ID::0x01",
                    ),
                    UpdateExpression="SET collection_id = :cid,"
                    "token_id = :tid,"
                    "account = :a,"
                    "data_version = :dv "
                    "ADD quantity :q",
                    ExpressionAttributeValues={
                        ":cid": "Collection ID",
                        ":tid": "0x01",
                        ":a": "To",
                        ":q": 3,
                        ":dv": 11,
                    },
                    ConditionExpression=Attr("data_version").not_exists()
                    | Attr("data_version").eq(11),
                ),
                call(
                    Key=dict(
                        blockchain_account="blockchain::From",
                        collection_id_token_id="Collection ID::0x01",
                    ),
                    UpdateExpression="SET collection_id = :cid,"
                    "token_id = :tid,"
                    "account = :a,"
                    "data_version = :dv "
                    "ADD quantity :q",
                    ExpressionAttributeValues={
                        ":cid": "Collection ID",
                        ":tid": "0x01",
                        ":a": "From",
                        ":q": -3,
                        ":dv": 11,
                    },
                    ConditionExpression=Attr("data_version").not_exists()
                    | Attr("data_version").eq(11),
                ),
            ],
            any_order=True,
        )

    async def test_sends_delete_for_zero_quantity_on_from_after_burn(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.BURN
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.delete_item.assert_awaited_once_with(
            Key=dict(
                blockchain_account="blockchain::From",
                collection_id_token_id="Collection ID::0x01",
            ),
            ConditionExpression=Attr("quantity").eq(0),
        )

    async def test_sends_delete_for_zero_quantity_on_to_after_mint(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.MINT
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.delete_item.assert_awaited_once_with(
            Key=dict(
                blockchain_account="blockchain::To",
                collection_id_token_id="Collection ID::0x01",
            ),
            ConditionExpression=Attr("quantity").eq(0),
        )

    async def test_sends_delete_for_zero_quantity_on_from_and_to_after_transfer(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.TRANSFER
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.delete_item.assert_has_awaits(
            [
                call(
                    Key=dict(
                        blockchain_account="blockchain::From",
                        collection_id_token_id="Collection ID::0x01",
                    ),
                    ConditionExpression=Attr("quantity").eq(0),
                ),
                call(
                    Key=dict(
                        blockchain_account="blockchain::To",
                        collection_id_token_id="Collection ID::0x01",
                    ),
                    ConditionExpression=Attr("quantity").eq(0),
                ),
            ],
            any_order=True,
        )

    async def test_resets_counter_when_data_version_is_newer(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.MINT
        self.__table_resource.update_item.side_effect = [
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException,
            None,
        ]
        self.__table_resource.get_item.return_value = dict(
            Item=dict(
                data_version=10,
            ),
        )
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.put_item.assert_awaited_once_with(
            Item=dict(
                blockchain_account="blockchain::To",
                collection_id_token_id="Collection ID::0x01",
                collection_id="Collection ID",
                token_id="0x01",
                account="To",
                quantity=3,
                data_version=11,
            ),
            ConditionExpression=Attr("data_version").lt(11),
        )

    async def test_tries_to_increment_again_after_reset_counter_fails_when_data_version_was_newer(
        self,
    ):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.MINT
        self.__table_resource.update_item.side_effect = [
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException,
            None,
        ]
        self.__table_resource.put_item.side_effect = (
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException
        )
        self.__table_resource.get_item.return_value = dict(
            Item=dict(
                data_version=10,
            ),
        )
        await self.__consumer.receive(self.__data_package)
        self.__table_resource.update_item.assert_has_awaits(
            [
                call(
                    Key=dict(
                        blockchain_account="blockchain::To",
                        collection_id_token_id="Collection ID::0x01",
                    ),
                    UpdateExpression="SET collection_id = :cid,"
                    "token_id = :tid,"
                    "account = :a,"
                    "data_version = :dv "
                    "ADD quantity :q",
                    ExpressionAttributeValues={
                        ":cid": "Collection ID",
                        ":tid": "0x01",
                        ":a": "To",
                        ":q": 3,
                        ":dv": 11,
                    },
                    ConditionExpression=Attr("data_version").not_exists()
                    | Attr("data_version").eq(11),
                ),
                call(
                    Key=dict(
                        blockchain_account="blockchain::To",
                        collection_id_token_id="Collection ID::0x01",
                    ),
                    UpdateExpression="SET collection_id = :cid,"
                    "token_id = :tid,"
                    "account = :a,"
                    "data_version = :dv "
                    "ADD quantity :q",
                    ExpressionAttributeValues={
                        ":cid": "Collection ID",
                        ":tid": "0x01",
                        ":a": "To",
                        ":q": 3,
                        ":dv": 11,
                    },
                    ConditionExpression=Attr("data_version").not_exists()
                    | Attr("data_version").eq(11),
                ),
            ]
        )

    async def test_checks_database_data_version_and_errors_when_database_is_newer(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.MINT
        self.__table_resource.update_item.side_effect = [
            self.__table_resource.meta.client.exceptions.ConditionalCheckFailedException,
            None,
        ]
        self.__table_resource.get_item.return_value = dict(
            Item=dict(
                data_version=12,
            ),
        )
        with self.assertRaisesRegex(ConsumerError, "Failed to add 3 to quantity for owner"):
            await self.__consumer.receive(self.__data_package)

        self.__table_resource.get_item.assert_called_once_with(
            Key=dict(
                blockchain_account="blockchain::To",
                collection_id_token_id="Collection ID::0x01",
            ),
        )
