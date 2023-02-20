from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, MagicMock, call

import ddt
from hexbytes import HexBytes

from blockcrawler.core.bus import DataPackage
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.types import Address, HexInt
from blockcrawler.nft.consumers import (
    NftCollectionPersistenceConsumer,
    NftTokenMintPersistenceConsumer,
    NftTokenTransferPersistenceConsumer,
    NftTokenQuantityUpdatingConsumer,
    NftMetadataUriUpdatingConsumer,
    OwnerPersistingConsumer,
    NftCurrentOwnerUpdatingConsumer,
)
from blockcrawler.nft.data_packages import (
    CollectionDataPackage,
    TokenTransferDataPackage,
    TokenMetadataUriUpdatedDataPackage,
)
from blockcrawler.nft.data_services import DataVersionTooOldException, DataService
from blockcrawler.nft.entities import (
    Collection,
    CollectionType,
    TokenTransfer,
    TokenTransactionType,
    EthereumCollectionType,
    Token,
    TokenOwner,
)


@ddt.ddt
class NftCollectionPersistenceBatchConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_service = AsyncMock(DataService)
        self.__consumer = NftCollectionPersistenceConsumer(self.__data_service)

    async def test_does_not_process_non_token_transaction_data_packages(self):
        await self.__consumer.receive(DataPackage())
        self.__data_service.write_collection.assert_not_called()

    async def test_stores_correct_data(self):
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
        data_package = CollectionDataPackage(collection)
        await self.__consumer.receive(data_package)
        self.__data_service.write_collection.assert_awaited_once_with(collection)

    async def test_does_not_react_to_data_version_too_old_when_saving(self):
        self.__data_service.write_collection.side_effect = DataVersionTooOldException
        data_package = CollectionDataPackage(
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
        self.__data_service.write_collection.assert_awaited_once()


@ddt.ddt
class NftTokenMintPersistenceConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_service = AsyncMock()
        self.__consumer = NftTokenMintPersistenceConsumer(self.__data_service)

    async def test_does_not_process_non_token_transaction_data_packages(self):
        await self.__consumer.receive(DataPackage())
        self.__data_service.write_token.assert_not_called()

    @ddt.data(TokenTransactionType.TRANSFER, TokenTransactionType.BURN, Mock(TokenTransactionType))
    async def test_does_not_process_token_transactions_other_than_mint(self, token_transfer_type):
        token_transfer_data_package = Mock(TokenTransferDataPackage)
        token_transfer_data_package.token_transfer = Mock(TokenTransfer)
        token_transfer_data_package.token_transfer.transaction_type = token_transfer_type
        await self.__consumer.receive(token_transfer_data_package)
        self.__data_service.write_token.assert_not_called()

    async def test_stores_correct_data(self):
        blockchain = Mock(BlockChain)
        blockchain.value = "Expected Blockchain"
        transaction_type = TokenTransactionType.MINT
        data_package = TokenTransferDataPackage(
            TokenTransfer(
                blockchain=blockchain,
                data_version=999,
                collection_id=Address("Collection Address"),
                collection_type=EthereumCollectionType.ERC721,
                token_id=HexInt(1),
                timestamp=HexInt(2),
                from_=Address("From"),
                to_=Address("To"),
                quantity=HexInt(1),
                block_id=HexInt(4),
                transaction_hash=HexBytes("0xaabbccdd"),
                transaction_index=HexInt(5),
                log_index=HexInt(6),
                transaction_type=transaction_type,
                attribute_version=HexInt("0x00000000000000000007"),
            ),
        )

        await self.__consumer.receive(data_package)
        self.__data_service.write_token.assert_awaited_once_with(
            Token(
                blockchain=blockchain,
                collection_id=Address("Collection Address"),
                token_id=HexInt(1),
                data_version=999,
                original_owner=Address("To"),
                current_owner=Address("To"),
                mint_block=HexInt(4),
                mint_date=HexInt(2),
                quantity=HexInt(0),
                attribute_version=HexInt("0x00000000000000000007"),
                metadata_url=None,
            )
        )


class NftTokenTransferPersistenceConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_service = AsyncMock()
        self.__consumer = NftTokenTransferPersistenceConsumer(self.__data_service)

    async def test_stores_correct_data(self):
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
        data_package = TokenTransferDataPackage(token_transfer=token_transfer)

        await self.__consumer.receive(data_package)
        self.__data_service.write_token_transfer.assert_awaited_once_with(token_transfer)


# noinspection PyDataclass,PyPropertyAccess
@ddt.ddt
class NftTokenQuantityUpdatingConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_service = AsyncMock(DataService)
        self.__consumer = NftTokenQuantityUpdatingConsumer(
            self.__data_service,
        )

    async def test_transfer_does_nothing(self):
        transfer = Mock(TokenTransfer)
        transfer.transaction_type = TokenTransactionType.TRANSFER

        data_package = TokenTransferDataPackage(transfer)
        await self.__consumer.receive(data_package)

        self.assertEqual([], self.__data_service.mock_calls)

    @ddt.data((TokenTransactionType.MINT, 12, 12), (TokenTransactionType.BURN, 13, -13))
    @ddt.unpack
    async def test_mint_and_burn_update_the_correct_token(self, tx_type, quantity, add_value):
        transfer = Mock(TokenTransfer)
        transfer.blockchain = Mock(BlockChain)
        transfer.blockchain.value = "blockchain"
        transfer.collection_id = "Collection ID"
        transfer.transaction_type = Mock(TokenTransactionType)
        transfer.token_id = Mock(HexInt)
        transfer.quantity = Mock(HexInt)
        transfer.quantity.int_value = quantity
        transfer.data_version = 999

        transfer.transaction_type = tx_type
        await self.__consumer.receive(TokenTransferDataPackage(transfer))
        self.__data_service.update_token_quantity.assert_called_once_with(
            blockchain=transfer.blockchain,
            collection_id=transfer.collection_id,
            token_id=transfer.token_id,
            quantity=add_value,
            data_version=transfer.data_version,
        )


# noinspection PyDataclass,PyPropertyAccess
class NftMetadataUriPersistingConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_service = AsyncMock(DataService)
        self.__consumer = NftMetadataUriUpdatingConsumer(self.__data_service)

    async def test_non_uri_update_does_nothing(self):
        transfer = Mock(TokenTransfer)
        transfer.transaction_type = TokenTransactionType.TRANSFER

        data_package = TokenTransferDataPackage(transfer)
        await self.__consumer.receive(data_package)
        self.assertEqual([], self.__data_service.mock_calls)

    async def test_updates_metadata_uri(self):
        data_package = Mock(TokenMetadataUriUpdatedDataPackage)
        data_package.blockchain = Mock(BlockChain)
        data_package.blockchain.value = "blockchain"
        data_package.collection_id = Address("Collection ID")
        data_package.token_id = Mock(HexInt)
        data_package.token_id.int_value = 0
        data_package.metadata_url = "Expected URI"
        data_package.metadata_url_version = Mock(HexInt)
        data_package.metadata_url_version.hex_value = "0x0"
        data_package.data_version = 999
        await self.__consumer.receive(data_package)
        self.__data_service.update_token_metadata_url.assert_awaited_once_with(
            blockchain=data_package.blockchain,
            collection_id=data_package.collection_id,
            token_id=data_package.token_id,
            metadata_url=data_package.metadata_url,
            metadata_url_version=data_package.metadata_url_version,
            data_version=data_package.data_version,
        )


class NftCurrentOwnerUpdatingConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_service = AsyncMock()
        self.__consumer = NftCurrentOwnerUpdatingConsumer(self.__data_service)

    async def test_stores_correct_data(self):
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
        data_package = TokenTransferDataPackage(token_transfer=token_transfer)

        await self.__consumer.receive(data_package)
        self.__data_service.update_token_current_owner.assert_awaited_once_with(
            blockchain=BlockChain.ETHEREUM_MAINNET,
            collection_id=Address("Collection ID"),
            token_id=HexInt("0x10"),
            owner=Address("To"),
            owner_version=HexInt("0x99"),
            data_version=11,
        )


@ddt.ddt
class OwnerPersistingConsumerTestCase(IsolatedAsyncioTestCase):
    class SpecialException(Exception):
        pass

    # noinspection PyDataclass
    async def asyncSetUp(self) -> None:
        self.__data_service = AsyncMock(DataService)
        self.__consumer = OwnerPersistingConsumer(self.__data_service)
        self.__data_package = MagicMock(TokenTransferDataPackage)
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
        self.assertEqual([], self.__data_service.mock_calls)

    async def test_adds_quantity_to_to_on_mint(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.MINT
        await self.__consumer.receive(self.__data_package)
        self.__data_service.update_token_owner.assert_awaited_once_with(
            TokenOwner(
                blockchain=self.__data_package.token_transfer.blockchain,
                account=self.__data_package.token_transfer.to_,
                collection_id=self.__data_package.token_transfer.collection_id,
                token_id=self.__data_package.token_transfer.token_id,
                quantity=self.__data_package.token_transfer.quantity,
                data_version=self.__data_package.token_transfer.data_version,
            )
        )

    async def test_subtracts_quantity_from_from_on_burn(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.BURN
        await self.__consumer.receive(self.__data_package)
        self.__data_service.update_token_owner.assert_awaited_once_with(
            TokenOwner(
                blockchain=self.__data_package.token_transfer.blockchain,
                account=self.__data_package.token_transfer.from_,
                collection_id=self.__data_package.token_transfer.collection_id,
                token_id=self.__data_package.token_transfer.token_id,
                quantity=0 - self.__data_package.token_transfer.quantity,
                data_version=self.__data_package.token_transfer.data_version,
            )
        )

    async def test_adds_quantity_to_to_and_subtracts_quantity_from_from_on_transfer(self):
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.TRANSFER
        await self.__consumer.receive(self.__data_package)
        self.__data_service.update_token_owner.assert_has_calls(
            [
                call(
                    TokenOwner(
                        blockchain=self.__data_package.token_transfer.blockchain,
                        account=self.__data_package.token_transfer.to_,
                        collection_id=self.__data_package.token_transfer.collection_id,
                        token_id=self.__data_package.token_transfer.token_id,
                        quantity=self.__data_package.token_transfer.quantity,
                        data_version=self.__data_package.token_transfer.data_version,
                    )
                ),
                call(
                    TokenOwner(
                        blockchain=self.__data_package.token_transfer.blockchain,
                        account=self.__data_package.token_transfer.from_,
                        collection_id=self.__data_package.token_transfer.collection_id,
                        token_id=self.__data_package.token_transfer.token_id,
                        quantity=0 - self.__data_package.token_transfer.quantity,
                        data_version=self.__data_package.token_transfer.data_version,
                    )
                ),
            ],
            any_order=True,
        )

    async def test_sends_delete_for_zero_quantity_on_from_after_burn(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.BURN
        await self.__consumer.receive(self.__data_package)
        self.__data_service.delete_token_owner_with_zero_tokens.assert_awaited_once_with(
            blockchain=self.__data_package.token_transfer.blockchain,
            collection_id=self.__data_package.token_transfer.collection_id,
            token_id=self.__data_package.token_transfer.token_id,
            account=self.__data_package.token_transfer.from_,
        )

    async def test_sends_delete_for_zero_quantity_on_to_after_mint(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.MINT
        await self.__consumer.receive(self.__data_package)
        self.__data_service.delete_token_owner_with_zero_tokens.assert_awaited_once_with(
            blockchain=self.__data_package.token_transfer.blockchain,
            collection_id=self.__data_package.token_transfer.collection_id,
            token_id=self.__data_package.token_transfer.token_id,
            account=self.__data_package.token_transfer.to_,
        )

    async def test_sends_delete_for_zero_quantity_on_from_and_to_after_transfer(self):
        # noinspection PyDataclass
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.TRANSFER
        await self.__consumer.receive(self.__data_package)
        self.__data_service.delete_token_owner_with_zero_tokens.assert_has_awaits(
            [
                call(
                    blockchain=self.__data_package.token_transfer.blockchain,
                    collection_id=self.__data_package.token_transfer.collection_id,
                    token_id=self.__data_package.token_transfer.token_id,
                    account=self.__data_package.token_transfer.from_,
                ),
                call(
                    blockchain=self.__data_package.token_transfer.blockchain,
                    collection_id=self.__data_package.token_transfer.collection_id,
                    token_id=self.__data_package.token_transfer.token_id,
                    account=self.__data_package.token_transfer.to_,
                ),
            ],
            any_order=True,
        )
