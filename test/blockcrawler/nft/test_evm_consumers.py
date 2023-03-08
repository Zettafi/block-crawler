from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, call, ANY

import ddt
from eth_abi import encode
from hexbytes import HexBytes

from blockcrawler.core.bus import DataPackage
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.rpc import RpcServerError, RpcDecodeError
from blockcrawler.core.types import Address, HexInt
from blockcrawler.evm.rpc import EvmRpcClient, EthCall
from blockcrawler.evm.services import BlockTimeService
from blockcrawler.evm.types import (
    EvmLog,
    Erc721MetadataFunctions,
    Erc721Events,
    Erc1155Events,
    Erc165Functions,
)
from blockcrawler.nft.data_packages import (
    CollectionDataPackage,
)
from blockcrawler.nft.data_services import DataService
from blockcrawler.nft.entities import (
    Collection,
    EthereumCollectionType,
    TokenTransfer,
    Token,
    TokenTransactionType,
    TokenOwner,
)
from blockcrawler.nft.evm.consumers import (
    CollectionToEverythingElseErc721CollectionBasedConsumer,
    CollectionToEverythingElseErc1155CollectionBasedConsumer,
)
from blockcrawler.nft.evm.oracles import TokenTransactionTypeOracle, LogVersionOracle


# noinspection PyDataclass
@ddt.ddt
class CollectionToEverythingElseErc721CollectionBasedConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock(EvmRpcClient)
        self.__max_block_height = HexInt(9999)
        self.__block_time_service = AsyncMock(BlockTimeService)
        self.__token_transaction_type_oracle = MagicMock(TokenTransactionTypeOracle)
        self.__log_version_oracle = MagicMock(LogVersionOracle)
        self.__log_version_oracle.version_from_log.return_value = HexInt(0)
        self.__data_service = AsyncMock(DataService)
        self.__consumer = CollectionToEverythingElseErc721CollectionBasedConsumer(
            self.__data_service,
            self.__rpc_client,
            self.__block_time_service,
            self.__token_transaction_type_oracle,
            self.__log_version_oracle,
            self.__max_block_height,
        )
        self.__data_package = CollectionDataPackage(MagicMock(Collection))  # type: ignore
        self.__data_package.collection.specification = EthereumCollectionType.ERC721  # type: ignore
        self.__data_package.collection.block_created = HexInt(100)  # type: ignore
        self.__data_package.collection.collection_id = Address("collection")  # type: ignore
        self.__data_package.collection.blockchain = MagicMock(BlockChain)  # type: ignore
        self.__data_package.collection.data_version = 19  # type: ignore

        self.__logs = [
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes("0x"),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_SINGLE.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000011"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                    HexBytes(encode(["uint256"], [0x13])),
                ],
                address=Address("contract"),
            ),
            EvmLog(
                removed=False,
                log_index=HexInt("0x20"),
                transaction_index=HexInt("0x21"),
                transaction_hash=HexBytes(b"thash2"),
                block_hash=HexBytes(b"bhash2"),
                block_number=HexInt("0x22"),
                data=HexBytes("0x"),
                topics=[
                    HexBytes(Erc721Events.TRANSFER.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000021"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000022"])),
                    HexBytes(encode(["uint256"], [0x13])),
                ],
                address=Address("contract"),
            ),
        ]
        self.__rpc_client.get_logs.return_value.__aiter__.return_value = self.__logs
        self.__rpc_client.call.return_value = ("Metadata URI",)

    async def test_does_nothing_with_non_collection_data_packages(self):
        await self.__consumer.receive(DataPackage())
        self.assertEqual([], self.__data_service.mock_calls, "Data Service should have 0 calls")
        self.assertEqual([], self.__rpc_client.mock_calls, "RPC Client should have 0 calls")

    async def test_does_nothing_if_not_erc721_specification(self):
        self.__data_package.collection.specification = EthereumCollectionType.ERC1155
        await self.__consumer.receive(self.__data_package)
        self.assertEqual([], self.__data_service.mock_calls, "Data Service should have 0 calls")
        self.assertEqual([], self.__rpc_client.mock_calls, "RPC Client should have 0 calls")

    async def test_gets_logs_from_collection_block_created_to_max_block_height(self):
        await self.__consumer.receive(self.__data_package)
        self.__rpc_client.get_logs.assert_called_once_with(
            from_block=self.__data_package.collection.block_created,
            to_block=self.__max_block_height,
            address=self.__data_package.collection.collection_id,
            topics=[Erc721Events.TRANSFER.event_signature_hash.hex()],
            starting_block_range_size=100_000,
        )

    async def test_sends_log_block_numbers_to_block_time_service(self):
        await self.__consumer.receive(self.__data_package)
        self.__block_time_service.get_block_timestamp.assert_has_awaits(
            (
                call(HexInt("0x12")),
                call(HexInt("0x22")),
            )
        )

    async def test_sends_log_to_log_version_oracle(self):
        await self.__consumer.receive(self.__data_package)
        self.__log_version_oracle.version_from_log.assert_has_calls(
            (call(log) for log in self.__logs)
        )

    async def test_writes_nft_token_transfers_with_data_service(self):
        expected = [
            TokenTransfer(
                blockchain=self.__data_package.collection.blockchain,
                data_version=self.__data_package.collection.data_version,
                collection_id=self.__data_package.collection.collection_id,
                collection_type=EthereumCollectionType.ERC721,
                token_id=HexInt("0x13"),
                timestamp=self.__block_time_service.get_block_timestamp.return_value,
                transaction_type=self.__token_transaction_type_oracle.type_from_log.return_value,
                # noqa: E501
                from_=Address("0x0000000000000000000000000000000000000011"),
                to_=Address("0x0000000000000000000000000000000000000012"),
                quantity=HexInt(1),
                block_id=HexInt("0x12"),
                transaction_hash=HexBytes(b"thash1"),
                transaction_index=HexInt("0x11"),
                log_index=HexInt("0x10"),
                attribute_version=self.__log_version_oracle.version_from_log.return_value,
            ),
            TokenTransfer(
                blockchain=self.__data_package.collection.blockchain,
                data_version=self.__data_package.collection.data_version,
                collection_id=self.__data_package.collection.collection_id,
                collection_type=EthereumCollectionType.ERC721,
                token_id=HexInt("0x13"),
                timestamp=self.__block_time_service.get_block_timestamp.return_value,
                transaction_type=self.__token_transaction_type_oracle.type_from_log.return_value,
                # noqa: E501
                from_=Address("0x0000000000000000000000000000000000000021"),
                to_=Address("0x0000000000000000000000000000000000000022"),
                quantity=HexInt(1),
                block_id=HexInt("0x22"),
                transaction_hash=HexBytes(b"thash2"),
                transaction_index=HexInt("0x21"),
                log_index=HexInt("0x20"),
                attribute_version=self.__log_version_oracle.version_from_log.return_value,
            ),
        ]
        await self.__consumer.receive(self.__data_package)
        self.__data_service.write_token_transfer_batch.assert_awaited_once_with(expected)

    async def test_ignores_erc20_transfer_logs(self):
        """
        Just in case any contract also emits ERC-20 events, we'll make sure we ignore
        them since they have the same event signature
        """
        for log in self.__logs:
            log.topics.pop()  # ERC-20 has no token ID topic

        await self.__consumer.receive(self.__data_package)
        self.assertEqual([], self.__data_service.mock_calls, "Data Service should have 0 calls")
        self.assertEqual(
            [],
            self.__log_version_oracle.mock_calls,
            "Log version oracle should have 0 interactions",
        )
        self.assertEqual(
            [],
            self.__block_time_service.mock_calls,
            "Block time service should have 0 interactions",
        )

    async def test_write_token_with_data_service_accounting_for_mint_and_transfers(self):
        self.__block_time_service.get_block_timestamp.return_value = HexInt(0x99)
        self.__token_transaction_type_oracle.type_from_log.side_effect = [
            TokenTransactionType.MINT,
            TokenTransactionType.TRANSFER,
        ]
        self.__rpc_client.call.return_value = ("Metadata URI",)
        self.__rpc_client.get_logs.return_value.__aiter__.return_value = [
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes("0x"),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_SINGLE.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000000"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                    HexBytes(encode(["uint256"], [0x13])),
                ],
                address=Address("contract"),
            ),
            EvmLog(
                removed=False,
                log_index=HexInt("0x20"),
                transaction_index=HexInt("0x21"),
                transaction_hash=HexBytes(b"thash2"),
                block_hash=HexBytes(b"bhash2"),
                block_number=HexInt("0x22"),
                data=HexBytes("0x"),
                topics=[
                    HexBytes(Erc721Events.TRANSFER.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000022"])),
                    HexBytes(encode(["uint256"], [0x13])),
                ],
                address=Address("contract"),
            ),
        ]
        expected = Token(
            blockchain=self.__data_package.collection.blockchain,
            collection_id=self.__data_package.collection.collection_id,
            token_id=HexInt(0x13),
            data_version=self.__data_package.collection.data_version,
            original_owner=Address("0x0000000000000000000000000000000000000012"),
            current_owner=Address("0x0000000000000000000000000000000000000022"),
            mint_block=HexInt(0x12),
            mint_date=HexInt(0x99),
            quantity=HexInt(1),
            attribute_version=self.__log_version_oracle.version_from_log.return_value,
            metadata_url="Metadata URI",
        )
        await self.__consumer.receive(self.__data_package)
        self.__data_service.write_token_batch.assert_called_once_with([expected])

    async def test_tries_to_get_token_uri_from_contract_when_tx_is_mint(self):
        self.__token_transaction_type_oracle.type_from_log.side_effect = [
            TokenTransactionType.MINT,
            TokenTransactionType.TRANSFER,
        ]
        await self.__consumer.receive(self.__data_package)
        self.__rpc_client.call.assert_has_awaits(
            [
                call(
                    EthCall(
                        from_=None,
                        to="collection",
                        function=Erc721MetadataFunctions.TOKEN_URI,
                        parameters=[0x13],
                        block=HexInt("0x12"),
                    )
                ),
            ]
        )

    @ddt.data(
        -32000,  # Infura catch-all
        3,  # Infura token does not exist
    )
    async def test_does_not_error_when_getting_token_uri_from_contract_and_not_supported(
        self, error_code
    ):
        def _call_side_effect(eth_call: EthCall) -> tuple:
            if eth_call.function == Erc165Functions.SUPPORTS_INTERFACE:
                return (True,)
            raise RpcServerError(None, None, error_code, None)

        self.__logs.pop()
        self.__token_transaction_type_oracle.type_from_log.return_value = TokenTransactionType.MINT
        self.__rpc_client.call.side_effect = _call_side_effect
        await self.__consumer.receive(self.__data_package)
        self.__rpc_client.call.assert_awaited()
        self.__data_service.write_token_batch.assert_awaited_once_with(
            [
                Token(
                    blockchain=ANY,
                    collection_id=ANY,
                    token_id=ANY,
                    data_version=ANY,
                    original_owner=ANY,
                    current_owner=ANY,
                    mint_block=ANY,
                    mint_date=ANY,
                    quantity=ANY,
                    attribute_version=ANY,
                    metadata_url=None,
                )
            ]
        )

    async def test_does_not_error_when_token_uri_response_from_contract_wont_decode(self):
        def _call_side_effect(eth_call: EthCall) -> tuple:
            if eth_call.function == Erc165Functions.SUPPORTS_INTERFACE:
                return (True,)
            raise RpcDecodeError

        self.__logs.pop()
        self.__token_transaction_type_oracle.type_from_log.return_value = TokenTransactionType.MINT
        self.__rpc_client.call.side_effect = _call_side_effect
        await self.__consumer.receive(self.__data_package)
        self.__rpc_client.call.assert_awaited()
        self.__data_service.write_token_batch.assert_awaited_once_with(
            [
                Token(
                    blockchain=ANY,
                    collection_id=ANY,
                    token_id=ANY,
                    data_version=ANY,
                    original_owner=ANY,
                    current_owner=ANY,
                    mint_block=ANY,
                    mint_date=ANY,
                    quantity=ANY,
                    attribute_version=ANY,
                    metadata_url=None,
                )
            ]
        )

    async def test_burn_transfers_to_burn_address_and_sets_quantity_to_0(self):
        self.__token_transaction_type_oracle.type_from_log.side_effect = [
            TokenTransactionType.MINT,
            TokenTransactionType.BURN,
        ]
        expected = Token(
            blockchain=self.__data_package.collection.blockchain,
            collection_id=self.__data_package.collection.collection_id,
            token_id=HexInt(0x13),
            data_version=self.__data_package.collection.data_version,
            original_owner=Address("0x0000000000000000000000000000000000000012"),
            current_owner=Address("0x0000000000000000000000000000000000000022"),
            mint_block=HexInt(0x12),
            mint_date=self.__block_time_service.get_block_timestamp.return_value,
            quantity=HexInt(0),
            attribute_version=self.__log_version_oracle.version_from_log.return_value,
            metadata_url="Metadata URI",
        )

        await self.__consumer.receive(self.__data_package)
        self.__data_service.write_token_batch.assert_awaited_once_with([expected])

    async def test_writes_token_owner_for_the_last_owner_of_the_token_regardless_of_process_order(
        self,
    ):
        self.__token_transaction_type_oracle.type_from_log.side_effect = [
            TokenTransactionType.MINT,
            TokenTransactionType.TRANSFER,
            TokenTransactionType.TRANSFER,
        ]
        self.__log_version_oracle.version_from_log.side_effect = (
            lambda log: log.block_number.hex_value
            + log.transaction_index.hex_value
            + log.log_index.hex_value
        )

        # Logs are out of order to simulate the async processing of the data processing
        # the logs outside the natural order
        self.__logs.clear()
        self.__logs.extend(
            [
                EvmLog(
                    removed=False,
                    log_index=HexInt("0x10"),
                    transaction_index=HexInt("0x11"),
                    transaction_hash=HexBytes(b"thash1"),
                    block_hash=HexBytes(b"bhash1"),
                    block_number=HexInt("0x12"),
                    data=HexBytes("0x"),
                    topics=[
                        HexBytes(Erc1155Events.TRANSFER_SINGLE.event_signature_hash),
                        HexBytes(
                            encode(["address"], ["0x0000000000000000000000000000000000000000"])
                        ),
                        HexBytes(
                            encode(["address"], ["0x0000000000000000000000000000000000000021"])
                        ),
                        HexBytes(encode(["uint256"], [0x13])),
                    ],
                    address=Address("contract"),
                ),
                EvmLog(
                    removed=False,
                    log_index=HexInt("0x30"),
                    transaction_index=HexInt("0x31"),
                    transaction_hash=HexBytes(b"thash2"),
                    block_hash=HexBytes(b"bhash2"),
                    block_number=HexInt("0x32"),
                    data=HexBytes("0x"),
                    topics=[
                        HexBytes(Erc721Events.TRANSFER.event_signature_hash),
                        HexBytes(
                            encode(["address"], ["0x0000000000000000000000000000000000000022"])
                        ),
                        HexBytes(
                            encode(["address"], ["0x0000000000000000000000000000000000000023"])
                        ),
                        HexBytes(encode(["uint256"], [0x13])),
                    ],
                    address=Address("contract"),
                ),
                EvmLog(
                    removed=False,
                    log_index=HexInt("0x20"),
                    transaction_index=HexInt("0x21"),
                    transaction_hash=HexBytes(b"thash2"),
                    block_hash=HexBytes(b"bhash2"),
                    block_number=HexInt("0x22"),
                    data=HexBytes("0x"),
                    topics=[
                        HexBytes(Erc721Events.TRANSFER.event_signature_hash),
                        HexBytes(
                            encode(["address"], ["0x0000000000000000000000000000000000000021"])
                        ),
                        HexBytes(
                            encode(["address"], ["0x0000000000000000000000000000000000000022"])
                        ),
                        HexBytes(encode(["uint256"], [0x13])),
                    ],
                    address=Address("contract"),
                ),
            ]
        )

        await self.__consumer.receive(self.__data_package)
        self.__data_service.write_token_owner_batch.assert_awaited_once_with(
            [
                TokenOwner(
                    blockchain=self.__data_package.collection.blockchain,
                    collection_id=self.__data_package.collection.collection_id,
                    token_id=HexInt(0x13),
                    account=Address("0x0000000000000000000000000000000000000023"),
                    quantity=HexInt(1),
                    data_version=self.__data_package.collection.data_version,
                )
            ]
        )

    async def test_deos_not_write_token_owner_record_for_burned_token(self):
        self.__token_transaction_type_oracle.type_from_log.side_effect = [
            TokenTransactionType.MINT,
            TokenTransactionType.BURN,
        ]
        await self.__consumer.receive(self.__data_package)
        self.__data_service.write_token_owner_batch.assert_not_called()


# noinspection PyDataclass
class CollectionToEverythingElseErc1155CollectionBasedConsumerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_service = AsyncMock(DataService)
        self.__rpc_client = AsyncMock(EvmRpcClient)
        self.__max_block_height = HexInt(9999)
        self.__block_time_service = AsyncMock(BlockTimeService)
        self.__token_transaction_type_oracle = MagicMock(TokenTransactionTypeOracle)
        self.__token_transaction_type_oracle.return_value = TokenTransactionType.MINT
        self.__log_version_oracle = MagicMock(LogVersionOracle)
        self.__log_version_oracle.version_from_log.return_value = HexInt(0)
        self.__consumer = CollectionToEverythingElseErc1155CollectionBasedConsumer(
            self.__data_service,
            self.__rpc_client,
            self.__block_time_service,
            self.__token_transaction_type_oracle,
            self.__log_version_oracle,
            self.__max_block_height,
        )
        self.__data_package = CollectionDataPackage(MagicMock(Collection))
        self.__data_package.collection.specification = (  # type: ignore
            EthereumCollectionType.ERC1155
        )
        self.__data_package.collection.block_created = HexInt(100)  # type: ignore
        self.__data_package.collection.collection_id = Address("collection")  # type: ignore
        self.__data_package.collection.blockchain = MagicMock(BlockChain)  # type: ignore
        self.__data_package.collection.data_version = 19  # type: ignore

        self.__logs = [
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes(encode(["uint256", "uint256"], [0x13, 0x1])),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_SINGLE.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000001"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000011"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                ],
                address=Address("contract"),
            ),
        ]
        self.__rpc_client.get_logs.return_value.__aiter__.return_value = self.__logs

    async def test_does_nothing_with_non_collection_data_packages(self):
        await self.__consumer.receive(DataPackage())
        self.assertEqual([], self.__data_service.mock_calls, "Data Service should have 0 calls")
        self.assertEqual([], self.__rpc_client.mock_calls, "RPC Client should have 0 calls")

    async def test_does_nothing_if_collection_is_not_erc1155_specification(self):
        self.__data_package.collection.specification = EthereumCollectionType.ERC721
        await self.__consumer.receive(self.__data_package)
        self.assertEqual([], self.__data_service.mock_calls, "Data Service should have 0 calls")
        self.assertEqual([], self.__rpc_client.mock_calls, "RPC Client should have 0 calls")

    async def test_gets_logs_from_collection_block_created_to_max_block_height(self):
        await self.__consumer.receive(self.__data_package)
        self.__rpc_client.get_logs.assert_called_once_with(
            from_block=self.__data_package.collection.block_created,
            to_block=self.__max_block_height,
            address=self.__data_package.collection.collection_id,
            topics=[
                [
                    Erc1155Events.TRANSFER_SINGLE.event_signature_hash.hex(),
                    Erc1155Events.TRANSFER_BATCH.event_signature_hash.hex(),
                    Erc1155Events.URI.event_signature_hash.hex(),
                ]
            ],
            starting_block_range_size=100_000,
        )

    async def test_sends_log_block_numbers_to_block_time_service(self):
        await self.__consumer.receive(self.__data_package)
        self.__block_time_service.get_block_timestamp.assert_awaited_once_with(HexInt("0x12"))

    async def test_sends_log_to_log_version_oracle(self):
        await self.__consumer.receive(self.__data_package)
        self.__log_version_oracle.version_from_log.assert_called_once_with(self.__logs[0])

    async def test_writes_nft_token_transfers_with_data_service_for_transfer_single(self):
        expected = TokenTransfer(
            blockchain=self.__data_package.collection.blockchain,
            data_version=self.__data_package.collection.data_version,
            collection_id=self.__data_package.collection.collection_id,
            collection_type=EthereumCollectionType.ERC1155,
            token_id=HexInt("0x13"),
            timestamp=self.__block_time_service.get_block_timestamp.return_value,
            transaction_type=self.__token_transaction_type_oracle.type_from_log.return_value,
            from_=Address("0x0000000000000000000000000000000000000011"),
            to_=Address("0x0000000000000000000000000000000000000012"),
            quantity=HexInt(1),
            block_id=HexInt("0x12"),
            transaction_hash=HexBytes(b"thash1"),
            transaction_index=HexInt("0x11"),
            log_index=HexInt("0x10"),
            attribute_version=self.__log_version_oracle.version_from_log.return_value,
        )
        await self.__consumer.receive(self.__data_package)
        self.__data_service.write_token_transfer_batch.assert_awaited_once_with([expected])

    async def test_writes_nft_token_transfers_with_data_service_for_transfer_batch(self):
        self.__rpc_client.get_logs.return_value.__aiter__.return_value = [
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes(encode(["uint256[]", "uint256[]"], [[0x13, 0x23], [0x1, 0x2]])),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_BATCH.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000001"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000011"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                ],
                address=Address("contract"),
            ),
        ]
        expected = [
            TokenTransfer(
                blockchain=self.__data_package.collection.blockchain,
                data_version=self.__data_package.collection.data_version,
                collection_type=EthereumCollectionType.ERC1155,
                collection_id=self.__data_package.collection.collection_id,
                token_id=HexInt("0x13"),
                timestamp=self.__block_time_service.get_block_timestamp.return_value,
                transaction_type=self.__token_transaction_type_oracle.type_from_log.return_value,
                from_=Address("0x0000000000000000000000000000000000000011"),
                to_=Address("0x0000000000000000000000000000000000000012"),
                quantity=HexInt(1),
                block_id=HexInt("0x12"),
                transaction_hash=HexBytes(b"thash1"),
                transaction_index=HexInt("0x11"),
                log_index=HexInt("0x10"),
                attribute_version=self.__log_version_oracle.version_from_log.return_value,
            ),
            TokenTransfer(
                blockchain=self.__data_package.collection.blockchain,
                data_version=self.__data_package.collection.data_version,
                collection_id=self.__data_package.collection.collection_id,
                collection_type=EthereumCollectionType.ERC1155,
                token_id=HexInt("0x23"),
                timestamp=self.__block_time_service.get_block_timestamp.return_value,
                transaction_type=self.__token_transaction_type_oracle.type_from_log.return_value,
                from_=Address("0x0000000000000000000000000000000000000011"),
                to_=Address("0x0000000000000000000000000000000000000012"),
                quantity=HexInt(2),
                block_id=HexInt("0x12"),
                transaction_hash=HexBytes(b"thash1"),
                transaction_index=HexInt("0x11"),
                log_index=HexInt("0x10"),
                attribute_version=self.__log_version_oracle.version_from_log.return_value,
            ),
        ]
        await self.__consumer.receive(self.__data_package)
        self.__data_service.write_token_transfer_batch.assert_awaited_once_with(expected)

    async def test_write_token_with_data_service_accounting_for_all_types(self):
        logs = [
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes(encode(["uint256[]", "uint256[]"], [[0x13, 0x14], [10, 100]])),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_BATCH.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000001"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000000"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                ],
                address=Address("contract"),
            ),
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes(encode(["uint256", "uint256"], [0x13, 1])),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_SINGLE.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000001"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000000"])),
                ],
                address=Address("contract"),
            ),
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes(encode(["uint256[]", "uint256[]"], [[0x13, 0x14], [9, 90]])),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_BATCH.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000001"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000000"])),
                ],
                address=Address("contract"),
            ),
        ]
        self.__rpc_client.get_logs.return_value.__aiter__.return_value = logs

        self.__block_time_service.get_block_timestamp.return_value = HexInt(0x99)
        self.__token_transaction_type_oracle.type_from_log.side_effect = [
            TokenTransactionType.MINT,
            TokenTransactionType.BURN,
            TokenTransactionType.BURN,
        ]
        expected = [
            Token(
                blockchain=self.__data_package.collection.blockchain,
                collection_id=self.__data_package.collection.collection_id,
                token_id=HexInt(0x13),
                data_version=self.__data_package.collection.data_version,
                original_owner=Address("0x0000000000000000000000000000000000000012"),
                current_owner=None,
                mint_block=HexInt(0x12),
                mint_date=HexInt(0x99),
                quantity=HexInt(0),
                attribute_version=self.__log_version_oracle.version_from_log.return_value,
                metadata_url=None,
            ),
            Token(
                blockchain=self.__data_package.collection.blockchain,
                collection_id=self.__data_package.collection.collection_id,
                token_id=HexInt(0x14),
                data_version=self.__data_package.collection.data_version,
                original_owner=Address("0x0000000000000000000000000000000000000012"),
                current_owner=None,
                mint_block=HexInt(0x12),
                mint_date=HexInt(0x99),
                quantity=HexInt(10),
                attribute_version=self.__log_version_oracle.version_from_log.return_value,
                metadata_url=None,
            ),
        ]
        await self.__consumer.receive(self.__data_package)
        self.__data_service.write_token_batch.assert_called_once_with(expected)

    async def test_updates_token_with_metadata_uri(self):
        self.__token_transaction_type_oracle.type_from_log.return_value = TokenTransactionType.MINT
        self.__logs.append(
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes(encode(["string"], ["Metadata URI"])),
                topics=[
                    HexBytes(Erc1155Events.URI.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000013"])),
                ],
                address=Address("contract"),
            )
        )

        expected = [
            Token(
                blockchain=ANY,
                collection_id=ANY,
                token_id=HexInt(0x13),
                data_version=ANY,
                original_owner=ANY,
                current_owner=ANY,
                mint_block=ANY,
                mint_date=ANY,
                quantity=ANY,
                attribute_version=self.__log_version_oracle.version_from_log.return_value,
                metadata_url="Metadata URI",
            ),
        ]

        await self.__consumer.receive(self.__data_package)
        self.__data_service.write_token_batch.assert_awaited_once_with(expected)

        await self.__consumer.receive(self.__data_package)

    async def test_writes_token_owner_records_accounting_for_transfers_and_burns(self):
        self.__rpc_client.get_logs.return_value.__aiter__.return_value = [
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes(encode(["uint256[]", "uint256[]"], [[0x13, 0x14], [20, 100]])),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_BATCH.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000001"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000000"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                ],
                address=Address("contract"),
            ),
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes(encode(["uint256", "uint256"], [0x13, 1])),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_SINGLE.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000001"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000013"])),
                ],
                address=Address("contract"),
            ),
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes(encode(["uint256[]", "uint256[]"], [[0x13, 0x14], [9, 90]])),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_BATCH.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000001"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000000"])),
                ],
                address=Address("contract"),
            ),
        ]

        self.__block_time_service.get_block_timestamp.return_value = HexInt(0x99)
        self.__token_transaction_type_oracle.type_from_log.side_effect = [
            TokenTransactionType.MINT,
            TokenTransactionType.TRANSFER,
            TokenTransactionType.BURN,
        ]
        expected = [
            TokenOwner(
                blockchain=self.__data_package.collection.blockchain,
                collection_id=self.__data_package.collection.collection_id,
                token_id=HexInt(0x13),
                account=Address("0x0000000000000000000000000000000000000012"),
                quantity=HexInt(10),
                data_version=self.__data_package.collection.data_version,
            ),
            TokenOwner(
                blockchain=self.__data_package.collection.blockchain,
                collection_id=self.__data_package.collection.collection_id,
                token_id=HexInt(0x13),
                account=Address("0x0000000000000000000000000000000000000013"),
                quantity=HexInt(1),
                data_version=self.__data_package.collection.data_version,
            ),
            TokenOwner(
                blockchain=self.__data_package.collection.blockchain,
                collection_id=self.__data_package.collection.collection_id,
                token_id=HexInt(0x14),
                account=Address("0x0000000000000000000000000000000000000012"),
                quantity=HexInt(10),
                data_version=self.__data_package.collection.data_version,
            ),
        ]

        await self.__consumer.receive(self.__data_package)
        self.__data_service.write_token_owner_batch.assert_awaited_once_with(expected)

    async def test_writes_token_owner_records_properly_for_burn_as_first_transfer(self):
        self.__rpc_client.get_logs.return_value.__aiter__.return_value = [
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes(encode(["uint256[]", "uint256[]"], [[0x13], [10]])),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_BATCH.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000001"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000000"])),
                ],
                address=Address("contract"),
            ),
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes(b"thash1"),
                block_hash=HexBytes(b"bhash1"),
                block_number=HexInt("0x12"),
                data=HexBytes(encode(["uint256[]", "uint256[]"], [[0x13], [20]])),
                topics=[
                    HexBytes(Erc1155Events.TRANSFER_BATCH.event_signature_hash),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000001"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000000"])),
                    HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000012"])),
                ],
                address=Address("contract"),
            ),
        ]

        self.__block_time_service.get_block_timestamp.return_value = HexInt(0x99)
        self.__token_transaction_type_oracle.type_from_log.side_effect = [
            TokenTransactionType.BURN,
            TokenTransactionType.MINT,
        ]
        expected = [
            TokenOwner(
                blockchain=self.__data_package.collection.blockchain,
                collection_id=self.__data_package.collection.collection_id,
                token_id=HexInt(0x13),
                account=Address("0x0000000000000000000000000000000000000012"),
                quantity=HexInt(10),
                data_version=self.__data_package.collection.data_version,
            ),
        ]

        await self.__consumer.receive(self.__data_package)
        self.__data_service.write_token_owner_batch.assert_awaited_once_with(expected)
        self.assertIsInstance(
            self.__data_service.write_token_owner_batch.call_args.args[0][0].quantity,
            HexInt,
            "TokenOwner.quantity must be a HexInt",
        )
