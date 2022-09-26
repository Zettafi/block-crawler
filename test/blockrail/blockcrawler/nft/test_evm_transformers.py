from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, Mock, call, ANY

import ddt
from eth_abi import encode
from hexbytes import HexBytes

from blockrail.blockcrawler.core.bus import DataBus, DataPackage
from blockrail.blockcrawler.core.entities import HexInt, BlockChain
from blockrail.blockcrawler.core.rpc import RPCServerError
from blockrail.blockcrawler.evm.data_packages import EvmLogDataPackage
from blockrail.blockcrawler.evm.data_packages import (
    EvmTransactionReceiptDataPackage,
)
from blockrail.blockcrawler.evm.rpc import EVMRPCClient, EthCall
from blockrail.blockcrawler.evm.types import (
    EVMBlock,
    Address,
    EVMTransactionReceipt,
    ERC165InterfaceID,
    EVMLog,
)
from blockrail.blockcrawler.evm.util import (
    ERC165Functions,
    ERC721MetadataFunctions,
    ERC721EnumerableFunctions,
    AdditionalFunctions,
    ERC721Events,
    ERC1155Events,
)
from blockrail.blockcrawler.nft.data_packages import (
    NftTokenTransferDataPackage,
    NftTokenMetadataUriUpdatedDataPackage,
    NftCollectionDataPackage,
)
from blockrail.blockcrawler.nft.entities import (
    EthereumCollectionType,
    TokenTransfer,
    TokenTransactionType,
    Collection,
)
from blockrail.blockcrawler.nft.evm.transformers import (
    EvmTransactionReceiptToNftCollectionTransformer,
    EvmLogErc721TransferToNftTokenTransferTransformer,
    TokenTransactionTypeOracle,
    EvmLogErc1155TransferSingleToNftTokenTransferTransformer,
    EvmLogErc1155TransferToNftTokenTransferTransformer,
    EvmLogErc1155UriEventToNftTokenMetadataUriUpdatedTransformer,
    EvmLogErc721TransferToNftTokenMetadataUriUpdatedTransformer,
)
from blockrail.blockcrawler.nft.evm import LogVersionOracle


class EvmTransactionReceiptToNftCollectionTransformerTestCase(IsolatedAsyncioTestCase):
    def __call_side_effect(self, request: EthCall):
        if request.function.function_signature_hash in self.__call_function_results:
            result = self.__call_function_results[request.function.function_signature_hash]
            if isinstance(result, dict):
                if request.parameters[0] in result:
                    result = result[request.parameters[0]]
                else:
                    raise RPCServerError(None, None, None, None)
        else:
            raise RPCServerError(None, None, None, None)

        return result

    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock(EVMRPCClient)
        self.__rpc_client.call.side_effect = self.__call_side_effect
        self.__data_bus = AsyncMock(DataBus)
        self.__blockchain = Mock(BlockChain)
        self.__block_timestamp = 12345
        self.__data_version = 999
        self.__transformer = EvmTransactionReceiptToNftCollectionTransformer(
            self.__data_bus, self.__blockchain, self.__rpc_client, self.__data_version
        )
        transaction_receipt = EVMTransactionReceipt(
            transaction_hash=HexBytes(b""),
            transaction_index=HexInt("0x0"),
            block_number=HexInt("0x0"),
            block_hash=HexBytes("0x0"),
            from_=Address("0x99"),
            to_=Address(""),
            cumulative_gas_used=HexInt("0x0"),
            gas_used=HexInt("0x0"),
            contract_address=Address("contract address"),
            logs=list(),
            logs_bloom=HexInt("0x0"),
            root=HexBytes(b""),
            status=HexInt("0x0"),
        )
        block = EVMBlock(
            number=HexInt("0x1"),
            hash=HexBytes("0x0"),
            parent_hash=HexBytes("0x0"),
            nonce=HexBytes("0x0"),
            sha3_uncles=HexBytes("0x0"),
            logs_bloom=HexInt("0x0"),
            transactions_root=HexBytes("0x0"),
            state_root=HexBytes("0x0"),
            receipts_root=HexBytes("0x0"),
            miner=Address("miner"),
            mix_hash=HexBytes("0x0"),
            difficulty=HexInt("0x0"),
            total_difficulty=HexInt("0x0"),
            extra_data=HexBytes("0x0"),
            size=HexInt("0x0"),
            gas_limit=HexInt("0x0"),
            gas_used=HexInt("0x0"),
            timestamp=HexInt(hex(self.__block_timestamp)),
            transactions=list(),
            uncles=list(),
        )
        self.__default_data_package = EvmTransactionReceiptDataPackage(
            self.__blockchain, transaction_receipt, block
        )
        self.__call_function_results: dict = dict()

    def __get_data_package_sent_to_data_bus(self):
        (data_package,) = self.__data_bus.send.call_args.args
        return data_package

    async def test_gets_contract_data(self):
        await self.__transformer.receive(self.__default_data_package)
        contract_address = self.__default_data_package.transaction_receipt.contract_address
        self.__rpc_client.call.assert_has_calls(
            [
                call(
                    EthCall(
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC721.bytes],
                    )
                ),
                call(
                    EthCall(
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC721_METADATA.bytes],
                    )
                ),
                call(
                    EthCall(
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC721_ENUMERABLE.bytes],
                    )
                ),
                call(
                    EthCall(
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC1155.bytes],
                    )
                ),
                call(
                    EthCall(
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC1155_METADATA_URI.bytes],
                    )
                ),
                call(
                    EthCall(
                        None,
                        self.__default_data_package.transaction_receipt.contract_address,
                        ERC721MetadataFunctions.SYMBOL,
                    )
                ),
                call(
                    EthCall(
                        None,
                        self.__default_data_package.transaction_receipt.contract_address,
                        ERC721MetadataFunctions.NAME,
                    )
                ),
                call(
                    EthCall(
                        None,
                        self.__default_data_package.transaction_receipt.contract_address,
                        ERC721EnumerableFunctions.TOTAL_SUPPLY,
                    )
                ),
                call(
                    EthCall(
                        None,
                        self.__default_data_package.transaction_receipt.contract_address,
                        AdditionalFunctions.OWNER,
                    )
                ),
            ]
        )

    async def test_sets_specification_to_ERC721_when_ERC_721_supported(self):
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (True,),
            ERC165InterfaceID.ERC1155.bytes: (False,),
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=ANY,
                    block_created=ANY,
                    name=ANY,
                    symbol=ANY,
                    specification=EthereumCollectionType.ERC721,
                    date_created=ANY,
                    total_supply=ANY,
                    data_version=ANY,
                )
            )
        )

    async def test_sets_specification_to_ERC1155_when_ERC_1155_supported(self):
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (False,),
            ERC165InterfaceID.ERC1155.bytes: (True,),
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=ANY,
                    block_created=ANY,
                    name=ANY,
                    symbol=ANY,
                    specification=EthereumCollectionType.ERC1155,
                    date_created=ANY,
                    total_supply=ANY,
                    data_version=ANY,
                )
            )
        )

    async def test_sets_specification_to_ERC721_when_ERC_721_AND_ERC_1155_supported(self):
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (True,),
            ERC165InterfaceID.ERC1155.bytes: (True,),
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=ANY,
                    block_created=ANY,
                    name=ANY,
                    symbol=ANY,
                    specification=EthereumCollectionType.ERC721,
                    date_created=ANY,
                    total_supply=ANY,
                    data_version=ANY,
                )
            )
        )

    async def test_does_not_send_results_to_bus_when_neither_ERC_721_not_ERC_1155_supported(self):
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_not_called()

    async def test_sets_data_version(self):
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (True,)
        }
        expected = 999
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=ANY,
                    block_created=ANY,
                    name=ANY,
                    symbol=ANY,
                    specification=ANY,
                    date_created=ANY,
                    total_supply=ANY,
                    data_version=expected,
                )
            )
        )

    async def test_sets_name_when_not_error(self):
        expected = "Expected Name"
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (True,)
        }
        self.__call_function_results[ERC721MetadataFunctions.NAME.function_signature_hash] = (
            expected[:],
        )
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=ANY,
                    block_created=ANY,
                    name=expected,
                    symbol=ANY,
                    specification=ANY,
                    date_created=ANY,
                    total_supply=ANY,
                    data_version=ANY,
                )
            )
        )

    async def test_sets_none_for_name_when_error(self):
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (True,)
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=ANY,
                    block_created=ANY,
                    name=None,
                    symbol=ANY,
                    specification=ANY,
                    date_created=ANY,
                    total_supply=ANY,
                    data_version=ANY,
                )
            )
        )

    async def test_sets_symbol_when_not_error(self):
        expected = "Expected Symbol"
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (True,)
        }
        self.__call_function_results[ERC721MetadataFunctions.SYMBOL.function_signature_hash] = (
            expected[:],
        )
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=ANY,
                    block_created=ANY,
                    name=ANY,
                    symbol=expected,
                    specification=ANY,
                    date_created=ANY,
                    total_supply=ANY,
                    data_version=ANY,
                )
            )
        )

    async def test_sets_none_for_symbol_when_error(self):
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (True,)
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=ANY,
                    block_created=ANY,
                    name=ANY,
                    symbol=None,
                    specification=ANY,
                    date_created=ANY,
                    total_supply=ANY,
                    data_version=ANY,
                )
            )
        )

    async def test_sets_total_supply_when_not_error(self):
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (True,)
        }
        self.__call_function_results[
            ERC721EnumerableFunctions.TOTAL_SUPPLY.function_signature_hash
        ] = (1,)
        expected = HexInt("0x1")
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=ANY,
                    block_created=ANY,
                    name=ANY,
                    symbol=ANY,
                    specification=ANY,
                    date_created=ANY,
                    total_supply=expected,
                    data_version=ANY,
                )
            )
        )

    async def test_sets_none_for_total_supply_when_error(self):
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (True,)
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=ANY,
                    block_created=ANY,
                    name=ANY,
                    symbol=ANY,
                    specification=ANY,
                    date_created=ANY,
                    total_supply=None,
                    data_version=ANY,
                )
            )
        )

    async def test_sets_owner_when_not_error(self):
        expected = Address("expected owner")
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (True,)
        }
        self.__call_function_results[AdditionalFunctions.OWNER.function_signature_hash] = (
            expected[:],
        )
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=expected,
                    block_created=ANY,
                    name=ANY,
                    symbol=ANY,
                    specification=ANY,
                    date_created=ANY,
                    total_supply=ANY,
                    data_version=ANY,
                )
            )
        )

    async def test_sets_none_for_owner_when_error(self):
        self.__call_function_results[ERC165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            ERC165InterfaceID.ERC721.bytes: (True,)
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            NftCollectionDataPackage(
                Collection(
                    blockchain=ANY,
                    collection_id=ANY,
                    creator=ANY,
                    owner=None,
                    block_created=ANY,
                    name=ANY,
                    symbol=ANY,
                    specification=ANY,
                    date_created=ANY,
                    total_supply=ANY,
                    data_version=ANY,
                )
            )
        )


@ddt.ddt
class TransactionTypeOracleTestCase(TestCase):
    @ddt.data(
        # 0 address to non-contract address is mint
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000001"),
            TokenTransactionType.MINT,
        ),
        # Contract address to non-contract address is mint
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000001"),
            TokenTransactionType.MINT,
        ),
        # Contract address to 0 address is burn
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            TokenTransactionType.BURN,
        ),
        # 0 address to 0 address is burn
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            TokenTransactionType.BURN,
        ),
        # Con-contract address to 0 address is burn
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000001"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            TokenTransactionType.BURN,
        ),
        # Contract address to contract address is transfer
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            TokenTransactionType.TRANSFER,
        ),
        # 0 address to contract address is transfer
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            TokenTransactionType.TRANSFER,
        ),
        # Non-contract address to Non-contract address is transfer
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000001"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000002"),
            TokenTransactionType.TRANSFER,
        ),
        # Non-contract address to contract address is transfer
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000001"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            TokenTransactionType.TRANSFER,
        ),
    )
    @ddt.unpack
    def test_derive_type_derives_correct_type(self, from_, to_, expected_type):
        log = Mock(EVMLog)
        log.address = "0x0000000000000000000000000000000000000099"
        log.topics = ["Ignore this topic", from_, to_]
        actual = TokenTransactionTypeOracle().derive_type(log)
        self.assertEqual(expected_type, actual)


class EvmLogErc721TransferToNftTokenTransferTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__data_version = 123
        self.__transaction_type_oracle = Mock(TokenTransactionTypeOracle)
        self.__transaction_type = Mock(TokenTransactionType)
        self.__transaction_type_oracle.derive_type.return_value = self.__transaction_type
        self.__log_version_oracle = Mock(LogVersionOracle)
        self.__log_version_oracle.version_from_log.return_value = HexInt(1234)
        self.__transformer = EvmLogErc721TransferToNftTokenTransferTransformer(
            self.__data_bus,
            self.__data_version,
            self.__transaction_type_oracle,
            self.__log_version_oracle,
        )

    async def test_does_nothing_with_non_evm_log_data_packages(self):
        await self.__transformer.receive(DataPackage())
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_empty_logs(self):
        log = Mock(EVMLog)
        log.topics = []
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_non_erc_721_transfer_logs(self):
        log = Mock(EVMLog)
        log.topics = ["0x1234567890abcdef1234567890abcdef"]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_transforms_erc721_transfer_log_to_nft_token_transfer(self):
        blockchain = Mock(BlockChain)
        log = Mock(EVMLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(100)
        log.transaction_hash = HexBytes("0x9901")
        log.transaction_index = HexInt(200)
        log.log_index = HexInt(300)
        transaction_receipt = Mock(EVMTransactionReceipt)
        block = Mock(EVMBlock)
        block.timestamp = HexInt(400)
        log.topics = [
            ERC721Events.TRANSFER.event_signature_hash,
            HexBytes(encode(["address"], ["0x8888888888888888888888888888888888888888"])),
            HexBytes(encode(["address"], ["0x9999999999999999999999999999999999999999"])),
            HexBytes("0x9dd72ab9d37c710742a330533979503a840692689b3144a6f2e5ff59af12b41e"),
        ]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=blockchain,
                log=log,
                transaction_receipt=transaction_receipt,
                block=block,
            )
        )
        self.__data_bus.send.assert_awaited_once_with(
            NftTokenTransferDataPackage(
                token_transfer=TokenTransfer(
                    blockchain=blockchain,
                    timestamp=HexInt(400),
                    collection_id=Address("Collection ID"),
                    from_=Address("0x8888888888888888888888888888888888888888"),
                    to_=Address("0x9999999999999999999999999999999999999999"),
                    data_version=self.__data_version,
                    token_id=HexInt(
                        "0x9dd72ab9d37c710742a330533979503a840692689b3144a6f2e5ff59af12b41e"
                    ),
                    block_id=HexInt(100),
                    transaction_hash=HexBytes("0x9901"),
                    transaction_index=HexInt(200),
                    log_index=HexInt(300),
                    quantity=HexInt(1),
                    transaction_type=self.__transaction_type,
                    attribute_version=HexInt(1234),
                )
            )
        )

    async def test_does_not_transform_erc20_transfer_log(self):
        log = Mock(EVMLog)
        log.topics = [
            ERC721Events.TRANSFER.event_signature_hash,
            encode(["address"], ["0x8888888888888888888888888888888888888888"]),
            encode(["address"], ["0x9999999999999999999999999999999999999999"]),
            # ERC-20 only indexes _from and _to
        ]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()


class EvmLogErc1155TransferSingleToNftTokenTransferTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__data_version = 123
        self.__transaction_type_oracle = Mock(TokenTransactionTypeOracle)
        self.__transaction_type = Mock(TokenTransactionType)
        self.__transaction_type_oracle.derive_type.return_value = self.__transaction_type
        self.__log_version_oracle = Mock(LogVersionOracle)
        self.__log_version_oracle.version_from_log.return_value = HexInt(1234)
        self.__transformer = EvmLogErc1155TransferSingleToNftTokenTransferTransformer(
            self.__data_bus,
            self.__data_version,
            self.__transaction_type_oracle,
            self.__log_version_oracle,
        )

    async def test_does_nothing_with_non_evm_log_data_packages(self):
        await self.__transformer.receive(DataPackage())
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_empty_logs(self):
        log = Mock(EVMLog)
        log.topics = []
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_non_erc_1155_transfer_single_logs(self):
        log = Mock(EVMLog)
        log.topics = ["0x1234567890abcdef1234567890abcdef"]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_transforms_erc1155_transfer_log_to_nft_token_transfer(self):
        blockchain = Mock(BlockChain)
        log = Mock(EVMLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(100)
        log.transaction_hash = HexBytes("0x9901")
        log.transaction_index = HexInt(200)
        log.log_index = HexInt(300)
        transaction_receipt = Mock(EVMTransactionReceipt)
        block = Mock(EVMBlock)
        block.timestamp = HexInt(400)
        log.topics = [
            ERC1155Events.TRANSFER_SINGLE.event_signature_hash,
            encode(["address"], ["0x8888888888888888888888888888888888888888"]),
            encode(["address"], ["0x9999999999999999999999999999999999999999"]),
        ]
        log.data = encode(["uint256", "uint256"], [1234, 99])

        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=blockchain,
                log=log,
                transaction_receipt=transaction_receipt,
                block=block,
            )
        )
        self.__data_bus.send.assert_awaited_once_with(
            NftTokenTransferDataPackage(
                token_transfer=TokenTransfer(
                    blockchain=blockchain,
                    timestamp=HexInt(400),
                    collection_id=Address("Collection ID"),
                    from_=Address("0x8888888888888888888888888888888888888888"),
                    to_=Address("0x9999999999999999999999999999999999999999"),
                    data_version=self.__data_version,
                    token_id=HexInt(1234),
                    block_id=HexInt(100),
                    transaction_hash=HexBytes("0x9901"),
                    transaction_index=HexInt(200),
                    log_index=HexInt(300),
                    quantity=HexInt(99),
                    transaction_type=self.__transaction_type,
                    attribute_version=HexInt(1234),
                )
            )
        )


class EvmLogErc1155TransferToNftTokenTransferTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__data_version = 123
        self.__transaction_type_oracle = Mock(TokenTransactionTypeOracle)
        self.__transaction_type = Mock(TokenTransactionType)
        self.__transaction_type_oracle.derive_type.return_value = self.__transaction_type
        self.__log_version_oracle = Mock(LogVersionOracle)
        self.__log_version_oracle.version_from_log.return_value = HexInt(1234)
        self.__transformer = EvmLogErc1155TransferToNftTokenTransferTransformer(
            self.__data_bus,
            self.__data_version,
            self.__transaction_type_oracle,
            self.__log_version_oracle,
        )

    async def test_does_nothing_with_non_evm_log_data_packages(self):
        await self.__transformer.receive(DataPackage())
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_empty_logs(self):
        log = Mock(EVMLog)
        log.topics = []
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_non_erc_1155_transfer_single_logs(self):
        log = Mock(EVMLog)
        log.topics = ["0x1234567890abcdef1234567890abcdef"]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_transforms_erc1155_transfer_log_to_nft_token_transfers(self):
        blockchain = Mock(BlockChain)
        log = Mock(EVMLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(100)
        log.transaction_hash = HexBytes("0x9901")
        log.transaction_index = HexInt(200)
        log.log_index = HexInt(300)
        transaction_receipt = Mock(EVMTransactionReceipt)
        block = Mock(EVMBlock)
        block.timestamp = HexInt(400)
        log.topics = [
            ERC1155Events.TRANSFER_BATCH.event_signature_hash,
            encode(["address"], ["0x8888888888888888888888888888888888888888"]),
            encode(["address"], ["0x9999999999999999999999999999999999999999"]),
        ]
        log.data = encode(["uint256[]", "uint256[]"], [[1234, 1235], [98, 99]])

        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=blockchain,
                log=log,
                transaction_receipt=transaction_receipt,
                block=block,
            )
        )
        self.__data_bus.send.assert_has_awaits(
            [
                call(
                    NftTokenTransferDataPackage(
                        token_transfer=TokenTransfer(
                            blockchain=blockchain,
                            timestamp=HexInt(400),
                            collection_id=Address("Collection ID"),
                            from_=Address("0x8888888888888888888888888888888888888888"),
                            to_=Address("0x9999999999999999999999999999999999999999"),
                            data_version=self.__data_version,
                            token_id=HexInt(1234),
                            block_id=HexInt(100),
                            transaction_hash=HexBytes("0x9901"),
                            transaction_index=HexInt(200),
                            log_index=HexInt(300),
                            quantity=HexInt(98),
                            transaction_type=self.__transaction_type,
                            attribute_version=HexInt(1234),
                        )
                    )
                ),
                call(
                    NftTokenTransferDataPackage(
                        token_transfer=TokenTransfer(
                            blockchain=blockchain,
                            timestamp=HexInt(400),
                            collection_id=Address("Collection ID"),
                            from_=Address("0x8888888888888888888888888888888888888888"),
                            to_=Address("0x9999999999999999999999999999999999999999"),
                            data_version=self.__data_version,
                            token_id=HexInt(1235),
                            block_id=HexInt(100),
                            transaction_hash=HexBytes("0x9901"),
                            transaction_index=HexInt(200),
                            log_index=HexInt(300),
                            quantity=HexInt(99),
                            transaction_type=self.__transaction_type,
                            attribute_version=HexInt(1234),
                        )
                    )
                ),
            ],
            any_order=True,
        )


class EvmLogErc1155UriEventToNftTokenUriUpdatedTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__data_version = 123
        self.__log_version_oracle = Mock(LogVersionOracle)
        self.__log_version_oracle.version_from_log.return_value = HexInt(1234)
        self.__transformer = EvmLogErc1155UriEventToNftTokenMetadataUriUpdatedTransformer(
            self.__data_bus,
            self.__log_version_oracle,
            self.__data_version,
        )

    async def test_does_nothing_with_non_evm_log_data_packages(self):
        await self.__transformer.receive(DataPackage())
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_empty_logs(self):
        log = Mock(EVMLog)
        log.topics = []
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_non_erc_1155_transfer_single_logs(self):
        log = Mock(EVMLog)
        log.topics = ["0x1234567890abcdef1234567890abcdef"]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_places_toke_uri_update_data_package_on_bus(self):
        blockchain = Mock(BlockChain)
        log = Mock(EVMLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(99_888_777)
        log.transaction_index = HexInt(234)
        log.log_index = HexInt(12)
        log.topics = [
            ERC1155Events.URI.event_signature_hash,
            encode(["uint256"], [12345]),
        ]
        log.data = encode(["string"], ["http://metadata.uri"])
        transaction_receipt = Mock(EVMTransactionReceipt)
        block = Mock(EVMBlock)

        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=blockchain,
                log=log,
                transaction_receipt=transaction_receipt,
                block=block,
            )
        )
        self.__data_bus.send.assert_awaited_once_with(
            NftTokenMetadataUriUpdatedDataPackage(
                blockchain=blockchain,
                collection_id=Address("Collection ID"),
                token_id=HexInt(12345),
                metadata_uri="http://metadata.uri",
                metadata_uri_version=HexInt(1234),
                data_version=self.__data_version,
            )
        )

    async def test_supports_erc1155_id_substitution(self):
        blockchain = Mock(BlockChain)
        log = Mock(EVMLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(99_888_777)
        log.transaction_index = HexInt(234)
        log.log_index = HexInt(12)
        log.topics = [
            ERC1155Events.URI.event_signature_hash,
            encode(["uint256"], [12345]),
        ]
        log.data = encode(["string"], ["http://metadata.uri/{id}.json"])
        transaction_receipt = Mock(EVMTransactionReceipt)
        block = Mock(EVMBlock)

        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=blockchain,
                log=log,
                transaction_receipt=transaction_receipt,
                block=block,
            )
        )
        self.__data_bus.send.assert_awaited_once_with(
            NftTokenMetadataUriUpdatedDataPackage(
                blockchain=blockchain,
                collection_id=Address("Collection ID"),
                token_id=HexInt(12345),
                metadata_uri="http://metadata.uri/12345.json",
                metadata_uri_version=HexInt(1234),
                data_version=self.__data_version,
            )
        )


class LogVersionOracleTestCase(TestCase):
    def test_returns_correct_version(self):
        log = Mock(EVMLog)
        log.block_number = HexInt(99_888_777)
        log.transaction_index = HexInt(234)
        log.log_index = HexInt(12)
        expected = HexInt("0x0000000000000000000000000162e0503e45ceac")
        actual = LogVersionOracle().version_from_log(log)
        self.assertEqual(expected, actual)


class EvmLogErc721TransferToNftTokenMetadataUriUpdatedTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__rpc_client = AsyncMock(EVMRPCClient)
        self.__rpc_client.call.side_effect = RPCServerError(None, None, None, None)
        self.__data_version = 123
        self.__log_version_oracle = Mock(LogVersionOracle)
        self.__log_version_oracle.version_from_log.return_value = HexInt(1234)
        self.__transformer = EvmLogErc721TransferToNftTokenMetadataUriUpdatedTransformer(
            self.__data_bus,
            self.__rpc_client,
            self.__log_version_oracle,
            self.__data_version,
        )

    async def test_does_nothing_with_non_evm_log_data_packages(self):
        await self.__transformer.receive(DataPackage())
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_empty_logs(self):
        log = Mock(EVMLog)
        log.topics = []
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_erc_20_transfer_logs(self):
        log = Mock(EVMLog)
        log.topics = [
            HexBytes(ERC721Events.TRANSFER.event_signature_hash),
            HexBytes(encode(["address"], ["0x8888888888888888888888888888888888888888"])),
            HexBytes(encode(["address"], ["0x8888888888888888888888888888888888888888"])),
        ]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_non_erc_721_transfer_mint_type_logs(self):
        log = Mock(EVMLog)
        log.topics = [
            HexBytes(ERC721Events.TRANSFER.event_signature_hash),
            HexBytes(encode(["address"], ["0x8888888888888888888888888888888888888888"])),
            HexBytes(encode(["address"], ["0x8888888888888888888888888888888888888888"])),
            HexBytes(encode(["uint256"], [1])),
        ]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EVMTransactionReceipt),
                block=Mock(EVMBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_places_token_uri_update_data_package_on_bus(self):
        blockchain = Mock(BlockChain)
        log = Mock(EVMLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(99_888_777)
        log.transaction_index = HexInt(234)
        log.log_index = HexInt(12)
        log.topics = [
            ERC721Events.TRANSFER.event_signature_hash,
            HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000000"])),
            HexBytes(encode(["address"], ["0x9999999999999999999999999999999999999999"])),
            HexBytes(encode(["uint256"], [54321])),
        ]
        transaction_receipt = Mock(EVMTransactionReceipt)
        block = Mock(EVMBlock)
        self.__rpc_client.call.side_effect = None
        self.__rpc_client.call.return_value = ("http://metadata.uri",)

        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=blockchain,
                log=log,
                transaction_receipt=transaction_receipt,
                block=block,
            )
        )
        self.__data_bus.send.assert_awaited_once_with(
            NftTokenMetadataUriUpdatedDataPackage(
                blockchain=blockchain,
                collection_id=Address("Collection ID"),
                token_id=HexInt(54321),
                metadata_uri="http://metadata.uri",
                metadata_uri_version=HexInt(1234),
                data_version=self.__data_version,
            )
        )

    async def test_sends_correct_data_to_RPC_API_to_get_URI(self):
        blockchain = Mock(BlockChain)
        log = Mock(EVMLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(99_888_777)
        log.transaction_index = HexInt(234)
        log.log_index = HexInt(12)
        token_id = 54321
        log.topics = [
            ERC721Events.TRANSFER.event_signature_hash,
            HexBytes(encode(["address"], ["0x8888888888888888888888888888888888888888"])),
            HexBytes(encode(["address"], ["0x9999999999999999999999999999999999999999"])),
            HexBytes(encode(["uint256"], [token_id])),
        ]
        transaction_receipt = Mock(EVMTransactionReceipt)
        block = Mock(EVMBlock)

        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=blockchain,
                log=log,
                transaction_receipt=transaction_receipt,
                block=block,
            )
        )
        HexInt("0x9dd72ab9d37c710742a330533979503a840692689b3144a6f2e5ff59af12b41e")
        self.__rpc_client.call.assert_awaited_once_with(
            EthCall(
                from_=None,
                to="Collection ID",
                function=ERC721MetadataFunctions.TOKEN_URI,
                parameters=[token_id],
            )
        )
