from typing import Dict, Union
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, call, ANY, MagicMock, patch

import ddt
from eth_abi import encode
from eth_abi.exceptions import NonEmptyPaddingBytes
from hexbytes import HexBytes

from blockcrawler.core.bus import DataBus, DataPackage, ConsumerError
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.rpc import RpcServerError
from blockcrawler.evm.data_packages import EvmLogDataPackage
from blockcrawler.evm.data_packages import (
    EvmTransactionReceiptDataPackage,
)
from blockcrawler.evm.rpc import EvmRpcClient, EthCall
from blockcrawler.evm.types import (
    EvmBlock,
    EvmTransactionReceipt,
    Erc165InterfaceID,
    EvmLog,
    Erc165Functions,
    Erc721MetadataFunctions,
    Erc721EnumerableFunctions,
    AdditionalFunctions,
    Erc721Events,
    Erc1155Events,
)
from blockcrawler.core.types import Address, HexInt
from blockcrawler.nft.data_packages import (
    TokenTransferDataPackage,
    TokenMetadataUriUpdatedDataPackage,
    CollectionDataPackage,
)
from blockcrawler.nft.entities import (
    EthereumCollectionType,
    TokenTransfer,
    TokenTransactionType,
    Collection,
)
from blockcrawler.nft.evm.oracles import LogVersionOracle, TokenTransactionTypeOracle
from blockcrawler.nft.evm.transformers import (
    EvmTransactionReceiptToNftCollectionTransformer,
    EvmLogErc721TransferToNftTokenTransferTransformer,
    EvmLogErc1155TransferSingleToNftTokenTransferTransformer,
    EvmLogErc1155TransferToNftTokenTransferTransformer,
    EvmLogErc1155UriEventToNftTokenMetadataUriUpdatedTransformer,
    Erc721TokenTransferToNftTokenMetadataUriUpdatedTransformer,
)


@ddt.ddt
class EvmTransactionReceiptToNftCollectionTransformerTestCase(IsolatedAsyncioTestCase):
    def __call_side_effect(self, request: EthCall):
        if request.function.function_signature_hash in self.__call_function_results:
            result = self.__call_function_results[request.function.function_signature_hash]
            if isinstance(result, dict):
                if request.parameters[0] in result:
                    result = result[request.parameters[0]]
                else:
                    raise RpcServerError(None, None, None, None)
            if isinstance(result, Exception):
                raise result
        else:
            raise RpcServerError(None, None, None, None)

        return result

    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock(EvmRpcClient)
        self.__rpc_client.call.side_effect = self.__call_side_effect
        self.__data_bus = AsyncMock(DataBus)
        self.__blockchain = Mock(BlockChain)
        self.__block_timestamp = 12345
        self.__data_version = 999
        self.__transformer = EvmTransactionReceiptToNftCollectionTransformer(
            self.__data_bus, self.__blockchain, self.__rpc_client, self.__data_version
        )
        transaction_receipt = EvmTransactionReceipt(
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
        block = EvmBlock(
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
            transaction_hashes=list(),
            transactions=list(),
            uncles=list(),
        )
        self.__default_data_package = EvmTransactionReceiptDataPackage(
            self.__blockchain, transaction_receipt, block
        )
        self.__call_function_results: Dict[
            bytes, Union[Dict[bytes, Union[Exception, tuple]], Union[Exception, tuple]]
        ] = {
            Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash: {
                Erc165InterfaceID.ERC721.bytes: (False,),
                Erc165InterfaceID.ERC1155.bytes: (False,),
            },
        }

    def __get_data_package_sent_to_data_bus(self):
        (data_package,) = self.__data_bus.send.call_args.args
        return data_package

    async def test_gets_no_contract_data_when_not_erc721_or_erc1155(self):
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = (
            False,
        )
        await self.__transformer.receive(self.__default_data_package)
        self.assertNotIn(
            call(
                EthCall(
                    None,
                    self.__default_data_package.transaction_receipt.contract_address,
                    Erc721MetadataFunctions.SYMBOL,
                )
            ),
            self.__rpc_client.mock_calls,
            "Unexpected call to get symbol",
        )
        self.assertNotIn(
            call(
                EthCall(
                    None,
                    self.__default_data_package.transaction_receipt.contract_address,
                    Erc721MetadataFunctions.NAME,
                )
            ),
            self.__rpc_client.mock_calls,
            "Unexpected call to get name",
        )
        self.assertNotIn(
            call(
                EthCall(
                    None,
                    self.__default_data_package.transaction_receipt.contract_address,
                    Erc721EnumerableFunctions.TOTAL_SUPPLY,
                )
            ),
            self.__rpc_client.mock_calls,
            "Unexpected call to get total supply",
        )
        self.assertNotIn(
            call(
                EthCall(
                    None,
                    self.__default_data_package.transaction_receipt.contract_address,
                    AdditionalFunctions.OWNER,
                )
            ),
            self.__rpc_client.mock_calls,
            "Unexpected call to get owner",
        )

    async def test_gets_contract_data_when_erc721(self):
        self.__call_function_results = {
            Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash: {
                Erc165InterfaceID.ERC721.bytes: (True,),
                Erc165InterfaceID.ERC1155.bytes: (False,),
            },
            Erc721MetadataFunctions.NAME.function_signature_hash: (None,),
            Erc721MetadataFunctions.SYMBOL.function_signature_hash: (None,),
            Erc721EnumerableFunctions.TOTAL_SUPPLY.function_signature_hash: (None,),
        }
        await self.__transformer.receive(self.__default_data_package)
        contract_address = self.__default_data_package.transaction_receipt.contract_address
        self.__rpc_client.call.assert_has_calls(
            [
                call(
                    EthCall(
                        None,
                        contract_address,
                        Erc165Functions.SUPPORTS_INTERFACE,
                        [Erc165InterfaceID.ERC721.bytes],
                    )
                ),
                call(
                    EthCall(
                        None,
                        contract_address,
                        Erc165Functions.SUPPORTS_INTERFACE,
                        [Erc165InterfaceID.ERC1155.bytes],
                    )
                ),
                call(
                    EthCall(
                        None,
                        self.__default_data_package.transaction_receipt.contract_address,
                        Erc721MetadataFunctions.SYMBOL,
                    )
                ),
                call(
                    EthCall(
                        None,
                        self.__default_data_package.transaction_receipt.contract_address,
                        Erc721MetadataFunctions.NAME,
                    )
                ),
                call(
                    EthCall(
                        None,
                        self.__default_data_package.transaction_receipt.contract_address,
                        Erc721EnumerableFunctions.TOTAL_SUPPLY,
                    )
                ),
                call(
                    EthCall(
                        None,
                        self.__default_data_package.transaction_receipt.contract_address,
                        AdditionalFunctions.OWNER,
                    )
                ),
            ],
            any_order=True,
        )

    @ddt.data(
        (True, False),
        (False, True),
        (True, True),
    )
    @ddt.unpack
    async def test_gets_owner_data_when_either_721_or_1155(self, is_721, is_1155):
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (is_721,),
            Erc165InterfaceID.ERC1155.bytes: (is_1155,),
        }
        self.__call_function_results[AdditionalFunctions.OWNER.function_signature_hash] = ("owner",)
        await self.__transformer.receive(self.__default_data_package)
        self.__rpc_client.call.assert_any_await(
            EthCall(
                None,
                self.__default_data_package.transaction_receipt.contract_address,
                AdditionalFunctions.OWNER,
            )
        )

    async def test_owner_is_none_when_get_owner_errors_in_1155(self):
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC1155.bytes: (True,),
        }
        self.__call_function_results[
            AdditionalFunctions.OWNER.function_signature_hash
        ] = RpcServerError(None, None, None, None)
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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

    async def test_owner_is_none_when_get_owner_errors_in_721(self):
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (True,),
        }
        self.__call_function_results[
            AdditionalFunctions.OWNER.function_signature_hash
        ] = RpcServerError(None, None, None, None)
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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

    async def test_symbol_is_none_when_get_symbol_errors_in_721(self):
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (True,),
        }
        self.__call_function_results[
            Erc721MetadataFunctions.SYMBOL.function_signature_hash
        ] = RpcServerError(None, None, None, None)
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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

    async def test_name_is_none_when_get_name_errors_in_721(self):
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (True,),
        }
        self.__call_function_results[
            Erc721MetadataFunctions.NAME.function_signature_hash
        ] = RpcServerError(None, None, None, None)
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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

    async def test_total_supply_is_none_when_get_total_supply_errors_in_721(self):
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (True,),
        }
        self.__call_function_results[
            Erc721EnumerableFunctions.TOTAL_SUPPLY.function_signature_hash
        ] = RpcServerError(None, None, None, None)
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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

    async def test_sets_specification_to_ERC721_when_ERC_721_supported(self):
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (True,),
            Erc165InterfaceID.ERC1155.bytes: (False,),
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (False,),
            Erc165InterfaceID.ERC1155.bytes: (True,),
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (True,),
            Erc165InterfaceID.ERC1155.bytes: (True,),
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = (
            True,
        )

        expected = 999
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash][
            Erc165InterfaceID.ERC721.bytes
        ] = (True,)
        self.__call_function_results[Erc721MetadataFunctions.NAME.function_signature_hash] = (
            expected[:],
        )
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash][
            Erc165InterfaceID.ERC721.bytes
        ] = (True,)
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash][
            Erc165InterfaceID.ERC721.bytes
        ] = (True,)
        self.__call_function_results[Erc721MetadataFunctions.SYMBOL.function_signature_hash] = (
            expected[:],
        )
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (True,)
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (True,)
        }
        self.__call_function_results[
            Erc721EnumerableFunctions.TOTAL_SUPPLY.function_signature_hash
        ] = (1,)
        expected = HexInt("0x1")
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (True,)
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: (True,)
        }
        self.__call_function_results[AdditionalFunctions.OWNER.function_signature_hash] = (
            expected[:],
        )
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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

    @patch("blockcrawler.nft.evm.transformers.decode")
    async def test_sets_none_for_owner_when_error(self, decode_patch):
        decode_patch.side_effect = NonEmptyPaddingBytes
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: ("X",)
        }
        await self.__transformer.receive(self.__default_data_package)
        self.__data_bus.send.assert_awaited_once_with(
            CollectionDataPackage(
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

    async def test_raises_consumer_error_for_rpc_client_error(self):
        self.__call_function_results[Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash] = {
            Erc165InterfaceID.ERC721.bytes: Exception("x")
        }
        with self.assertRaises(ConsumerError):
            await self.__transformer.receive(self.__default_data_package)


class EvmLogErc721TransferToNftTokenTransferTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__data_version = 123
        self.__transaction_type_oracle = Mock(TokenTransactionTypeOracle)
        self.__transaction_type = Mock(TokenTransactionType)
        self.__transaction_type_oracle.type_from_log.return_value = self.__transaction_type
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
        log = Mock(EvmLog)
        log.topics = []
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EvmTransactionReceipt),
                block=Mock(EvmBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_non_erc_721_transfer_logs(self):
        log = Mock(EvmLog)
        log.topics = ["0x1234567890abcdef1234567890abcdef"]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EvmTransactionReceipt),
                block=Mock(EvmBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_transforms_erc721_transfer_log_to_nft_token_transfer(self):
        blockchain = Mock(BlockChain)
        log = Mock(EvmLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(100)
        log.transaction_hash = HexBytes("0x9901")
        log.transaction_index = HexInt(200)
        log.log_index = HexInt(300)
        transaction_receipt = Mock(EvmTransactionReceipt)
        block = Mock(EvmBlock)
        block.timestamp = HexInt(400)
        log.topics = [
            Erc721Events.TRANSFER.event_signature_hash,
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
            TokenTransferDataPackage(
                token_transfer=TokenTransfer(
                    blockchain=blockchain,
                    timestamp=HexInt(400),
                    collection_id=Address("Collection ID"),
                    collection_type=EthereumCollectionType.ERC721,
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
        log = Mock(EvmLog)
        log.topics = [
            Erc721Events.TRANSFER.event_signature_hash,
            encode(["address"], ["0x8888888888888888888888888888888888888888"]),
            encode(["address"], ["0x9999999999999999999999999999999999999999"]),
            # ERC-20 only indexes _from and _to
        ]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EvmTransactionReceipt),
                block=Mock(EvmBlock),
            )
        )
        self.__data_bus.send.assert_not_called()


class EvmLogErc1155TransferSingleToNftTokenTransferTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__data_version = 123
        self.__transaction_type_oracle = Mock(TokenTransactionTypeOracle)
        self.__transaction_type = Mock(TokenTransactionType)
        self.__transaction_type_oracle.type_from_log.return_value = self.__transaction_type
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
        log = Mock(EvmLog)
        log.topics = []
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EvmTransactionReceipt),
                block=Mock(EvmBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_non_erc_1155_transfer_single_logs(self):
        log = Mock(EvmLog)
        log.topics = ["0x1234567890abcdef1234567890abcdef"]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EvmTransactionReceipt),
                block=Mock(EvmBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_transforms_erc1155_transfer_log_to_nft_token_transfer(self):
        blockchain = Mock(BlockChain)
        log = Mock(EvmLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(100)
        log.transaction_hash = HexBytes("0x9901")
        log.transaction_index = HexInt(200)
        log.log_index = HexInt(300)
        transaction_receipt = Mock(EvmTransactionReceipt)
        block = Mock(EvmBlock)
        block.timestamp = HexInt(400)
        log.topics = [
            Erc1155Events.TRANSFER_SINGLE.event_signature_hash,
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
            TokenTransferDataPackage(
                token_transfer=TokenTransfer(
                    blockchain=blockchain,
                    timestamp=HexInt(400),
                    collection_id=Address("Collection ID"),
                    collection_type=EthereumCollectionType.ERC1155,
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
        self.__transaction_type_oracle.type_from_log.return_value = self.__transaction_type
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
        log = Mock(EvmLog)
        log.topics = []
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EvmTransactionReceipt),
                block=Mock(EvmBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_non_erc_1155_transfer_single_logs(self):
        log = Mock(EvmLog)
        log.topics = ["0x1234567890abcdef1234567890abcdef"]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EvmTransactionReceipt),
                block=Mock(EvmBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_transforms_erc1155_transfer_log_to_nft_token_transfers(self):
        blockchain = Mock(BlockChain)
        log = Mock(EvmLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(100)
        log.transaction_hash = HexBytes("0x9901")
        log.transaction_index = HexInt(200)
        log.log_index = HexInt(300)
        transaction_receipt = Mock(EvmTransactionReceipt)
        block = Mock(EvmBlock)
        block.timestamp = HexInt(400)
        log.topics = [
            Erc1155Events.TRANSFER_BATCH.event_signature_hash,
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
                    TokenTransferDataPackage(
                        token_transfer=TokenTransfer(
                            blockchain=blockchain,
                            timestamp=HexInt(400),
                            collection_id=Address("Collection ID"),
                            collection_type=EthereumCollectionType.ERC1155,
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
                    TokenTransferDataPackage(
                        token_transfer=TokenTransfer(
                            blockchain=blockchain,
                            timestamp=HexInt(400),
                            collection_id=Address("Collection ID"),
                            collection_type=EthereumCollectionType.ERC1155,
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
        log = Mock(EvmLog)
        log.topics = []
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EvmTransactionReceipt),
                block=Mock(EvmBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_non_erc_1155_transfer_single_logs(self):
        log = Mock(EvmLog)
        log.topics = ["0x1234567890abcdef1234567890abcdef"]
        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=Mock(BlockChain),
                log=log,
                transaction_receipt=Mock(EvmTransactionReceipt),
                block=Mock(EvmBlock),
            )
        )
        self.__data_bus.send.assert_not_called()

    async def test_places_toke_uri_update_data_package_on_bus(self):
        blockchain = Mock(BlockChain)
        log = Mock(EvmLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(99_888_777)
        log.transaction_index = HexInt(234)
        log.log_index = HexInt(12)
        log.topics = [
            Erc1155Events.URI.event_signature_hash,
            encode(["uint256"], [12345]),
        ]
        log.data = encode(["string"], ["http://metadata.uri"])
        transaction_receipt = Mock(EvmTransactionReceipt)
        block = Mock(EvmBlock)

        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=blockchain,
                log=log,
                transaction_receipt=transaction_receipt,
                block=block,
            )
        )
        self.__data_bus.send.assert_awaited_once_with(
            TokenMetadataUriUpdatedDataPackage(
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
        log = Mock(EvmLog)
        log.address = Address("Collection ID")
        log.block_number = HexInt(99_888_777)
        log.transaction_index = HexInt(234)
        log.log_index = HexInt(12)
        log.topics = [
            Erc1155Events.URI.event_signature_hash,
            encode(["uint256"], [12345]),
        ]
        log.data = encode(["string"], ["http://metadata.uri/{id}.json"])
        transaction_receipt = Mock(EvmTransactionReceipt)
        block = Mock(EvmBlock)

        await self.__transformer.receive(
            EvmLogDataPackage(
                blockchain=blockchain,
                log=log,
                transaction_receipt=transaction_receipt,
                block=block,
            )
        )
        self.__data_bus.send.assert_awaited_once_with(
            TokenMetadataUriUpdatedDataPackage(
                blockchain=blockchain,
                collection_id=Address("Collection ID"),
                token_id=HexInt(12345),
                metadata_uri="http://metadata.uri/12345.json",
                metadata_uri_version=HexInt(1234),
                data_version=self.__data_version,
            )
        )


# noinspection PyDataclass
@ddt.ddt
class Erc721TokenTransferToNftTokenMetadataUriUpdatedTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__rpc_client = AsyncMock(EvmRpcClient)
        self.__rpc_client.call.return_value = ("Some URI",)
        self.__transformer = Erc721TokenTransferToNftTokenMetadataUriUpdatedTransformer(
            self.__data_bus,
            self.__rpc_client,
        )

        tt = MagicMock(TokenTransfer)
        self.__data_package = TokenTransferDataPackage(tt)
        tt.transaction_type = TokenTransactionType.MINT
        tt.collection_type = EthereumCollectionType.ERC721
        tt.collection_id = Address("Collection ID")
        tt.attribute_version = HexInt(1234)
        tt.token_id = HexInt(54321)
        tt.data_version = 112233
        tt.blockchain = MagicMock(BlockChain)
        tt.block_id = HexInt(0x1234)

    async def test_does_nothing_with_non_token_transfer_data_packages(self):
        await self.__transformer.receive(DataPackage())
        self.__data_bus.send.assert_not_called()

    @ddt.data(TokenTransactionType.TRANSFER, TokenTransactionType.BURN)
    async def test_does_nothing_with_transfer_or_burn(self, tx_type):
        self.__data_package.token_transfer.transaction_type = tx_type
        await self.__transformer.receive(self.__data_package)
        self.__data_bus.send.assert_not_called()

    async def test_does_nothing_with_non_erc721_transfers(self):
        self.__data_package.token_transfer.transaction_type = TokenTransactionType.MINT
        self.__data_package.token_transfer.collection_type = EthereumCollectionType.ERC1155
        await self.__transformer.receive(self.__data_package)
        self.__data_bus.send.assert_not_called()

    async def test_places_token_uri_update_data_package_on_bus(self):
        self.__rpc_client.call.side_effect = None
        self.__rpc_client.call.return_value = ("http://metadata.uri",)

        await self.__transformer.receive(self.__data_package)
        self.__data_bus.send.assert_awaited_once_with(
            TokenMetadataUriUpdatedDataPackage(
                blockchain=self.__data_package.token_transfer.blockchain,
                collection_id=self.__data_package.token_transfer.collection_id,
                token_id=self.__data_package.token_transfer.token_id,
                metadata_uri="http://metadata.uri",
                metadata_uri_version=self.__data_package.token_transfer.attribute_version,
                data_version=self.__data_package.token_transfer.data_version,
            )
        )

    async def test_sends_correct_data_to_RPC_API_to_get_URI(self):
        self.__rpc_client.call.side_effect = None
        self.__rpc_client.call.return_value = ("http://metadata.uri",)

        await self.__transformer.receive(self.__data_package)
        self.__rpc_client.call.assert_awaited_once_with(
            EthCall(
                from_=None,
                to=str(self.__data_package.token_transfer.collection_id),
                function=Erc721MetadataFunctions.TOKEN_URI,
                parameters=[self.__data_package.token_transfer.token_id.int_value],
                block=self.__data_package.token_transfer.block_id.hex_value,
            )
        )

    async def test_ignores_rpc_error_code_32000_and_exits(self):
        self.__rpc_client.call.side_effect = RpcServerError(None, None, -32000, None)

        await self.__transformer.receive(self.__data_package)
        self.__rpc_client.call.assert_awaited_once()
        self.__data_bus.send.assert_not_called()

    @ddt.data(RpcServerError(None, None, -32001, None), Exception())
    async def test_logs_consumer_error_for_not_32000_error_codes(self, error):
        self.__rpc_client.call.side_effect = error

        with self.assertRaises(ConsumerError) as actual:
            await self.__transformer.receive(self.__data_package)

        message = str(actual.exception)
        self.assertIn(
            self.__data_package.token_transfer.collection_id,
            message,
            "Expected collection ID in error",
        )
        self.assertIn(
            str(self.__data_package.token_transfer.token_id.int_value),
            message,
            "Expected token ID in error",
        )
        self.__rpc_client.call.assert_awaited_once()
        self.__data_bus.send.assert_not_called()
