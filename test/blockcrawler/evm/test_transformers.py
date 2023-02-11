from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, call, Mock, MagicMock, ANY

from hexbytes import HexBytes

from blockcrawler.core.bus import DataBus, DataPackage, ConsumerError
from blockcrawler.core.entities import BlockChain
from blockcrawler.evm.services import BlockTimeService
from blockcrawler.evm.data_packages import (
    EvmBlockDataPackage,
    EvmTransactionHashDataPackage,
    EvmTransactionReceiptDataPackage,
    EvmLogDataPackage,
    EvmBlockIDDataPackage,
    EvmTransactionDataPackage,
)
from blockcrawler.evm.rpc import EvmRpcClient
from blockcrawler.evm.transformers import (
    BlockIdToEvmBlockTransformer,
    EvmBlockToEvmTransactionHashTransformer,
    EvmTransactionHashToEvmTransactionReceiptTransformer,
    EvmTransactionReceiptToEvmLogTransformer,
    EvmBlockIdToEvmBlockAndEvmTransactionAndEvmTransactionHashTransformer,
)
from blockcrawler.evm.types import (
    EvmBlock,
    EvmTransactionReceipt,
    EvmLog,
    EvmTransaction,
    Erc165InterfaceID,
    Erc165Functions,
)
from blockcrawler.core.types import Address, HexInt
from blockcrawler.evm.transformers import (
    EvmTransactionToContractEvmTransactionReceiptTransformer,
)


class BlockIdToEvmBlockBatchTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock(EvmRpcClient)
        self.__data_bus = AsyncMock(DataBus)
        self.__rpc_client.get_block.return_value = None
        self.__blockchain = Mock(BlockChain)
        self.__transformer = BlockIdToEvmBlockTransformer(
            blockchain=self.__blockchain,
            data_bus=self.__data_bus,
            rpc_client=self.__rpc_client,
        )

    async def test_ignores_non_block_id_data_packages(self):
        # noinspection PyTypeChecker
        await self.__transformer.receive([None])
        self.__data_bus.send.assert_not_awaited()
        self.__rpc_client.get_block.assert_not_awaited()

    async def test_sends_block_ids_to_rpc_client_get_blocks(self):
        await self.__transformer.receive(EvmBlockIDDataPackage(self.__blockchain, HexInt(1)))
        self.__rpc_client.get_block.assert_awaited_once_with(HexInt(1))

    async def test_sends_blocks_retrieved_to_the_data_bus(self):
        block = Mock(EvmBlock)
        self.__rpc_client.get_block.return_value = block
        await self.__transformer.receive(EvmBlockIDDataPackage(self.__blockchain, HexInt(0)))
        self.__data_bus.send.assert_awaited_once_with(
            EvmBlockDataPackage(self.__blockchain, block),
        )


class EvmBlockToEvmTransactionHashTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__transformer = EvmBlockToEvmTransactionHashTransformer(self.__data_bus)
        self.__blockchain = Mock(BlockChain)

    async def test_places_transaction_hash_from_block_on_data_bus(self):
        block = Mock(EvmBlock)
        block.transaction_hashes = [HexBytes("0x01"), HexBytes("0x02")]
        data_package = EvmBlockDataPackage(self.__blockchain, block)
        await self.__transformer.receive(data_package)
        self.__data_bus.send.assert_has_awaits(
            [
                call(EvmTransactionHashDataPackage(self.__blockchain, HexBytes("0x01"), block)),
                call(EvmTransactionHashDataPackage(self.__blockchain, HexBytes("0x02"), block)),
            ]
        )


class EvmTransactionHashToEvmTransactionReceiptBatchTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock(EvmRpcClient)
        self.__data_bus = AsyncMock(DataBus)
        self.__rpc_client.get_transaction_receipt.return_value = None
        self.__blockchain = Mock(BlockChain)
        self.__transformer = EvmTransactionHashToEvmTransactionReceiptTransformer(
            blockchain=self.__blockchain,
            data_bus=self.__data_bus,
            rpc_client=self.__rpc_client,
        )

    async def test_ignores_non_block_id_data_packages(self):
        # noinspection PyTypeChecker
        await self.__transformer.receive([None])
        self.__data_bus.send.assert_not_awaited()
        self.__rpc_client.get_transaction_receipt.assert_not_awaited()

    async def test_sends_transaction_hashes_to_rpc_client_get_transaction_receipt(self):
        block = Mock(EvmBlock)
        await self.__transformer.receive(
            EvmTransactionHashDataPackage(self.__blockchain, HexBytes("0x01"), block)
        )
        self.__rpc_client.get_transaction_receipt.assert_awaited_once_with(HexBytes("0x01"))

    async def test_sends_transaction_receipt_retrieved_to_the_data_bus(self):
        block = Mock(EvmBlock)
        receipt = Mock(EvmTransactionReceipt)
        receipt.transaction_hash = HexBytes("0x11")
        self.__rpc_client.get_transaction_receipt.return_value = receipt
        await self.__transformer.receive(
            EvmTransactionHashDataPackage(self.__blockchain, HexBytes("0x11"), block),
        )
        self.__data_bus.send.assert_awaited_once_with(
            EvmTransactionReceiptDataPackage(self.__blockchain, receipt, block),
        )


class EvmTransactionReceiptToEvmLogTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__transformer = EvmTransactionReceiptToEvmLogTransformer(self.__data_bus)

    async def test_places_transformed_logs_on_data_bus(self):
        blockchain = Mock(BlockChain)
        block = Mock(EvmBlock)
        transaction_receipt = Mock(EvmTransactionReceipt)
        log_1 = Mock(EvmLog)
        log_2 = Mock(EvmLog)
        transaction_receipt.logs = [log_1, log_2]
        data_package = EvmTransactionReceiptDataPackage(
            blockchain=blockchain,
            transaction_receipt=transaction_receipt,
            block=block,
        )
        expected_calls = [
            call(EvmLogDataPackage(blockchain, log_1, transaction_receipt, block)),
            call(EvmLogDataPackage(blockchain, log_2, transaction_receipt, block)),
        ]
        await self.__transformer.receive(data_package)
        self.__data_bus.send.assert_has_awaits(expected_calls, any_order=True)

    async def test_does_not_pass_non_transaction_receipt_data_package_to_bus(self):
        await self.__transformer.receive(DataPackage())
        self.__data_bus.send.assert_not_called()


class EvmBlockIdToEvmBlockAndEvmTransactionTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__block_time_service = AsyncMock(BlockTimeService)
        self.__rpc_client = AsyncMock(EvmRpcClient)

        self.__transformer = EvmBlockIdToEvmBlockAndEvmTransactionAndEvmTransactionHashTransformer(
            self.__data_bus,
            self.__block_time_service,
            self.__rpc_client,
        )

        self.__get_block_response = MagicMock(EvmBlock)
        self.__get_block_response.timestamp = HexInt(0)
        self.__get_block_response.transactions = list()
        self.__get_block_response.transaction_hashes = list()
        self.__rpc_client.get_block.return_value = self.__get_block_response

    async def test_ignores_other_data_packages(self):
        await self.__transformer.receive(DataPackage())

    async def test_calls_rpc_client_get_block_with_proper_block_id_param(self):
        block_id = HexInt(433685734)
        blockchain = MagicMock(BlockChain)
        await self.__transformer.receive(EvmBlockIDDataPackage(blockchain, block_id))
        self.__rpc_client.get_block.assert_awaited_once_with(block_id, ANY)

    async def test_calls_rpc_client_get_block_with_proper_full_transactions_param(self):
        block_id = HexInt(433685734)
        blockchain = MagicMock(BlockChain)
        await self.__transformer.receive(EvmBlockIDDataPackage(blockchain, block_id))
        self.__rpc_client.get_block.assert_awaited_once_with(ANY, True)

    async def test_stores_block_timestamp_in_block_time_service(self):
        self.__get_block_response.timestamp = HexInt(297602976905874)
        block_id = HexInt(433685734)
        blockchain = MagicMock(BlockChain)
        await self.__transformer.receive(EvmBlockIDDataPackage(blockchain, block_id))
        self.__block_time_service.set_block_timestamp.assert_awaited_once_with(
            block_id, 297602976905874
        )

    async def test_places_block_data_package_on_bus(self):
        blockchain = MagicMock(BlockChain)
        expected = EvmBlockDataPackage(blockchain, self.__get_block_response)
        await self.__transformer.receive(EvmBlockIDDataPackage(blockchain, HexInt(0)))
        self.__data_bus.send.assert_awaited_once_with(expected)

    async def test_places_transaction_data_packages_on_bus(self):
        blockchain = MagicMock(BlockChain)
        transactions = [MagicMock(EvmTransaction), MagicMock(EvmTransaction)]
        self.__get_block_response.transactions = transactions
        expected_calls = [
            call(
                (
                    EvmTransactionDataPackage(
                        blockchain=blockchain,
                        transaction=transactions[0],
                        block=self.__get_block_response,
                    )
                )
            ),
            call(
                (
                    EvmTransactionDataPackage(
                        blockchain=blockchain,
                        transaction=transactions[1],
                        block=self.__get_block_response,
                    )
                )
            ),
        ]
        await self.__transformer.receive(EvmBlockIDDataPackage(blockchain, HexInt(0)))
        self.__data_bus.send.assert_has_awaits(expected_calls, any_order=True)

    async def test_places_transaction_hash_data_packages_on_bus(self):
        blockchain = MagicMock(BlockChain)
        transaction_hashes = [HexBytes(b"1"), HexBytes(b"2")]
        self.__get_block_response.transaction_hashes = transaction_hashes
        expected_calls = [
            call(
                (
                    EvmTransactionHashDataPackage(
                        blockchain=blockchain,
                        hash=transaction_hashes[0],
                        block=self.__get_block_response,
                    )
                )
            ),
            call(
                (
                    EvmTransactionHashDataPackage(
                        blockchain=blockchain,
                        hash=transaction_hashes[1],
                        block=self.__get_block_response,
                    )
                )
            ),
        ]
        await self.__transformer.receive(EvmBlockIDDataPackage(blockchain, HexInt(0)))
        self.__data_bus.send.assert_has_awaits(expected_calls, any_order=True)

    async def test_block_without_full_transactions_raises_consumer_error(self):
        self.__get_block_response.transactions = None
        with self.assertRaisesRegex(
            ConsumerError, "Block returned did not have full transactions!"
        ):
            await self.__transformer.receive(
                EvmBlockIDDataPackage(MagicMock(BlockChain), HexInt(0))
            )


class EvmTransactionToContractEvmTransactionTransformerTestCaste(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__rpc_client = AsyncMock(EvmRpcClient)
        self.__get_txr_response = AsyncMock(EvmTransactionReceipt)
        self.__get_txr_response.contract_address = None
        self.__rpc_client.get_transaction_receipt.return_value = self.__get_txr_response
        self.__transformer = EvmTransactionToContractEvmTransactionReceiptTransformer(
            self.__data_bus, self.__rpc_client
        )
        self.__transaction = MagicMock(EvmTransaction)
        self.__transaction.to_ = None
        self.__transaction.hash = HexBytes(b"hash")
        self.__transaction.input = (
            HexBytes("0x63")
            + Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash
            + HexBytes("0x63")
            + Erc165InterfaceID.ERC721.bytes
            + HexBytes("0x63")
            + Erc165InterfaceID.ERC1155.bytes
        )

    async def test_ignores_other_data_packages(self):
        await self.__transformer.receive(DataPackage())

    async def test_does_not_get_transaction_receipt_when_transaction_to_is_not_null(self):
        self.__transaction.to_ = Address("Me")
        await self.__transformer.receive(
            EvmTransactionDataPackage(
                MagicMock(BlockChain), self.__transaction, MagicMock(EvmBlock)
            )
        )
        self.__rpc_client.get_transaction_receipt.assert_not_called()

    async def test_does_not_get_transaction_receipt_when_push4_erc165_func_hash_not_in_input(self):
        self.__transaction.input = HexBytes(
            Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash
        )
        await self.__transformer.receive(
            EvmTransactionDataPackage(
                MagicMock(BlockChain), self.__transaction, MagicMock(EvmBlock)
            )
        )
        self.__rpc_client.get_transaction_receipt.assert_not_called()

    async def test_gets_transaction_receipt_when_transaction_to_is_null(self):
        self.__transaction.to_ = None
        self.__transaction.hash = HexBytes(b"hash")
        await self.__transformer.receive(
            EvmTransactionDataPackage(
                MagicMock(BlockChain), self.__transaction, MagicMock(EvmBlock)
            )
        )
        self.__rpc_client.get_transaction_receipt.assert_awaited_once_with(HexBytes(b"hash"))

    async def test_does_not_send_transaction_receipt_when_address_is_none(self):
        self.__get_txr_response.contract_address = None
        await self.__transformer.receive(
            EvmTransactionDataPackage(
                MagicMock(BlockChain), self.__transaction, MagicMock(EvmBlock)
            )
        )
        self.__rpc_client.get_transaction_receipt.assert_awaited()
        self.__data_bus.send.assert_not_called()

    async def test_sends_transaction_receipt_when_address_is_not_none(self):
        self.__get_txr_response.contract_address = Address("ct addr")
        blockchain = MagicMock(BlockChain)
        block = MagicMock(EvmBlock)
        await self.__transformer.receive(
            EvmTransactionDataPackage(blockchain, self.__transaction, block)
        )
        self.__data_bus.send.assert_awaited_once_with(
            EvmTransactionReceiptDataPackage(
                blockchain=blockchain, transaction_receipt=self.__get_txr_response, block=block
            )
        )
