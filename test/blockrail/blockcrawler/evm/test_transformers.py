from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, call, Mock

from hexbytes import HexBytes

from blockrail.blockcrawler.core.bus import DataBus, DataPackage
from blockrail.blockcrawler.core.entities import BlockChain, HexInt
from blockrail.blockcrawler.evm.producers import EVMBlockIDDataPackage
from blockrail.blockcrawler.evm.rpc import EVMRPCClient
from blockrail.blockcrawler.evm.transformers import (
    BlockIdToEvmBlockTransformer,
    EvmBlockToEvmTransactionHashTransformer,
    EvmTransactionHashToEvmTransactionReceipTransformer,
    EvmTransactionToLogTransformer,
)
from blockrail.blockcrawler.evm.data_packages import (
    EVMBlockDataPackage,
    EVMTransactionHashDataPackage,
    EvmTransactionReceiptDataPackage,
    EvmLogDataPackage,
)
from blockrail.blockcrawler.evm.types import EVMBlock, EVMTransactionReceipt, EVMLog


class BlockIdToEvmBlockBatchTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock(EVMRPCClient)
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
        await self.__transformer.receive(EVMBlockIDDataPackage(self.__blockchain, HexInt(1)))
        self.__rpc_client.get_block.assert_awaited_once_with(HexInt(1))

    async def test_sends_blocks_retrieved_to_the_data_bus(self):
        block = Mock(EVMBlock)
        self.__rpc_client.get_block.return_value = block
        await self.__transformer.receive(EVMBlockIDDataPackage(self.__blockchain, HexInt(0)))
        self.__data_bus.send.assert_awaited_once_with(
            EVMBlockDataPackage(self.__blockchain, block),
        )


class EvmBlockToEvmTransactionHashTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__transformer = EvmBlockToEvmTransactionHashTransformer(self.__data_bus)
        self.__blockchain = Mock(BlockChain)

    async def test_places_transaction_hash_from_block_on_data_bus(self):
        block = Mock(EVMBlock)
        block.transactions = [HexBytes("0x01"), HexBytes("0x02")]
        data_package = EVMBlockDataPackage(self.__blockchain, block)
        await self.__transformer.receive(data_package)
        self.__data_bus.send.assert_has_awaits(
            [
                call(EVMTransactionHashDataPackage(self.__blockchain, HexBytes("0x01"), block)),
                call(EVMTransactionHashDataPackage(self.__blockchain, HexBytes("0x02"), block)),
            ]
        )


class EvmTransactionHashToEvmTransactionReceiptBatchTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock(EVMRPCClient)
        self.__data_bus = AsyncMock(DataBus)
        self.__rpc_client.get_transaction_receipt.return_value = None
        self.__blockchain = Mock(BlockChain)
        self.__transformer = EvmTransactionHashToEvmTransactionReceipTransformer(
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
        block = Mock(EVMBlock)
        await self.__transformer.receive(
            EVMTransactionHashDataPackage(self.__blockchain, HexBytes("0x01"), block)
        )
        self.__rpc_client.get_transaction_receipt.assert_awaited_once_with(HexBytes("0x01"))

    async def test_sends_transaction_receipt_retrieved_to_the_data_bus(self):
        block = Mock(EVMBlock)
        receipt = Mock(EVMTransactionReceipt)
        receipt.transaction_hash = HexBytes("0x11")
        self.__rpc_client.get_transaction_receipt.return_value = receipt
        await self.__transformer.receive(
            EVMTransactionHashDataPackage(self.__blockchain, HexBytes("0x11"), block),
        )
        self.__data_bus.send.assert_awaited_once_with(
            EvmTransactionReceiptDataPackage(self.__blockchain, receipt, block),
        )


class EvmTransactionToLogTransformerTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_bus = AsyncMock(DataBus)
        self.__transformer = EvmTransactionToLogTransformer(self.__data_bus)

    async def test_places_transformed_logs_on_data_bus(self):
        blockchain = Mock(BlockChain)
        block = Mock(EVMBlock)
        transaction_receipt = Mock(EVMTransactionReceipt)
        log_1 = Mock(EVMLog)
        log_2 = Mock(EVMLog)
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
