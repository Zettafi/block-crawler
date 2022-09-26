from blockrail.blockcrawler.core.bus import (
    Transformer,
    DataPackage,
    DataBus,
)
from blockrail.blockcrawler.core.entities import BlockChain
from blockrail.blockcrawler.evm.data_packages import (
    EVMBlockDataPackage,
    EVMTransactionHashDataPackage,
    EvmTransactionReceiptDataPackage,
    EvmLogDataPackage,
)
from blockrail.blockcrawler.evm.producers import EVMBlockIDDataPackage
from blockrail.blockcrawler.evm.rpc import EVMRPCClient


class BlockIdToEvmBlockTransformer(Transformer):
    def __init__(self, data_bus: DataBus, blockchain: BlockChain, rpc_client: EVMRPCClient) -> None:
        super().__init__(data_bus)
        self.__blockchain = blockchain
        self.__rpc_client = rpc_client

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, EVMBlockIDDataPackage):
            return
        block = await self.__rpc_client.get_block(data_package.block_id)
        data_package = EVMBlockDataPackage(self.__blockchain, block)
        await self._get_data_bus().send(data_package)


class EvmBlockToEvmTransactionHashTransformer(Transformer):
    async def receive(self, data_package: DataPackage):
        if isinstance(data_package, EVMBlockDataPackage):
            for transaction in data_package.block.transactions:
                hash_package = EVMTransactionHashDataPackage(
                    data_package.blockchain, transaction, data_package.block
                )
                await self._get_data_bus().send(hash_package)


class EvmTransactionHashToEvmTransactionReceipTransformer(Transformer):
    def __init__(self, data_bus: DataBus, blockchain: BlockChain, rpc_client: EVMRPCClient) -> None:
        super().__init__(data_bus)
        self.__blockchain = blockchain
        self.__rpc_client = rpc_client

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, EVMTransactionHashDataPackage):
            return

        transaction_receipt = await self.__rpc_client.get_transaction_receipt(data_package.hash)
        receipt_data_package = EvmTransactionReceiptDataPackage(
            self.__blockchain, transaction_receipt, data_package.block
        )
        await self._get_data_bus().send(receipt_data_package)


class EvmTransactionToLogTransformer(Transformer):
    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, EvmTransactionReceiptDataPackage):
            return

        for log in data_package.transaction_receipt.logs:
            log_data_package = EvmLogDataPackage(
                data_package.blockchain,
                log,
                data_package.transaction_receipt,
                data_package.block,
            )
            await self._get_data_bus().send(log_data_package)
