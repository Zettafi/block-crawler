from blockcrawler.core.bus import (
    Transformer,
    DataPackage,
    DataBus,
    ConsumerError,
)
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
from blockcrawler.evm.types import Erc165InterfaceID, Erc165Functions


class BlockIdToEvmBlockTransformer(Transformer):
    def __init__(self, data_bus: DataBus, blockchain: BlockChain, rpc_client: EvmRpcClient) -> None:
        super().__init__(data_bus)
        self.__blockchain = blockchain
        self.__rpc_client = rpc_client

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, EvmBlockIDDataPackage):
            return
        block = await self.__rpc_client.get_block(data_package.block_id)
        data_package = EvmBlockDataPackage(self.__blockchain, block)
        await self._get_data_bus().send(data_package)


class EvmBlockToEvmTransactionHashTransformer(Transformer):
    async def receive(self, data_package: DataPackage):
        if isinstance(data_package, EvmBlockDataPackage):
            for transaction in data_package.block.transaction_hashes:
                hash_package = EvmTransactionHashDataPackage(
                    data_package.blockchain, transaction, data_package.block
                )
                await self._get_data_bus().send(hash_package)


class EvmBlockIdToEvmBlockAndEvmTransactionAndEvmTransactionHashTransformer(Transformer):
    def __init__(
        self, data_bus: DataBus, block_time_service: BlockTimeService, rpc_client: EvmRpcClient
    ) -> None:
        super().__init__(data_bus)
        self.__block_time_service = block_time_service
        self.__rpc_client = rpc_client

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, EvmBlockIDDataPackage):
            return

        try:
            block = await self.__rpc_client.get_block(data_package.block_id, True)
            await self.__block_time_service.set_block_timestamp(
                data_package.block_id, block.timestamp
            )
            await self._get_data_bus().send(EvmBlockDataPackage(data_package.blockchain, block))
            for transaction_hash in block.transaction_hashes:
                await self._get_data_bus().send(
                    EvmTransactionHashDataPackage(data_package.blockchain, transaction_hash, block)
                )

            if block.transactions is None:
                raise ConsumerError("Block returned did not have full transactions!")
            for transaction in block.transactions:
                await self._get_data_bus().send(
                    EvmTransactionDataPackage(data_package.blockchain, transaction, block)
                )
        except Exception as e:
            raise ConsumerError(f"Error processing block ID {data_package.block_id} - {e}")


class EvmTransactionHashToEvmTransactionReceiptTransformer(Transformer):
    def __init__(self, data_bus: DataBus, blockchain: BlockChain, rpc_client: EvmRpcClient) -> None:
        super().__init__(data_bus)
        self.__blockchain = blockchain
        self.__rpc_client = rpc_client

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, EvmTransactionHashDataPackage):
            return

        transaction_receipt = await self.__rpc_client.get_transaction_receipt(data_package.hash)
        receipt_data_package = EvmTransactionReceiptDataPackage(
            self.__blockchain, transaction_receipt, data_package.block
        )
        await self._get_data_bus().send(receipt_data_package)


class EvmTransactionReceiptToEvmLogTransformer(Transformer):
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


class EvmTransactionToContractEvmTransactionReceiptTransformer(Transformer):
    def __init__(self, data_bus: DataBus, rpc_client: EvmRpcClient) -> None:
        super().__init__(data_bus)
        self.__rpc_client = rpc_client

    async def receive(self, data_package: DataPackage):
        if (
            not isinstance(data_package, EvmTransactionDataPackage)
            or data_package.transaction.to_ is not None
            # If there is a "to" address, it's not contrat creation
            or Erc165Functions.SUPPORTS_INTERFACE.function_signature_hash
            not in data_package.transaction.input
            # If it's ERC-721 or ERC-1165, it will have the ERC-165 function signature hash stored
            or not (
                Erc165InterfaceID.ERC721.bytes in data_package.transaction.input
                or Erc165InterfaceID.ERC1155.bytes in data_package.transaction.input
            )
        ):
            return

        receipt = await self.__rpc_client.get_transaction_receipt(data_package.transaction.hash)
        if receipt.contract_address is not None:
            await self._get_data_bus().send(
                EvmTransactionReceiptDataPackage(
                    blockchain=data_package.blockchain,
                    transaction_receipt=receipt,
                    block=data_package.block,
                )
            )
