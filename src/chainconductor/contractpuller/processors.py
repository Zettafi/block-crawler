"""
Contracts
    blockchain - Ethereum Mainnet
    address -
    creation_block
    creation_tx
    date_created
    contract_specification
    abi
"""
import asyncio
from asyncio import Queue, QueueEmpty
from datetime import datetime, timedelta

from .events import EventBus
from .stats import StatsService
from ..data.models import Contracts
from ..web3.rpc import RPCClient

CONTRACT_PROCESSOR_STOPPED_EVENT = "contract_processor_stopped"
TRANSACTION_PROCESSOR_STOPPED_EVENT = "transaction_processor_stopped"
BLOCK_PROCESSOR_STOPPED_EVENT = "block_processor_stopped"

""" Collect all smart contracts from blockchain
@provider_url Specify url for archive node
@from_block Which block to start from
"""


class Processor:
    def __init__(self) -> None:
        self.__running: bool = False
        self.__stopping = False

    async def start(self):
        self.__running = True
        await self._process()

    async def abort(self):
        self.__running = False

    async def stop(self):
        self.__stopping = True

    async def is_running(self):
        return self.__running

    async def is_stopping(self):
        return self.__stopping

    async def _process(self):
        raise NotImplementedError

    async def __full_stop(self):
        self.__running = False

    async def _get_batch_items_from_queue(
        self, queue: Queue, batch_size: int, max_batch_wait: timedelta
    ):
        last_batch_time = datetime.now()
        items = list()
        while True:
            try:
                item = queue.get_nowait()
                items.append(item)
                queue.task_done()
            except QueueEmpty:
                if await self.is_stopping():
                    # If the queue is empty, and we have been signalled to stop,
                    # stop the process and return the items
                    await self.__full_stop()
                    break
                else:
                    # If we're not stopping, release the loop and continue
                    await asyncio.sleep(0)
                    continue
            now = datetime.now()
            if len(items) >= batch_size or now - last_batch_time > max_batch_wait:
                break
        return items


class BlockProcessor(Processor):
    """
    Block processor processes block IDs in the block_id_queue passing the blocks to the transaction_queue
    until a "None" ID is received. Calls to get blocks wil be in batches specified by rpc_batch_size unless
    the max_batch_wait is exceeded which will process all bloc IDs received up to that point.
    """

    def __init__(
        self,
        rpc_client: RPCClient,
        stats_service: StatsService,
        event_bus: EventBus,
        rpc_batch_size: int,
        block_id_queue: Queue,
        transaction_queue: Queue,
        max_batch_wait: int,
    ) -> None:
        super().__init__()
        self.__rpc_client: RPCClient = rpc_client
        self.__stats_service: StatsService = stats_service
        self.__event_bus = event_bus
        self.__rpc_batch_size: int = rpc_batch_size
        self.__block_id_queue: Queue = block_id_queue
        self.__transaction_queue: Queue = transaction_queue
        self.__max_batch_wait: timedelta = timedelta(seconds=max_batch_wait)

    async def _process(self):
        while await self.is_running():
            block_ids = await self._get_batch_items_from_queue(
                self.__block_id_queue, self.__rpc_batch_size, self.__max_batch_wait
            )
            if len(block_ids) == 0:
                # Don't process empty batches
                continue
            with self.__stats_service.timer("rpc_get_blocks"):
                blocks = await self.__rpc_client.get_blocks(
                    set(block_ids), full_transactions=True
                )
            for block in blocks:
                for transaction in block.transactions:
                    if transaction.to_ is None:
                        await self.__transaction_queue.put((block, transaction))
                        self.__stats_service.increment(StatsService.TRANSACTIONS)
                self.__stats_service.increment(StatsService.BLOCKS)
        await self.__event_bus.trigger(BLOCK_PROCESSOR_STOPPED_EVENT)


class TransactionProcessor(Processor):
    def __init__(
        self,
        rpc_client: RPCClient,
        stats_service: StatsService,
        event_bus: EventBus,
        rpc_batch_size: int,
        transaction_queue: Queue,
        contract_queue: Queue,
        max_batch_wait: int,
    ) -> None:
        super().__init__()
        self.__rpc_client: RPCClient = rpc_client
        self.__stats_service: StatsService = stats_service
        self.__event_bus = event_bus
        self.__rpc_batch_size: int = rpc_batch_size
        self.__transaction_queue: Queue = transaction_queue
        self.__contract_queue: Queue = contract_queue
        self.__max_batch_wait: timedelta = timedelta(seconds=max_batch_wait)

    async def _process(self):
        while await self.is_running():
            block_transactions = await self._get_batch_items_from_queue(
                self.__transaction_queue, self.__rpc_batch_size, self.__max_batch_wait
            )
            if len(block_transactions) == 0:
                # Don't process empty batches
                continue

            transaction_hashes = list()
            block_id_block_transactions_map = dict()
            for block, transaction in block_transactions:
                block_id_block_transactions_map[block.number] = (block, transaction)
                transaction_hashes.append(transaction.hash)
            with self.__stats_service.timer("rpc_get_transaction_receipts"):
                receipts = await self.__rpc_client.get_transaction_receipts(
                    transaction_hashes
                )
            for receipt in receipts:
                block, transaction = block_id_block_transactions_map[
                    receipt.block_number
                ]
                await self.__contract_queue.put((block, transaction, receipt))
                self.__stats_service.increment(StatsService.TRANSACTION_RECEIPTS)

        await self.__event_bus.trigger(TRANSACTION_PROCESSOR_STOPPED_EVENT)


class ContractProcessor(Processor):
    def __init__(
        self,
        dynamodb,
        stats_service: StatsService,
        event_bus: EventBus,
        contract_queue: Queue,
        dynamodb_batch_size: int,
        max_batch_wait: int,
    ) -> None:
        super().__init__()
        self.__dynamodb = dynamodb
        self.__stats_service: StatsService = stats_service
        self.__event_bus = event_bus
        self.__contract_queue: Queue = contract_queue
        self.__dynamodb_batch_size: int = dynamodb_batch_size
        self.__max_batch_wait: timedelta = timedelta(seconds=max_batch_wait)

    async def _process(self):
        contracts = await self.__dynamodb.Table(Contracts.table_name)
        while await self.is_running():
            block_transaction_receipts = await self._get_batch_items_from_queue(
                self.__contract_queue, self.__dynamodb_batch_size, self.__max_batch_wait
            )
            if len(block_transaction_receipts) == 0:
                # Don't process empty batches
                continue

            with self.__stats_service.timer("dynamodb_batch_write"):
                async with contracts.batch_writer() as batch:
                    for block, transaction, receipt in block_transaction_receipts:
                        await batch.put_item(
                            Item={
                                "blockchain": "Ethereum Mainnet",
                                "address": receipt.contract_address,
                                "block_number": transaction.block_number.hex_value,
                                "transaction_hash": transaction.hash,
                                "creator": transaction.from_,
                                "timestamp": block.timestamp.int_value,
                            }
                        )
                        self.__stats_service.increment(StatsService.CONTRACTS)
        await self.__event_bus.trigger(CONTRACT_PROCESSOR_STOPPED_EVENT)
