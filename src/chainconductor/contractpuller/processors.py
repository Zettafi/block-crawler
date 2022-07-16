import asyncio
from asyncio import Queue, QueueEmpty
from datetime import datetime, timedelta
from typing import Optional, List

from .events import EventBus
from .stats import StatsService
from ..data.models import Contracts
from ..web3.rpc import RPCClient, RPCError, EthCall
from ..web3.types import TransactionReceipt, Transaction, Block, Contract, ERC165InterfaceID
from ..web3.util import (
    ERC165Functions,
    contract_implements_function,
    ERC721MetadataFunctions,
    ERC721EnumerableFunctions,
)


class ContractTransportObject:
    def __init__(
        self,
        *,
        block: Optional[Block] = None,
        transaction: Optional[Transaction] = None,
        transaction_receipt: Optional[TransactionReceipt] = None,
        contract: Optional[Contract] = None
    ) -> None:
        self.__block: Optional[Block] = block
        self.__transaction: Optional[Transaction] = transaction
        self.__transaction_receipt: Optional[TransactionReceipt] = transaction_receipt
        self.__contract: Optional[Contract] = contract

    @property
    def block(self) -> Optional[Block]:
        return self.__block

    @property
    def transaction(self) -> Optional[Transaction]:
        return self.__transaction

    @property
    def transaction_receipt(self) -> Optional[TransactionReceipt]:
        return self.__transaction_receipt

    @property
    def contract(self) -> Optional[Contract]:
        return self.__contract


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

    PROCESSED_STAT = "blocks"
    PROCESSOR_STOPPED_EVENT = "block_processor_stopped"
    RPC_TIMER_GET_BLOCKS = "rpc_get_blocks"

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
            with self.__stats_service.timer(self.RPC_TIMER_GET_BLOCKS):
                try:
                    blocks = await self.__rpc_client.get_blocks(
                        set(block_ids), full_transactions=True
                    )
                except RPCError as e:
                    raise  # TODO: handle this
                except Exception as e:
                    raise  # TODO: handle this

            for block in blocks:
                for transaction in block.transactions:
                    if transaction.to_ is None:
                        transport = ContractTransportObject(
                            block=block, transaction=transaction
                        )
                        await self.__transaction_queue.put(transport)
                self.__stats_service.increment(self.PROCESSED_STAT)
        await self.__event_bus.trigger(self.PROCESSOR_STOPPED_EVENT)


class TransactionProcessor(Processor):
    PROCESSED_STAT = "transactions_processed"
    PROCESSOR_STOPPED_EVENT = "transaction_processor_stopped"
    RPC_TIMER_GET_TRANSACTION_RECEIPTS = "rpc_get_transaction_receipts"

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
            transport_objects: List[
                ContractTransportObject
            ] = await self._get_batch_items_from_queue(
                self.__transaction_queue, self.__rpc_batch_size, self.__max_batch_wait
            )
            if len(transport_objects) == 0:
                # Don't process empty batches
                continue

            transaction_hashes = list()
            transactions_hash_map = dict()
            for in_transport_object in transport_objects:
                transactions_hash_map[
                    in_transport_object.transaction.hash
                ] = in_transport_object
                transaction_hashes.append(in_transport_object.transaction.hash)
            with self.__stats_service.timer(self.RPC_TIMER_GET_TRANSACTION_RECEIPTS):
                receipts = await self.__rpc_client.get_transaction_receipts(
                    transaction_hashes
                )
            for receipt in receipts:
                # TODO - FIX block ID could have multiple transactions in the block
                in_transport_object = transactions_hash_map[receipt.transaction_hash]
                out_transport_object = ContractTransportObject(
                    block=in_transport_object.block,
                    transaction=in_transport_object.transaction,
                    transaction_receipt=receipt,
                )
                await self.__contract_queue.put(out_transport_object)
                self.__stats_service.increment(self.PROCESSED_STAT)

        await self.__event_bus.trigger(self.PROCESSOR_STOPPED_EVENT)


class ContractProcessor(Processor):
    RPPC_TIMER_CALL_SUPPORTS_INTERFACES = "rpc_timer_call_supports_interfaces"
    RPC_TIMER_CALL_CONTRACT_METADATA = "rpc_timer_call_contract_metadata"
    PROCESSED_STAT = "contracts_processed"
    PROCESSOR_STOPPED_EVENT = "contract_processor_stopped"
    CODE_TIMER_OPCODE_DISCOVERY = "code_contract_bytes_to_opcode"

    def __init__(
        self,
        rpc_client: RPCClient,
        stats_service: StatsService,
        event_bus: EventBus,
        rpc_batch_size: int,
        contract_queue: Queue,
        persistence_queue: Queue,
        max_batch_wait: int,
    ) -> None:
        super().__init__()
        self.__rpc_client: RPCClient = rpc_client
        self.__stats_service: StatsService = stats_service
        self.__event_bus = event_bus
        self.__rpc_batch_size: int = rpc_batch_size
        self.__contract_queue: Queue = contract_queue
        self.__persistence_queue: Queue = persistence_queue
        self.__max_batch_wait: timedelta = timedelta(seconds=max_batch_wait)

    async def _process(self):
        while await self.is_running():
            transport_objects: List[
                ContractTransportObject
            ] = await self._get_batch_items_from_queue(
                self.__contract_queue, self.__rpc_batch_size, self.__max_batch_wait
            )
            if len(transport_objects) == 0:
                # Don't process empty batches
                continue
            for in_transport_object in transport_objects:
                contract_implements_supports_interface = contract_implements_function(
                    in_transport_object.transaction.input,
                    ERC165Functions.SUPPORTS_INTERFACE.function_hash[:],
                )

                if contract_implements_supports_interface:
                    contract_id = (
                        in_transport_object.transaction_receipt.contract_address
                    )
                    supported_interfaces = await self.__get_supported_interfaces(
                        contract_id
                    )
                    if ERC165InterfaceID.ERC721 in supported_interfaces:

                        name, symbol, total_supply = await self.__get_contract_metadata(
                            contract_id, supported_interfaces
                        )
                        contract = Contract(
                            address=in_transport_object.transaction_receipt.contract_address,
                            creator=in_transport_object.transaction.from_,
                            interfaces=supported_interfaces,
                            name=name,
                            symbol=symbol,
                            total_supply=total_supply,
                            tokens=[],
                        )

                        out_transport_object = ContractTransportObject(
                            block=in_transport_object.block,
                            transaction=in_transport_object.transaction,
                            transaction_receipt=in_transport_object.transaction_receipt,
                            contract=contract,
                        )
                        await self.__persistence_queue.put(out_transport_object)
                self.__stats_service.increment(self.PROCESSED_STAT)
        await self.__event_bus.trigger(self.PROCESSOR_STOPPED_EVENT)

    async def __get_supported_interfaces(
        self, contract_address
    ) -> List[ERC165InterfaceID]:
        with self.__stats_service.timer(self.RPPC_TIMER_CALL_SUPPORTS_INTERFACES):
            supports_interface_responses = await self.__rpc_client.calls(
                [
                    EthCall(
                        ERC165InterfaceID.ERC721.value,
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC721.bytes],
                    ),
                    EthCall(
                        ERC165InterfaceID.ERC721_METADATA.value,
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC721_METADATA.bytes],
                    ),
                    EthCall(
                        ERC165InterfaceID.ERC721_ENUMERABLE.value,
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC721_ENUMERABLE.bytes],
                    ),
                    EthCall(
                        ERC165InterfaceID.ERC721_ENUMERABLE.value,
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC1155_METADATA_URI.bytes],
                    ),
                ]
            )
        supports_interfaces = [
            ERC165InterfaceID.from_value(key)
            for key, value in supports_interface_responses.items()
            if not isinstance(value, RPCError) and value[0] is True
        ]
        return supports_interfaces

    async def __get_contract_metadata(
        self, contract_address: str, supported_interfaces: List[ERC165InterfaceID]
    ) -> (str, str, int):
        calls = list()
        if ERC165InterfaceID.ERC721_METADATA in supported_interfaces:
            calls += [
                EthCall(
                    "symbol",
                    None,
                    contract_address,
                    ERC721MetadataFunctions.SYMBOL,
                ),
                EthCall(
                    "name",
                    None,
                    contract_address,
                    ERC721MetadataFunctions.NAME,
                ),
            ]

        if ERC165InterfaceID.ERC721_ENUMERABLE in supported_interfaces:
            calls += [
                EthCall(
                    "total_supply",
                    None,
                    contract_address,
                    ERC721EnumerableFunctions.TOTAL_SUPPLY,
                ),
            ]

        name = None
        symbol = None
        total_supply = None
        if calls:
            with self.__stats_service.timer(self.RPC_TIMER_CALL_CONTRACT_METADATA):
                responses = await self.__rpc_client.calls(calls)

            if "name" in responses:
                name = (
                    responses["name"][0]
                    if not isinstance(responses["name"], RPCError)
                    else "#ERROR"
                )

            if "symbol" in responses:
                symbol = (
                    responses["symbol"][0]
                    if not isinstance(responses["symbol"], RPCError)
                    else "#ERROR"
                )
            if "total_supply" in responses:
                total_supply = (
                    responses["total_supply"][0]
                    if not isinstance(responses["total_supply"], RPCError)
                    else "#ERROR"
                )

        return name, symbol, total_supply


class ContractPersistenceProcessor(Processor):
    PROCESSOR_STOPPED_EVENT = "contract_persistence_stopped"
    PROCESSED_STAT = "contract_persistence_processed"
    DYNAMODB_TIMER_WRITE_CONTRACT = "dynamodb_write_contract"

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
            transport_objects = await self._get_batch_items_from_queue(
                self.__contract_queue, self.__dynamodb_batch_size, self.__max_batch_wait
            )
            if len(transport_objects) == 0:
                # Don't process empty batches
                continue

            with self.__stats_service.timer(self.DYNAMODB_TIMER_WRITE_CONTRACT):
                async with contracts.batch_writer() as batch:
                    for transport_object in transport_objects:
                        await batch.put_item(
                            Item={
                                "blockchain": "Ethereum Mainnet",
                                "address": transport_object.contract.address,
                                "block_number": transport_object.transaction.block_number.int_value,
                                "transaction_index": transport_object.transaction.transaction_index.int_value,
                                "transaction_hash": transport_object.transaction.hash,
                                "creator": transport_object.contract.creator,
                                "timestamp": transport_object.block.timestamp.int_value,
                                "name": transport_object.contract.name,
                                "symbol": transport_object.contract.symbol,
                                "total_supply": transport_object.contract.total_supply,
                                "interfaces": [
                                    i.value
                                    for i in transport_object.contract.interfaces
                                ],
                            }
                        )
                        self.__stats_service.increment(self.PROCESSED_STAT)
        await self.__event_bus.trigger(self.PROCESSOR_STOPPED_EVENT)
