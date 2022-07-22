import asyncio
from asyncio import Queue, QueueEmpty
from datetime import datetime, timedelta
from typing import Optional, List, Union

from eth_abi import decode
from eth_utils import decode_hex

from .events import EventBus
from .stats import StatsService
from ..data.models import Contracts, TokenTransfers
from ..web3.rpc import RPCClient, RPCError, EthCall
from ..web3.types import (
    TransactionReceipt,
    Transaction,
    Block,
    Contract,
    ERC165InterfaceID,
)
from ..web3.util import (
    ERC165Functions,
    contract_implements_function,
    ERC721MetadataFunctions,
    ERC721EnumerableFunctions,
    ERC721Functions,
)


class ProcessorException(Exception):
    pass


class TransportObject:
    pass


class BlockIDTransportObject(TransportObject):
    def __init__(self, block_id: int) -> None:
        self.__block_id: int = block_id

    @property
    def block_id(self) -> int:
        return self.__block_id


class ContractTransportObject(TransportObject):
    def __init__(
        self,
        *,
        block: Optional[Block] = None,
        transaction: Optional[Transaction] = None,
        transaction_receipt: Optional[TransactionReceipt] = None,
        contract: Optional[Contract] = None,
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


class TokenTransportObject(TransportObject):
    def __init__(
        self,
        *,
        block: Optional[Block] = None,
        transaction: Optional[Transaction] = None,
        transaction_receipt: Optional[TransactionReceipt] = None,
    ) -> None:
        self.__block: Optional[Block] = block
        self.__transaction: Optional[Transaction] = transaction
        self.__transaction_receipt: Optional[TransactionReceipt] = transaction_receipt

    @property
    def block(self) -> Optional[Block]:
        return self.__block

    @property
    def transaction(self) -> Optional[Transaction]:
        return self.__transaction

    @property
    def transaction_receipt(self) -> Optional[TransactionReceipt]:
        return self.__transaction_receipt


class Processor:
    def __init__(
        self,
        inbound_queue: Queue,
        max_batch_size: int,
        max_batch_wait: float,
        event_bus: EventBus,
        stopped_event: str,
    ) -> None:
        self._inbound_queue: Queue = inbound_queue
        self._max_batch_size: int = max_batch_size
        self._max_batch_wait: timedelta = timedelta(seconds=max_batch_wait)
        self.__event_bus: EventBus = event_bus
        self.__stopped_event = stopped_event
        self.__running: bool = False
        self.__stopping = False

    async def start(self):
        self.__running = True
        await self.__process()

    async def abort(self):
        self.__running = False

    async def stop(self):
        self.__stopping = True

    async def is_running(self):
        return self.__running

    async def is_stopping(self):
        return self.__stopping

    async def __full_stop(self):
        self.__running = False

    async def __process(self):
        while await self.is_running():
            transport_objects = await self.__get_batch_items_from_queue(
                self._inbound_queue, self._max_batch_size, self._max_batch_wait
            )
            if len(transport_objects) == 0:
                # Don't process empty batches
                continue
            await self._process_batch(transport_objects)
        await self.__event_bus.trigger(self.__stopped_event)

    async def __get_batch_items_from_queue(
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

    async def _process_batch(self, transport_objects: List[TransportObject]):
        raise NotImplementedError


class BlockProcessor(Processor):
    """
    Block processor processes block IDs in the block_id_queue passing the blocks to
    the transaction_queue until a "None" ID is received. Calls to get blocks wil be
    in batches specified by rpc_batch_size unless the max_batch_wait is exceeded
    which will process all bloc IDs received up to that point.
    """

    PROCESSED_STAT = "blocks_processed"
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
        super().__init__(
            inbound_queue=block_id_queue,
            max_batch_size=rpc_batch_size,
            max_batch_wait=max_batch_wait,
            event_bus=event_bus,
            stopped_event=self.PROCESSOR_STOPPED_EVENT,
        )
        self.__rpc_client: RPCClient = rpc_client
        self.__stats_service: StatsService = stats_service
        self.__transaction_queue: Queue = transaction_queue
        self.__token_transfer_method_hashes = [
            key for key in TokenProcessor.FUNCTION_HASH_LOOKUP.keys()
        ]

    async def _process_batch(self, transport_objects: List[BlockIDTransportObject]):
        block_ids = [transport_object.block_id for transport_object in transport_objects]
        with self.__stats_service.timer(self.RPC_TIMER_GET_BLOCKS):
            try:
                blocks = await self.__rpc_client.get_blocks(set(block_ids), full_transactions=True)
            except RPCError:
                raise  # TODO: handle retries

        for block in blocks:
            for transaction in block.transactions:
                out_transport_object = None
                if transaction.to_ is None:
                    # No to address is indicative of contract creation
                    # If there is no to address,
                    # create the Contract transport object and send it on to be processed
                    # by the transaction processor
                    out_transport_object = ContractTransportObject(
                        block=block, transaction=transaction
                    )
                if transaction.input[:10] in self.__token_transfer_method_hashes:
                    # The first 4 bytes of hex in a contract method call are the ABI method hash
                    # If the first 4 bytes of the transaction input match
                    # a token transfer method hash, create the Token transport object and send
                    # it on to be processed buy the transaction processor
                    out_transport_object = TokenTransportObject(
                        block=block, transaction=transaction
                    )

                if out_transport_object:
                    await self.__transaction_queue.put(out_transport_object)
            self.__stats_service.increment(self.PROCESSED_STAT)


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
        token_queue: Queue,
        max_batch_wait: int,
    ) -> None:
        super().__init__(
            inbound_queue=transaction_queue,
            max_batch_size=rpc_batch_size,
            max_batch_wait=max_batch_wait,
            event_bus=event_bus,
            stopped_event=self.PROCESSOR_STOPPED_EVENT,
        )
        self.__rpc_client: RPCClient = rpc_client
        self.__stats_service: StatsService = stats_service
        self.__contract_queue: Queue = contract_queue
        self.__token_queue: Queue = token_queue

    async def _process_batch(
        self,
        transport_objects: List[Union[ContractTransportObject, TokenTransportObject]],
    ):
        transaction_hashes = list()
        transactions_hash_map = dict()
        for in_transport_object in transport_objects:
            transactions_hash_map[in_transport_object.transaction.hash] = in_transport_object
            transaction_hashes.append(in_transport_object.transaction.hash)
        with self.__stats_service.timer(self.RPC_TIMER_GET_TRANSACTION_RECEIPTS):
            receipts = await self.__rpc_client.get_transaction_receipts(transaction_hashes)
        for receipt in receipts:
            in_transport_object = transactions_hash_map[receipt.transaction_hash]
            if isinstance(in_transport_object, ContractTransportObject):
                out_transport_object_class = ContractTransportObject
                queue: Queue = self.__contract_queue
            elif isinstance(in_transport_object, TokenTransportObject):
                out_transport_object_class = TokenTransportObject
                queue: Queue = self.__token_queue
            else:
                raise ValueError(
                    "Unexpected transport object type {}".format(in_transport_object.__class__)
                )
            out_transport_object = out_transport_object_class(
                block=in_transport_object.block,
                transaction=in_transport_object.transaction,
                transaction_receipt=receipt,
            )
            await queue.put(out_transport_object)
            self.__stats_service.increment(self.PROCESSED_STAT)


class ContractProcessor(Processor):
    RPC_TIMER_CALL_SUPPORTS_INTERFACES = "rpc_timer_call_supports_interfaces"
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
        super().__init__(
            inbound_queue=contract_queue,
            max_batch_size=rpc_batch_size,
            max_batch_wait=max_batch_wait,
            event_bus=event_bus,
            stopped_event=self.PROCESSOR_STOPPED_EVENT,
        )
        self.__rpc_client: RPCClient = rpc_client
        self.__stats_service: StatsService = stats_service
        self.__persistence_queue: Queue = persistence_queue

    async def _process_batch(self, transport_objects: List[ContractTransportObject]):
        for in_transport_object in transport_objects:
            contract_implements_supports_interface = contract_implements_function(
                in_transport_object.transaction.input,
                ERC165Functions.SUPPORTS_INTERFACE.function_hash[:],
            )

            if contract_implements_supports_interface:
                contract_id = in_transport_object.transaction_receipt.contract_address
                supported_interfaces = await self.__get_supported_interfaces(contract_id)
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

    async def __get_supported_interfaces(self, contract_address) -> List[ERC165InterfaceID]:
        with self.__stats_service.timer(self.RPC_TIMER_CALL_SUPPORTS_INTERFACES):
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
    PROCESSED_STAT = "contracts_persisted"
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
        super().__init__(
            inbound_queue=contract_queue,
            max_batch_size=dynamodb_batch_size,
            max_batch_wait=max_batch_wait,
            event_bus=event_bus,
            stopped_event=self.PROCESSOR_STOPPED_EVENT,
        )
        self.__dynamodb = dynamodb
        self.__stats_service: StatsService = stats_service

    async def _process_batch(self, transport_objects: List[ContractTransportObject]):
        contracts = await self.__dynamodb.Table(Contracts.table_name)
        with self.__stats_service.timer(self.DYNAMODB_TIMER_WRITE_CONTRACT):
            async with contracts.batch_writer() as batch:
                for transport_object in transport_objects:

                    address = transport_object.contract.address
                    block_number = transport_object.transaction.block_number.int_value
                    transaction_index = transport_object.transaction.transaction_index.int_value
                    transaction_hash = transport_object.transaction.hash
                    creator = transport_object.contract.creator
                    timestamp = transport_object.block.timestamp.int_value
                    name = transport_object.contract.name
                    symbol = transport_object.contract.symbol
                    total_supply = transport_object.contract.total_supply
                    interfaces = [i.value for i in transport_object.contract.interfaces]
                    await batch.put_item(
                        Item={
                            "blockchain": "Ethereum Mainnet",
                            "address": address,
                            "block_number": block_number,
                            "transaction_index": transaction_index,
                            "transaction_hash": transaction_hash,
                            "creator": creator,
                            "timestamp": timestamp,
                            "name": name,
                            "symbol": symbol,
                            "total_supply": total_supply,
                            "interfaces": interfaces,
                        }
                    )
                    self.__stats_service.increment(self.PROCESSED_STAT)


class TokenProcessor(Processor):
    PROCESSOR_STOPPED_EVENT = "token_processor_stopped"
    PROCESSED_STAT = "tokens_processed"
    DYNAMODB_TIMER_WRITE_TOKEN_TRANSFER = "dynamodb_write_token_transfer"

    FUNCTION_HASH_LOOKUP = dict()
    for function in [
        ERC721Functions.SAFE_TRANSFER_FROM_WITH_DATA,
        ERC721Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA,
        ERC721Functions.TRANSFER_FROM,
        # Not handling ERC1155 for now
        # ERC1155Functions.SAFE_BATCH_TRANSFER_FROM,
        # ERC1155Functions.SAFE_TRANSFER_FROM_WITH_DATA,
        # ERC1155Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA,
    ]:
        FUNCTION_HASH_LOOKUP[function.function_hash] = function

    def __init__(
        self,
        dynamodb,
        stats_service: StatsService,
        event_bus: EventBus,
        token_queue: Queue,
        dynamodb_batch_size: int,
        max_batch_wait: int,
    ) -> None:
        super().__init__(
            inbound_queue=token_queue,
            max_batch_size=dynamodb_batch_size,
            max_batch_wait=max_batch_wait,
            event_bus=event_bus,
            stopped_event=self.PROCESSOR_STOPPED_EVENT,
        )
        self.__dynamodb = dynamodb
        self.__stats_service: StatsService = stats_service

    async def _process_batch(self, transport_objects: List[ContractTransportObject]):
        transfers = await self.__dynamodb.Table(TokenTransfers.table_name)
        with self.__stats_service.timer(self.DYNAMODB_TIMER_WRITE_TOKEN_TRANSFER):
            async with transfers.batch_writer() as batch:
                for transport_object in transport_objects:
                    function_hash = transport_object.transaction.input[:10]
                    if function_hash not in self.FUNCTION_HASH_LOOKUP:
                        raise ProcessorException(
                            f"Transaction {transport_object.transaction.hash} "
                            f"has unexpected function hash {function_hash}"
                        )

                    function = self.FUNCTION_HASH_LOOKUP[function_hash]
                    function_data = transport_object.transaction.input[10:]

                    try:
                        parameters = decode(function.param_types, decode_hex(function_data))
                    except Exception as e:
                        # There are occasional bad transactions in the chain,
                        # print them for now and continue
                        print(
                            "\n\n\nException processing function params: "
                            "{}\nFunction: {}\nData: {}\n\n\n".format(
                                e, function.description, function_data
                            )
                        )
                    from_address = None
                    if function is ERC721Functions.SAFE_TRANSFER_FROM_WITH_DATA:
                        from_address, to_address, token_id, _ = parameters
                    if function is ERC721Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA:
                        from_address, to_address, token_id = parameters
                    if function is ERC721Functions.TRANSFER_FROM:
                        from_address, to_address, token_id = parameters
                    # TODO: Not processing ERC1155 until we figure out how
                    #       to handle the mess they leave behind
                    # if function is ERC1155Functions.SAFE_BATCH_TRANSFER_FROM:
                    #     (
                    #         from_address,
                    #         to_address,
                    #         token_types,
                    #         token_ids,
                    #         _,
                    #     ) = parameters
                    # if function is ERC1155Functions.SAFE_TRANSFER_FROM_WITH_DATA:
                    #     from_address, to_address, token_type, token_id, _ = parameters
                    # if function is ERC1155Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA:
                    #     from_address, to_address, token_type, token_id = parameters
                    transaction_hash = transport_object.transaction_receipt.transaction_hash
                    collection_id = transport_object.transaction_receipt.to_
                    timestamp = transport_object.block.timestamp.int_value
                    await batch.put_item(
                        Item={
                            "blockchain": "Ethereum Mainnet",
                            "transaction_hash": transaction_hash,
                            "collection_id": collection_id,
                            "token_id": str(token_id),
                            "from": from_address,
                            "to": to_address,
                            "timestamp": timestamp,
                        }
                    )
                    self.__stats_service.increment(self.PROCESSED_STAT)
