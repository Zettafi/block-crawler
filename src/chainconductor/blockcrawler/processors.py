import asyncio
import re
from asyncio import Queue, QueueEmpty
from datetime import datetime, timedelta
from typing import Optional, List, Union, Set

from eth_abi import decode
from eth_abi.encoding import encode_uint_256
from eth_abi.exceptions import ValueOutOfBounds
from eth_bloom import BloomFilter
from eth_hash.auto import keccak
from eth_utils import decode_hex

from .data_clients import (
    IpfsBatchDataClient,
    HttpBatchDataClient,
    BatchDataClient,
    ProtocolError,
    ArweaveBatchDataClient,
)
from .events import EventBus
from .stats import StatsService
from ..data.models import Collections, TokenTransfers, Tokens
from ..web3.rpc import RPCClient, RPCError, EthCall
from ..web3.types import (
    TransactionReceipt,
    Transaction,
    Block,
    Contract,
    ERC165InterfaceID,
    HexInt,
)
from ..web3.util import (
    ERC165Functions,
    contract_implements_function,
    ERC721MetadataFunctions,
    ERC721EnumerableFunctions,
    ERC721Events,
    ERC1155MetadataURIFunctions,
)


class ProcessorException(Exception):  # pragma: no cover
    pass


class TransportObject:  # pragma: no cover
    pass


class BlockIDTransportObject(TransportObject):  # pragma: no cover
    def __init__(self, block_id: int) -> None:
        self.__block_id: int = block_id

    @property
    def block_id(self) -> int:
        return self.__block_id


class ContractTransportObject(TransportObject):  # pragma: no cover
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


class Token:  # pragma: no cover
    def __init__(
        self,
        *,
        collection_id: str,
        original_owner: str,
        token_id: HexInt,
        timestamp: HexInt,
        metadata_uri: str = None,
        metadata: str = None,
    ) -> None:
        self.__collection_id = collection_id
        self.__original_owner = original_owner
        self.__token_id = token_id
        self.__timestamp = timestamp
        self.__metadata_uri = metadata_uri
        self.__metadata = metadata

    @property
    def collection_id(self) -> str:
        return self.__collection_id

    @property
    def original_owner(self) -> str:
        return self.__original_owner

    @property
    def token_id(self) -> HexInt:
        return self.__token_id

    @property
    def timestamp(self) -> HexInt:
        return self.__timestamp

    @property
    def metadata_uri(self):
        return self.__metadata_uri

    @property
    def metadata(self):
        return self.__metadata

    def __repr__(self):
        return (
            self.__class__.__name__
            + {
                "collection_id": self.collection_id,
                "token_id": self.token_id,
                "original_owner": self.__original_owner,
                "timestamp": self.timestamp,
                "metadata_uri": self.metadata_uri,
                "metadata": self.__metadata,
            }.__repr__()
        )

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.collection_id == other.collection_id
            and self.token_id == other.token_id
            and self.original_owner == other.original_owner
            and self.timestamp == other.timestamp
            and self.metadata_uri == other.metadata_uri
            and self.metadata == other.metadata
        )


class TokenTransportObject(TransportObject):  # pragma: no cover
    def __init__(
        self,
        *,
        block: Optional[Block] = None,
        transaction: Optional[Transaction] = None,
        transaction_receipt: Optional[TransactionReceipt] = None,
        token: Optional[Token] = None,
    ) -> None:
        self.__block: Optional[Block] = block
        self.__transaction: Optional[Transaction] = transaction
        self.__transaction_receipt: Optional[TransactionReceipt] = transaction_receipt
        self.__token: Optional[Token] = token

    def __repr__(self):
        return (
            str(self.__class__.__name__)
            + {
                "block": self.__block,
                "transaction": self.__transaction,
                "transaction_receipt": self.__transaction_receipt,
                "token": self.__token,
            }.__repr__()
        )

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.block == other.block
            and self.transaction == other.transaction
            and self.transaction_receipt == other.transaction_receipt
            and self.token == other.token
        )

    def __hash__(self):
        hash_ = (
            hash(self.__class__.__name__) * hash(self.__block.hash)
            if self.__block
            else 1 * hash(self.__transaction.hash)
            if self.__transaction
            else 1 * hash(self.__transaction_receipt.transaction_hash)
            if self.__transaction_receipt
            else 1
            * (
                hash(self.__token.collection_id)
                if self.__token
                else 1 + hash(self.__token.token_id.hex_value)
                if self.__token and self.__token.token_id
                else 0
            )
        )
        return hash_

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
    def token(self) -> Optional[Token]:
        return self.__token


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
        inbound_queue: Queue,
        transaction_queue: Queue,
        max_batch_wait: int,
    ) -> None:
        super().__init__(
            inbound_queue=inbound_queue,
            max_batch_size=rpc_batch_size,
            max_batch_wait=max_batch_wait,
            event_bus=event_bus,
            stopped_event=self.PROCESSOR_STOPPED_EVENT,
        )
        self.__rpc_client: RPCClient = rpc_client
        self.__stats_service: StatsService = stats_service
        self.__transaction_queue: Queue = transaction_queue
        self.__transfer_event_hash = decode_hex(ERC721Events.TRANSFER.event_hash)

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
                elif self.__transfer_event_hash in BloomFilter(int(block.logs_bloom, 16)):
                    # If the event hash is in the block's bloom filter, the block may
                    # have a transaction log with the event
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
        inbound_queue: Queue,
        outbound_contract_queue: Queue,
        outbound_token_queue: Queue,
        max_batch_wait: int,
    ) -> None:
        super().__init__(
            inbound_queue=inbound_queue,
            max_batch_size=rpc_batch_size,
            max_batch_wait=max_batch_wait,
            event_bus=event_bus,
            stopped_event=self.PROCESSOR_STOPPED_EVENT,
        )
        self.__rpc_client: RPCClient = rpc_client
        self.__stats_service: StatsService = stats_service
        self.__contract_queue: Queue = outbound_contract_queue
        self.__token_queue: Queue = outbound_token_queue
        self.__transfer_event_hash = decode_hex(ERC721Events.TRANSFER.event_hash)

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
                queue = self.__contract_queue
            elif isinstance(in_transport_object, TokenTransportObject):
                if self.__transfer_event_hash in BloomFilter(int(receipt.logs_bloom, 16)):
                    out_transport_object_class = TokenTransportObject
                    queue = self.__token_queue
                else:
                    out_transport_object_class = None
                    queue = None
            else:
                raise ValueError(
                    "Unexpected transport object type {}".format(in_transport_object.__class__)
                )
            if queue:
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
        inbound_queue: Queue,
        outbound_queue: Queue,
        max_batch_wait: int,
    ) -> None:
        super().__init__(
            inbound_queue=inbound_queue,
            max_batch_size=rpc_batch_size,
            max_batch_wait=max_batch_wait,
            event_bus=event_bus,
            stopped_event=self.PROCESSOR_STOPPED_EVENT,
        )
        self.__rpc_client: RPCClient = rpc_client
        self.__stats_service: StatsService = stats_service
        self.__persistence_queue: Queue = outbound_queue

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


class CollectionPersistenceProcessor(Processor):
    PROCESSOR_STOPPED_EVENT = "collection_persistence_stopped"
    PROCESSED_STAT = "collection_persisted"
    DYNAMODB_TIMER_WRITE_CONTRACTS = "dynamodb_write_collections"

    def __init__(
        self,
        dynamodb,
        stats_service: StatsService,
        event_bus: EventBus,
        inbound_queue: Queue,
        dynamodb_batch_size: int,
        max_batch_wait: int,
    ) -> None:
        super().__init__(
            inbound_queue=inbound_queue,
            max_batch_size=dynamodb_batch_size,
            max_batch_wait=max_batch_wait,
            event_bus=event_bus,
            stopped_event=self.PROCESSOR_STOPPED_EVENT,
        )
        self.__dynamodb = dynamodb
        self.__stats_service: StatsService = stats_service

    async def _process_batch(self, transport_objects: List[ContractTransportObject]):
        collections = await self.__dynamodb.Table(Collections.table_name)
        with self.__stats_service.timer(self.DYNAMODB_TIMER_WRITE_CONTRACTS):
            async with collections.batch_writer() as batch:
                for transport_object in transport_objects:
                    collection_id = transport_object.contract.address
                    block_number = transport_object.transaction_receipt.block_number.int_value
                    transaction_index = (
                        transport_object.transaction_receipt.transaction_index.int_value
                    )
                    transaction_hash = transport_object.transaction_receipt.transaction_hash
                    creator = transport_object.contract.creator
                    timestamp = transport_object.block.timestamp.int_value
                    name = transport_object.contract.name
                    symbol = transport_object.contract.symbol
                    total_supply = transport_object.contract.total_supply
                    interfaces = [i.value for i in transport_object.contract.interfaces]
                    await batch.put_item(
                        Item={
                            "blockchain": "Ethereum Mainnet",
                            "collection_id": collection_id,
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


class TokenTransferProcessor(Processor):
    PROCESSOR_STOPPED_EVENT = "token_transfer_processor_stopped"
    PROCESSED_STAT = "token_transfers_processed"
    DYNAMODB_TIMER_WRITE_TOKEN_TRANSFERS = "dynamodb_write_token_transfers"
    TOPICS_DECODE_ERRORS = "token_transfers_log_topic_decode_error"

    def __init__(
        self,
        dynamodb,
        stats_service: StatsService,
        event_bus: EventBus,
        inbound_queue: Queue,
        outbound_queue: Queue,
        dynamodb_batch_size: int,
        max_batch_wait: int,
    ) -> None:
        super().__init__(
            inbound_queue=inbound_queue,
            max_batch_size=dynamodb_batch_size,
            max_batch_wait=max_batch_wait,
            event_bus=event_bus,
            stopped_event=self.PROCESSOR_STOPPED_EVENT,
        )
        self.__dynamodb = dynamodb
        self.__outbound_queue: Queue = outbound_queue
        self.__stats_service: StatsService = stats_service

    async def _process_batch(self, transport_objects: List[ContractTransportObject]):
        transfers = await self.__dynamodb.Table(TokenTransfers.table_name)
        with self.__stats_service.timer(self.DYNAMODB_TIMER_WRITE_TOKEN_TRANSFERS):
            async with transfers.batch_writer() as batch:
                for transport_object in transport_objects:
                    for log in transport_object.transaction_receipt.logs:
                        if (
                            len(log.topics) == 4
                            and log.topics[0] == ERC721Events.TRANSFER.event_hash
                        ):
                            # ERC-20 transfer events only have 3 topics. ERC-721 transfer events
                            # have 4 topics. Process only logs with 4 topics.
                            try:
                                from_address = decode(["address"], decode_hex(log.topics[1]))[0]
                                to_address = decode(["address"], decode_hex(log.topics[2]))[0]
                                token_id = decode(["uint256"], decode_hex(log.topics[3]))[0]
                                collection_id = log.address
                            except Exception:
                                # There are occasional bad transactions in the chain,
                                # it's usually token IDs that are larger than 256 bits
                                self.__stats_service.increment(self.TOPICS_DECODE_ERRORS)
                                continue
                            timestamp = transport_object.block.timestamp
                            transaction_log_index_hash = keccak(
                                (
                                    log.block_number.hex_value
                                    + log.transaction_index.hex_value
                                    + log.log_index.hex_value
                                ).encode("utf8")
                            ).hex()
                            await batch.put_item(
                                Item={
                                    "blockchain": "Ethereum Mainnet",
                                    "transaction_log_index_hash": transaction_log_index_hash,
                                    "collection_id": collection_id,
                                    "token_id": str(token_id),
                                    "from": from_address,
                                    "to": to_address,
                                    "block": log.block_number.int_value,
                                    "transaction_index": log.transaction_index.int_value,
                                    "log_index": log.log_index.int_value,
                                    "timestamp": timestamp.int_value,
                                }
                            )
                            if (
                                # Transferring from the 0 or collection address
                                (from_address == collection_id or int(from_address, 16) == 0)
                                # and not to the collection address
                                and to_address != collection_id
                                # and not to the 0 address
                                and int(to_address, 16) != 0
                                # this is a minting transfer
                            ):
                                token_mint = Token(
                                    collection_id=collection_id,
                                    original_owner=from_address,
                                    token_id=HexInt(f"0x{token_id}"),
                                    timestamp=timestamp,
                                )
                                out_transport_object = TokenTransportObject(
                                    block=transport_object.block,
                                    transaction=transport_object.transaction,
                                    transaction_receipt=transport_object.transaction_receipt,
                                    token=token_mint,
                                )
                                await self.__outbound_queue.put(out_transport_object)

                            self.__stats_service.increment(self.PROCESSED_STAT)


class TokenMetadataProcessor(Processor):
    PROCESSOR_STOPPED_EVENT = "token_metadata_processor_stopped"
    PROCESSED_STAT = "token_metadata_processed"
    RPC_GET_TOKEN_METADATA_URI = "rpc_call_token_metadata_uri"
    TOKEN_METADATA_INVALID_TOKEN_ID = "token_metadata_invalid_token_id"

    def __init__(
        self,
        rpc_client: RPCClient,
        stats_service: StatsService,
        event_bus: EventBus,
        rpc_batch_size: int,
        inbound_queue: Queue,
        outbound_token_gathering_queue: Queue,
        outbound_token_persistence_queue: Queue,
        max_batch_wait: float,
    ) -> None:
        super().__init__(
            inbound_queue,
            rpc_batch_size // 2,  # We'll be making 2 queries for every entry
            max_batch_wait,
            event_bus,
            self.PROCESSOR_STOPPED_EVENT,
        )
        self.__rpc_client = rpc_client
        self.__rpc_batch_size = rpc_batch_size
        self.__stats_service = stats_service
        self.__outbound_gathering_queue = outbound_token_gathering_queue
        self.__outbound_persistence_queue = outbound_token_persistence_queue

    async def _process_batch(self, transport_objects: List[TokenTransportObject]):
        def _get_721_id_for_token(token):
            return f"{token.token_id}-721"

        def _get_1155_id_for_token(token):
            return f"{token.token_id}-1155"

        requests = list()
        for transport_object in transport_objects:
            token_id = transport_object.token.token_id.int_value
            try:
                encode_uint_256.validate_value(token_id)
            except ValueOutOfBounds:
                self.__stats_service.increment(self.TOKEN_METADATA_INVALID_TOKEN_ID)
                continue
            requests.append(
                EthCall(
                    _get_721_id_for_token(transport_object.token),
                    None,
                    transport_object.token.collection_id,
                    ERC721MetadataFunctions.TOKEN_URI,
                    [token_id],
                )
            )
            requests.append(
                EthCall(
                    _get_1155_id_for_token(transport_object.token),
                    None,
                    transport_object.token.collection_id,
                    ERC1155MetadataURIFunctions.URI,
                    [token_id],
                )
            )

        if requests:
            with self.__stats_service.timer(self.RPC_GET_TOKEN_METADATA_URI):
                responses = await self.__rpc_client.calls(requests)

            for transport_object in transport_objects:
                metadata_uri = None
                for get_id in [_get_1155_id_for_token, _get_721_id_for_token]:
                    try:
                        response = responses[get_id(transport_object.token)]
                    except KeyError:
                        continue
                    if not isinstance(response, RPCError):
                        metadata_uri = response[0]
                        break

                out_transport_object = TokenTransportObject(
                    block=transport_object.block,
                    transaction=transport_object.transaction,
                    transaction_receipt=transport_object.transaction_receipt,
                    token=Token(
                        collection_id=transport_object.token.collection_id,
                        original_owner=transport_object.token.original_owner,
                        token_id=transport_object.token.token_id,
                        timestamp=transport_object.token.timestamp,
                        metadata_uri=metadata_uri,
                    ),
                )
                if metadata_uri:
                    await self.__outbound_gathering_queue.put(out_transport_object)
                else:
                    await self.__outbound_persistence_queue.put(out_transport_object)
                self.__stats_service.increment(self.PROCESSED_STAT)
        else:
            for transport_object in transport_objects:
                await self.__outbound_persistence_queue.put(transport_object)


class TokenMetadataGatheringProcessor(Processor):
    PROCESSOR_STOPPED_EVENT = "token_metadata_gathering_processor_stopped"
    PROCESSED_STAT = "token_metadata_gathering_processed"
    PROTOCOL_MATCH_REGEX = re.compile("^([^:]+)://.+")
    METADATA_RETRIEVAL_HTTP_TIMER = "metadata_retrieval_http"
    METADATA_RETRIEVAL_HTTP_ERROR_STAT = "metadata_retrieval_http_error"
    METADATA_RETRIEVAL_IPFS_TIMER = "metadata_retrieval_ipfs"
    METADATA_RETRIEVAL_IPFS_ERROR_STAT = "metadata_retrieval_ipfs_error"
    METADATA_RETRIEVAL_ARWEAVE_TIMER = "metadata_retrieval_arweave"
    METADATA_RETRIEVAL_ARWEAVE_ERROR_STAT = "metadata_retrieval_arweave_error"
    METADATA_RETRIEVAL_UNSUPPORTED_PROTOCOL_STAT = "metadata_retrieval_unsupported_protocol"

    def __init__(
        self,
        stats_service: StatsService,
        event_bus: EventBus,
        http_batch_size: int,
        inbound_queue: Queue,
        outbound_queue: Queue,
        http_batch_client: HttpBatchDataClient,
        ipfs_batch_client: IpfsBatchDataClient,
        arweave_batch_client: ArweaveBatchDataClient,
        max_batch_wait: float,
    ) -> None:
        super().__init__(
            inbound_queue,
            http_batch_size,
            max_batch_wait,
            event_bus,
            self.PROCESSOR_STOPPED_EVENT,
        )
        self.__stats_service = stats_service
        self.__outbound_queue = outbound_queue
        self.__http_batch_client = http_batch_client
        self.__ipfs_batch_client = ipfs_batch_client
        self.__arweave_batch_client = arweave_batch_client

    async def _process_batch(self, transport_objects: List[TokenTransportObject]):
        http_uris = set()
        ipfs_uris = set()
        arweave_uris = set()
        tto_uri_map = dict()
        coroutines = list()
        for transport_object in transport_objects:
            uri = transport_object.token.metadata_uri
            proto = self.__get_protocol_from_uri(uri)

            tto_uri_map[transport_object] = uri
            if proto in ("https", "http"):
                http_uris.add(uri)
            elif proto == "ipfs":
                ipfs_uris.add(uri)
            elif proto == "ar":
                arweave_uris.add(uri)
            else:
                self.__stats_service.increment(self.METADATA_RETRIEVAL_UNSUPPORTED_PROTOCOL_STAT)
                await self.__outbound_queue.put(transport_object)
                self.__stats_service.increment(self.PROCESSED_STAT)

        if http_uris:
            coroutines.append(
                self.__timed_batch_client_get(
                    self.METADATA_RETRIEVAL_HTTP_TIMER, self.__http_batch_client, http_uris
                )
            )
        if ipfs_uris:
            coroutines.append(
                self.__timed_batch_client_get(
                    self.METADATA_RETRIEVAL_IPFS_TIMER, self.__ipfs_batch_client, ipfs_uris
                )
            )
        if arweave_uris:
            coroutines.append(
                self.__timed_batch_client_get(
                    self.METADATA_RETRIEVAL_ARWEAVE_TIMER, self.__arweave_batch_client, arweave_uris
                )
            )

        if coroutines:
            responses = await asyncio.gather(*coroutines)
            for response in responses:
                for transport_object in transport_objects:
                    if transport_object in tto_uri_map:
                        uri = tto_uri_map[transport_object]
                        if uri in response:
                            uri_response = response[uri]
                            if isinstance(uri_response, ProtocolError):
                                out_transport_object = transport_object
                                proto = self.__get_protocol_from_uri(uri)
                                if proto in ("http", "https"):
                                    self.__stats_service.increment(
                                        self.METADATA_RETRIEVAL_HTTP_ERROR_STAT
                                    )
                                elif proto == "ipfs":
                                    self.__stats_service.increment(
                                        self.METADATA_RETRIEVAL_IPFS_ERROR_STAT
                                    )
                                elif proto == "ar":
                                    self.__stats_service.increment(
                                        self.METADATA_RETRIEVAL_ARWEAVE_ERROR_STAT
                                    )
                            else:
                                out_transport_object = TokenTransportObject(
                                    block=transport_object.block,
                                    transaction=transport_object.transaction,
                                    transaction_receipt=transport_object.transaction_receipt,
                                    token=Token(
                                        token_id=transport_object.token.token_id,
                                        collection_id=transport_object.token.collection_id,
                                        original_owner=transport_object.token.original_owner,
                                        timestamp=transport_object.token.timestamp,
                                        metadata_uri=transport_object.token.metadata_uri,
                                        metadata=response[uri],
                                    ),
                                )
                            await self.__outbound_queue.put(out_transport_object)
                            self.__stats_service.increment(self.PROCESSED_STAT)

    def __get_protocol_from_uri(self, uri):
        match = self.PROTOCOL_MATCH_REGEX.fullmatch(uri)
        return match.group(1).lower() if match else None

    async def __timed_batch_client_get(
        self, timer: str, batch_client: BatchDataClient, uris: Set[str]
    ):
        with self.__stats_service.timer(timer):
            return await batch_client.get(uris)


class TokenPersistenceProcessor(Processor):
    PROCESSOR_STOPPED_EVENT = "token_persistence_processor_stopped"
    PROCESSED_STAT = "tokens_persisted"
    DYNAMODB_TIMER_WRITE_TOKENS = "dynamodb_write_tokens"

    def __init__(
        self,
        dynamodb,
        stats_service: StatsService,
        event_bus: EventBus,
        inbound_queue: Queue,
        dynamodb_batch_size: int,
        max_batch_wait: float,
    ) -> None:
        super().__init__(
            inbound_queue,
            dynamodb_batch_size,
            max_batch_wait,
            event_bus,
            self.PROCESSOR_STOPPED_EVENT,
        )
        self.__stats_service = stats_service
        self.__dynamodb = dynamodb

    async def _process_batch(self, transport_objects: List[TokenTransportObject]):
        table = await self.__dynamodb.Table(Tokens.table_name)
        with self.__stats_service.timer(self.DYNAMODB_TIMER_WRITE_TOKENS):
            async with table.batch_writer() as batch_writer:
                for transport_object in transport_objects:
                    blockchain = "Ethereum Mainnet"
                    collection_id = transport_object.token.collection_id
                    blockchain_collection_id = keccak(
                        (blockchain + collection_id).encode("utf8")
                    ).hex()
                    timestamp = transport_object.token.timestamp.int_value
                    original_owner = transport_object.token.original_owner
                    token_id = str(transport_object.token.token_id.int_value)
                    metadata_uri = transport_object.token.metadata_uri
                    metadata = transport_object.token.metadata
                    await batch_writer.put_item(
                        Item={
                            "blockchain_collection_id": blockchain_collection_id,
                            "token_id": token_id,
                            "blockchain": blockchain,
                            "collection_id": collection_id,
                            "original_owner": original_owner,
                            "mint_timestamp": timestamp,
                            "metadata_uri": metadata_uri,
                            "metadata": metadata,
                        }
                    )
                    self.__stats_service.increment(self.PROCESSED_STAT)
