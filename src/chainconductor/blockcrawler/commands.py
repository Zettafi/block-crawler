import asyncio
from asyncio import Queue
from typing import List

import aioboto3
from botocore.exceptions import ClientError

from .data_clients import HttpBatchDataClient, IpfsBatchDataClient, ArweaveBatchDataClient
from .events import EventBus
from .processors import (
    BlockProcessor,
    TransactionProcessor,
    CollectionPersistenceProcessor,
    Processor,
    ContractProcessor,
    BlockIDTransportObject,
    TokenTransferProcessor,
    TokenMetadataProcessor,
    TokenPersistenceProcessor,
    TokenMetadataGatheringProcessor,
)
from .stats import StatsService
from ..data.models import Collections, TokenTransfers, Tokens
from ..web3.rpc import RPCClient

try:  # If dotenv in installed, use it load env vars
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


class RunManager:
    BLOCK_ADDING_ENDED_EVENT = "block adding ended"

    def __init__(
        self,
        event_bus: EventBus,
        block_processors: List[BlockProcessor],
        transaction_processors: List[TransactionProcessor],
        contract_processors: List[ContractProcessor],
        contract_persistence_processors: List[CollectionPersistenceProcessor],
        token_transfer_processors: List[TokenTransferProcessor],
        token_metadata_processors: List[TokenMetadataProcessor],
        token_metadata_gathering_processors: List[TokenMetadataGatheringProcessor],
        token_persistence_processors: List[TokenPersistenceProcessor],
    ) -> None:

        self.__event_bus: EventBus = event_bus
        self.__processors: List[Processor] = list()

        self.__block_processors: List[BlockProcessor] = block_processors
        self.__block_processors_remaining: int = len(block_processors)
        self.__processors.extend(self.__block_processors)

        self.__transaction_processors: List[TransactionProcessor] = transaction_processors
        self.__transaction_processors_remaining: int = len(transaction_processors)
        self.__processors.extend(self.__transaction_processors)

        self.__contract_processors: List[ContractProcessor] = contract_processors
        self.__contract_processors_remaining: int = len(contract_processors)
        self.__processors.extend(self.__contract_processors)

        self.__persistence_processors: List[
            CollectionPersistenceProcessor
        ] = contract_persistence_processors
        self.__contract_persistence_processors_remaining: int = len(contract_persistence_processors)
        self.__processors.extend(self.__persistence_processors)

        self.__token_transfer_processors: List[TokenTransferProcessor] = token_transfer_processors
        self.__token_transfer_processors_remaining: int = len(token_transfer_processors)
        self.__processors.extend(self.__token_transfer_processors)

        self.__token_metadata_processors: List[TokenMetadataProcessor] = token_metadata_processors
        self.__token_metadata_processors_remaining: int = len(token_metadata_processors)
        self.__processors.extend(self.__token_metadata_processors)

        self.__token_metadata_gathering_processors: List[
            TokenMetadataGatheringProcessor
        ] = token_metadata_gathering_processors
        self.__token_metadata_gathering_processors_remaining: int = len(
            token_metadata_gathering_processors
        )
        self.__processors.extend(self.__token_metadata_gathering_processors)

        self.__token_persistence_processors: List[
            TokenPersistenceProcessor
        ] = token_persistence_processors
        self.__token_persistence_processors_remaining: int = len(token_persistence_processors)
        self.__processors.extend(self.__token_persistence_processors)

    async def __register_process_stop_events(self):
        await self.__event_bus.register(
            self.BLOCK_ADDING_ENDED_EVENT, self.__block_id_processor_stopped
        )
        await self.__event_bus.register(
            BlockProcessor.PROCESSOR_STOPPED_EVENT, self.__block_processor_stopped
        )
        await self.__event_bus.register(
            TransactionProcessor.PROCESSOR_STOPPED_EVENT,
            self.__transaction_processor_stopped,
        )

        await self.__event_bus.register(
            ContractProcessor.PROCESSOR_STOPPED_EVENT, self.__contract_processor_stopped
        )

        await self.__event_bus.register(
            TokenTransferProcessor.PROCESSOR_STOPPED_EVENT, self.__token_transfer_processor_stopped
        )

        await self.__event_bus.register(
            TokenMetadataProcessor.PROCESSOR_STOPPED_EVENT, self.__token_metadata_processor_stopped
        )

        await self.__event_bus.register(
            TokenMetadataGatheringProcessor.PROCESSOR_STOPPED_EVENT,
            self.__token_metadata_gathering_processor_stopped,
        )

    async def __block_id_processor_stopped(self):
        for block_processor in self.__block_processors:
            await block_processor.stop()

    async def __block_processor_stopped(self):
        self.__block_processors_remaining -= 1
        if self.__block_processors_remaining < 1:
            for processor in self.__transaction_processors:
                await processor.stop()

    async def __transaction_processor_stopped(self):
        self.__transaction_processors_remaining -= 1
        if self.__transaction_processors_remaining < 1:
            for contract_processor in self.__contract_processors:
                await contract_processor.stop()
            for processor in self.__token_transfer_processors:
                await processor.stop()

    async def __contract_processor_stopped(self):
        self.__contract_processors_remaining -= 1
        if self.__contract_processors_remaining < 1:
            for processor in self.__persistence_processors:
                await processor.stop()

    async def __token_transfer_processor_stopped(self):
        self.__token_transfer_processors_remaining -= 1
        if self.__token_transfer_processors_remaining < 1:
            for processor in self.__token_metadata_processors:
                await processor.stop()

    async def __token_metadata_processor_stopped(self):
        self.__token_metadata_processors_remaining -= 1
        if self.__token_metadata_processors_remaining < 1:
            for processor in self.__token_metadata_gathering_processors:
                await processor.stop()

    async def __token_metadata_gathering_processor_stopped(self):
        self.__token_metadata_gathering_processors_remaining -= 1
        if self.__token_metadata_gathering_processors_remaining < 1:
            for processor in self.__token_persistence_processors:
                await processor.stop()

    async def __queue_block_ids(self, block_id_queue, starting_block, ending_block):
        for block_id in range(starting_block, ending_block + 1):
            await block_id_queue.put(BlockIDTransportObject(block_id))
        await asyncio.sleep(0.1)
        await self.__event_bus.trigger(
            self.BLOCK_ADDING_ENDED_EVENT
        )  # Alert that no more blocks are being added

    async def run(self, block_id_queue, starting_block, ending_block):
        await self.__register_process_stop_events()

        processor_coroutines = [processor.start() for processor in self.__processors]

        # Start all the processes on the event loop
        await asyncio.gather(
            *processor_coroutines,
            self.__queue_block_ids(block_id_queue, starting_block, ending_block),
        )


async def process_contracts_async(
    stats_service: StatsService,
    archive_node_uri: str,
    dynamodb_uri: str,
    ipfs_node_uri: str,
    arweave_node_uri: str,
    starting_block: int,
    ending_block: int,
    rpc_batch_size: int,
    dynamodb_batch_size: int,
    http_batch_size: int,
    max_batch_wait_time: int,
    block_processor_instances: int,
    transaction_processor_instances: int,
    contract_processor_instances: int,
    contract_persistence_processor_instances: int,
    token_processor_instances: int,
    token_metadata_processor_instances: int,
    token_metadata_gathering_processor_instances: int,
    token_persistence_processor_instances: int,
):
    rpc_client = RPCClient(archive_node_uri)
    event_bus = EventBus()
    dynamo_kwargs = {}
    if dynamodb_uri is not None:  # This would only be in non-deployed environments
        dynamo_kwargs["endpoint_url"] = dynamodb_uri
    session = aioboto3.Session()
    async with session.resource("dynamodb", **dynamo_kwargs) as dynamodb:
        block_id_queue: Queue = Queue()
        transaction_queue: Queue = Queue()
        token_transfer_queue: Queue = Queue()
        contract_queue: Queue = Queue()
        contract_persistence_queue = Queue()
        token_metadata_queue = Queue()
        token_metadata_gathering_queue = Queue()
        token_persistence_queue = Queue()
        block_processors: List[BlockProcessor] = list()
        for _ in range(block_processor_instances):
            processor = BlockProcessor(
                rpc_client=rpc_client,
                stats_service=stats_service,
                event_bus=event_bus,
                rpc_batch_size=rpc_batch_size,
                inbound_queue=block_id_queue,
                transaction_queue=transaction_queue,
                max_batch_wait=max_batch_wait_time,
            )
            block_processors.append(processor)

        transaction_processors: List[TransactionProcessor] = list()
        for _ in range(transaction_processor_instances):
            processor = TransactionProcessor(
                rpc_client=rpc_client,
                stats_service=stats_service,
                event_bus=event_bus,
                rpc_batch_size=rpc_batch_size,
                inbound_queue=transaction_queue,
                outbound_contract_queue=contract_queue,
                outbound_token_queue=token_transfer_queue,
                max_batch_wait=max_batch_wait_time,
            )
            transaction_processors.append(processor)

        contract_processors: List[ContractProcessor] = list()
        for _ in range(contract_processor_instances):
            processor = ContractProcessor(
                rpc_client=rpc_client,
                stats_service=stats_service,
                event_bus=event_bus,
                rpc_batch_size=rpc_batch_size,
                inbound_queue=contract_queue,
                outbound_queue=contract_persistence_queue,
                max_batch_wait=max_batch_wait_time,
            )
            contract_processors.append(processor)

        persistence_processors: List[CollectionPersistenceProcessor] = list()
        for _ in range(contract_persistence_processor_instances):
            processor = CollectionPersistenceProcessor(
                dynamodb=dynamodb,
                stats_service=stats_service,
                event_bus=event_bus,
                inbound_queue=contract_persistence_queue,
                dynamodb_batch_size=dynamodb_batch_size,
                max_batch_wait=max_batch_wait_time,
            )
            persistence_processors.append(processor)

        token_processors: List[TokenTransferProcessor] = list()
        for _ in range(token_processor_instances):
            processor = TokenTransferProcessor(
                dynamodb=dynamodb,
                stats_service=stats_service,
                event_bus=event_bus,
                inbound_queue=token_transfer_queue,
                outbound_queue=token_metadata_queue,
                dynamodb_batch_size=dynamodb_batch_size,
                max_batch_wait=max_batch_wait_time,
            )
            token_processors.append(processor)

        token_metadata_processors: List[TokenMetadataProcessor] = list()
        for _ in range(token_metadata_processor_instances):
            processor = TokenMetadataProcessor(
                rpc_client=rpc_client,
                rpc_batch_size=rpc_batch_size,
                stats_service=stats_service,
                event_bus=event_bus,
                inbound_queue=token_metadata_queue,
                outbound_token_gathering_queue=token_metadata_gathering_queue,
                outbound_token_persistence_queue=token_persistence_queue,
                max_batch_wait=max_batch_wait_time,
            )
            token_metadata_processors.append(processor)

        token_metadata_gathering_processors: List[TokenMetadataGatheringProcessor] = list()
        http_batch_data_client = HttpBatchDataClient()
        ipfs_batch_data_client = IpfsBatchDataClient(ipfs_node_uri)
        arweave_batch_client = ArweaveBatchDataClient(arweave_node_uri)
        for _ in range(token_metadata_gathering_processor_instances):
            processor = TokenMetadataGatheringProcessor(
                stats_service=stats_service,
                event_bus=event_bus,
                inbound_queue=token_metadata_gathering_queue,
                outbound_queue=token_persistence_queue,
                http_batch_client=http_batch_data_client,
                ipfs_batch_client=ipfs_batch_data_client,
                arweave_batch_client=arweave_batch_client,
                http_batch_size=http_batch_size,
                max_batch_wait=max_batch_wait_time,
            )
            token_metadata_gathering_processors.append(processor)

        token_persistence_processors: List[TokenPersistenceProcessor] = list()
        for _ in range(token_persistence_processor_instances):
            processor = TokenPersistenceProcessor(
                dynamodb=dynamodb,
                stats_service=stats_service,
                event_bus=event_bus,
                inbound_queue=token_persistence_queue,
                dynamodb_batch_size=dynamodb_batch_size,
                max_batch_wait=max_batch_wait_time,
            )
            token_persistence_processors.append(processor)

        run_manager = RunManager(
            event_bus,
            block_processors,
            transaction_processors,
            contract_processors,
            persistence_processors,
            token_processors,
            token_metadata_processors,
            token_metadata_gathering_processors,
            token_persistence_processors,
        )
        await run_manager.run(block_id_queue, starting_block, ending_block)


async def reset_db_async(endpoint_url):
    session = aioboto3.Session()
    async with session.resource(
        "dynamodb",
        endpoint_url=endpoint_url,
    ) as dynamodb:
        for model in (Collections, TokenTransfers, Tokens):
            table = await dynamodb.Table(model.table_name)
            # noinspection PyUnresolvedReferences
            try:
                await table.delete()
            except ClientError as err:
                if type(err).__name__ != "ResourceNotFoundException":
                    # ResourceNotFound means table did not exist which is fine.
                    # Re-raise otherwise.
                    raise
            table = await dynamodb.create_table(**model.schema)
            await table.wait_until_exists()
