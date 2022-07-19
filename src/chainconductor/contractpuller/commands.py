import asyncio
from asyncio import Queue
from typing import List

import aioboto3
from botocore.exceptions import ClientError

from .events import EventBus
from .processors import (
    BlockProcessor,
    TransactionProcessor,
    ContractPersistenceProcessor,
    Processor,
    ContractProcessor,
    BlockIDTransportObject,
)
from .stats import StatsService
from ..data.models import Contracts
from ..web3.rpc import RPCClient

try:  # If dotenv in installed, use it load env vars
    from dotenv import load_dotenv
    load_dotenv()
except ModuleNotFoundError:
    pass


class RunManager:
    def __init__(
        self,
        event_bus: EventBus,
        block_processors: List[BlockProcessor],
        transaction_processors: List[TransactionProcessor],
        contract_processors: List[ContractProcessor],
        persistence_processors: List[ContractPersistenceProcessor],
    ) -> None:
        self.__contract_processors = contract_processors
        self.__event_bus: EventBus = event_bus
        self.__block_processors: List[BlockProcessor] = block_processors
        self.__block_processors_remaining: int = len(block_processors)
        self.__transaction_processors: List[
            TransactionProcessor
        ] = transaction_processors
        self.__transaction_processors_remaining: int = len(transaction_processors)
        self.__contract_processors: List[ContractProcessor] = contract_processors
        self.__persistence_processors: List[
            ContractPersistenceProcessor
        ] = persistence_processors
        self.__contract_processors_remaining: int = len(persistence_processors)

    async def initialize(self):
        await self.__event_bus.register(
            BLOCK_ADDING_ENDED_EVENT, self.block_id_processor_stopped
        )
        await self.__event_bus.register(
            BlockProcessor.PROCESSOR_STOPPED_EVENT, self.block_processor_stopped
        )
        await self.__event_bus.register(
            TransactionProcessor.PROCESSOR_STOPPED_EVENT,
            self.transaction_processor_stopped,
        )

        await self.__event_bus.register(
            ContractProcessor.PROCESSOR_STOPPED_EVENT, self.contract_processor_stopped
        )

    async def block_id_processor_stopped(self):
        for block_processor in self.__block_processors:
            await block_processor.stop()

    async def block_processor_stopped(self):
        self.__block_processors_remaining -= 1
        if self.__block_processors_remaining < 1:
            for transaction_processor in self.__transaction_processors:
                await transaction_processor.stop()

    async def transaction_processor_stopped(self):
        self.__transaction_processors_remaining -= 1
        if self.__transaction_processors_remaining < 1:
            for contract_processor in self.__contract_processors:
                await contract_processor.stop()

    async def contract_processor_stopped(self):
        self.__contract_processors_remaining -= 1
        if self.__contract_processors_remaining < 1:
            for persistence_processor in self.__persistence_processors:
                await persistence_processor.stop()


BLOCK_ADDING_ENDED_EVENT = "block adding ended"


async def process_contracts_async(
    stats_service: StatsService,
    archive_node_uri: str,
    dynamodb_uri: str,
    starting_block: int,
    ending_block: int,
    rpc_batch_size: int,
    dynamodb_batch_size: int,
    max_batch_wait_time: int,
    block_processor_instances: int,
    transaction_processor_instances: int,
    contract_processor_instances: int,
    contract_persistence_processor_instances: int,
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
        contract_queue: Queue = Queue()
        persistence_queue = Queue()
        block_processors: List[BlockProcessor] = list()
        for _ in range(block_processor_instances):
            processor = BlockProcessor(
                rpc_client,
                stats_service,
                event_bus,
                rpc_batch_size,
                block_id_queue,
                transaction_queue,
                max_batch_wait_time,
            )
            block_processors.append(processor)

        transaction_processors: List[TransactionProcessor] = list()
        for _ in range(transaction_processor_instances):
            processor = TransactionProcessor(
                rpc_client,
                stats_service,
                event_bus,
                rpc_batch_size,
                transaction_queue,
                contract_queue,
                max_batch_wait_time,
            )
            transaction_processors.append(processor)

        contract_processors: List[ContractProcessor] = list()
        for _ in range(contract_processor_instances):
            processor = ContractProcessor(
                rpc_client,
                stats_service,
                event_bus,
                rpc_batch_size,
                contract_queue,
                persistence_queue,
                max_batch_wait_time,
            )
            contract_processors.append(processor)

        persistence_processors: List[ContractPersistenceProcessor] = list()
        for _ in range(contract_persistence_processor_instances):
            processor = ContractPersistenceProcessor(
                dynamodb,
                stats_service,
                event_bus,
                persistence_queue,
                dynamodb_batch_size,
                max_batch_wait_time,
            )
            persistence_processors.append(processor)

        run_manager = RunManager(
            event_bus,
            block_processors,
            transaction_processors,
            contract_processors,
            persistence_processors,
        )
        await run_manager.initialize()

        # noinspection PyTypeChecker
        processors: List[Processor] = [
            processor.start()
            for processor in (
                block_processors
                + transaction_processors
                + contract_processors
                + persistence_processors
            )
        ]

        async def __queue_block_ids():
            for block_id in range(starting_block, ending_block + 1):
                await block_id_queue.put(BlockIDTransportObject(block_id))
            await asyncio.sleep(0.1)
            await event_bus.trigger(
                BLOCK_ADDING_ENDED_EVENT
            )  # Alert that no more blocks are being added

        # Start all the processes on the event loop
        await asyncio.gather(
            *processors,
            __queue_block_ids(),
        )


async def reset_db_async(endpoint_url):
    session = aioboto3.Session()
    async with session.resource(
        "dynamodb",
        endpoint_url=endpoint_url,
    ) as dynamodb:
        table = await dynamodb.Table(Contracts.table_name)
        # noinspection PyUnresolvedReferences
        try:
            await table.delete()
        except ClientError as err:
            if type(err).__name__ != "ResourceNotFoundException":
                # ResourceNotFound means table did not exist which is fine. Re-raise otherwise.
                raise
        table = await dynamodb.create_table(**Contracts.schema)
        await table.wait_until_exists()
