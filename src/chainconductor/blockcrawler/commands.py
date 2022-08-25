import asyncio
import datetime
import time
from asyncio import Queue
from logging import Logger

import aioboto3
from botocore.config import Config

from chainconductor.blockcrawler.data_clients import (
    HttpDataClient,
    IpfsDataClient,
    ArweaveDataClient,
    ProtocolError,
)
from chainconductor.blockcrawler.processors import (
    CollectionPersistenceBatchProcessor,
    ContractBatchProcessor,
    TokenTransferPersistenceBatchProcessor,
    TokenMetadataUriBatchProcessor,
    TokenPersistenceBatchProcessor,
    TokenMetadataRetrievalBatchProcessor,
    BlockBatchProcessor,
    TransactionBatchProcessor,
    ContractTransportObject,
    TokenTransportObject,
    RPCErrorRetryDecoratingBatchProcessor,
)
from chainconductor.blockcrawler.processors.direct_batch import (
    DirectBatchProcessor,
    MetadataDirectDispositionStrategy,
    TypedDirectDispositionStrategy,
)
from chainconductor.blockcrawler.processors.queued_batch import (
    QueuedAcquisitionStrategy,
    QueuedDispositionStrategy,
    TypeQueuedDispositionStrategy,
    DevNullDispositionStrategy,
    END_OF_RUN_MARKER,
    QueuedBatchProcessor,
    MetadataQueuedDispositionStrategy,
)
from chainconductor.blockcrawler.stats import StatsService
from chainconductor.data.models import LastBlock
from chainconductor.web3.rpc import RPCClient, RPCError


def __build_evm_queue_based_pipeline(
    block_id_queue: Queue,
    stats_service: StatsService,
    dynamodb,
    logger: Logger,
    archive_node_uri: str,
    blockchain: str,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    rpc_batch_size: int,
    dynamodb_batch_size: int,
    http_batch_size: int,
    max_batch_wait_time: int,
    block_processors: int,
    transaction_processors: int,
    contract_processors: int,
    collection_persistence_processors: int,
    token_transfer_persistence_processors: int,
    token_metadata_uri_processors: int,
    token_metadata_retrieval_processors: int,
    token_persistence_processors: int,
    rpc_max_queue_size: int,
    dynamo_max_queue_size: int,
    http_max_queue_size: int,
):
    rpc_client = RPCClient(archive_node_uri)
    transaction_queue: Queue = Queue(maxsize=rpc_max_queue_size)
    token_transfer_persistence_queue: Queue = Queue(maxsize=rpc_max_queue_size)
    contract_queue: Queue = Queue(maxsize=rpc_max_queue_size)
    collection_persistence_queue = Queue(maxsize=dynamo_max_queue_size)
    token_metadata_uri_queue = Queue(maxsize=rpc_max_queue_size)
    token_metadata_retrieval_queue = Queue(maxsize=http_max_queue_size)
    token_persistence_queue = Queue(maxsize=dynamo_max_queue_size)
    processors = list()
    processors.append(
        QueuedBatchProcessor(
            acquisition_strategy=QueuedAcquisitionStrategy(
                queue=block_id_queue,
                max_batch_size=rpc_batch_size,
                max_batch_wait=max_batch_wait_time,
            ),
            disposition_strategy=QueuedDispositionStrategy(transaction_queue),
            batch_processor=RPCErrorRetryDecoratingBatchProcessor(
                BlockBatchProcessor(
                    rpc_client=rpc_client,
                    stats_service=stats_service,
                ),
                5,
            ),
            logger=logger,
            max_processors=block_processors,
        )
    )

    processors.append(
        QueuedBatchProcessor(
            acquisition_strategy=QueuedAcquisitionStrategy(
                queue=transaction_queue,
                max_batch_size=rpc_batch_size,
                max_batch_wait=max_batch_wait_time,
            ),
            disposition_strategy=TypeQueuedDispositionStrategy(
                (ContractTransportObject, contract_queue),
                (TokenTransportObject, token_transfer_persistence_queue),
            ),
            batch_processor=RPCErrorRetryDecoratingBatchProcessor(
                TransactionBatchProcessor(
                    rpc_client=rpc_client,
                    stats_service=stats_service,
                ),
                5,
            ),
            logger=logger,
            max_processors=transaction_processors,
        )
    )

    processors.append(
        QueuedBatchProcessor(
            acquisition_strategy=QueuedAcquisitionStrategy(
                queue=contract_queue,
                max_batch_size=rpc_batch_size,
                max_batch_wait=max_batch_wait_time,
            ),
            disposition_strategy=QueuedDispositionStrategy(collection_persistence_queue),
            batch_processor=RPCErrorRetryDecoratingBatchProcessor(
                ContractBatchProcessor(
                    rpc_client=rpc_client,
                    stats_service=stats_service,
                ),
                5,
            ),
            logger=logger,
            max_processors=contract_processors,
        )
    )

    processors.append(
        QueuedBatchProcessor(
            acquisition_strategy=QueuedAcquisitionStrategy(
                queue=collection_persistence_queue,
                max_batch_size=dynamodb_batch_size,
                max_batch_wait=max_batch_wait_time,
            ),
            disposition_strategy=DevNullDispositionStrategy(),
            batch_processor=CollectionPersistenceBatchProcessor(
                dynamodb=dynamodb,
                stats_service=stats_service,
                blockchain=blockchain,
            ),
            logger=logger,
            max_processors=collection_persistence_processors,
        )
    )

    processors.append(
        QueuedBatchProcessor(
            acquisition_strategy=QueuedAcquisitionStrategy(
                queue=token_transfer_persistence_queue,
                max_batch_size=dynamodb_batch_size,
                max_batch_wait=max_batch_wait_time,
            ),
            disposition_strategy=QueuedDispositionStrategy(token_metadata_uri_queue),
            batch_processor=TokenTransferPersistenceBatchProcessor(
                dynamodb=dynamodb,
                stats_service=stats_service,
                logger=logger,
                blockchain=blockchain,
            ),
            logger=logger,
            max_processors=token_transfer_persistence_processors,
        )
    )

    processors.append(
        QueuedBatchProcessor(
            acquisition_strategy=QueuedAcquisitionStrategy(
                queue=token_metadata_uri_queue,
                max_batch_size=rpc_batch_size,
                max_batch_wait=max_batch_wait_time,
            ),
            disposition_strategy=MetadataQueuedDispositionStrategy(
                storage_queue=token_persistence_queue,
                capture_queue=token_metadata_retrieval_queue,
            ),
            batch_processor=RPCErrorRetryDecoratingBatchProcessor(
                TokenMetadataUriBatchProcessor(
                    rpc_client=rpc_client,
                    stats_service=stats_service,
                ),
                5,
            ),
            logger=logger,
            max_processors=token_metadata_uri_processors,
        )
    )

    http_batch_data_client = HttpDataClient(http_metadata_timeout)
    ipfs_batch_data_client = IpfsDataClient(ipfs_node_uri, ipfs_metadata_timeout)
    arweave_batch_client = ArweaveDataClient(arweave_node_uri, arweave_metadata_timeout)
    processors.append(
        QueuedBatchProcessor(
            acquisition_strategy=QueuedAcquisitionStrategy(
                queue=token_metadata_retrieval_queue,
                max_batch_size=http_batch_size,
                max_batch_wait=max_batch_wait_time,
            ),
            disposition_strategy=QueuedDispositionStrategy(token_persistence_queue),
            batch_processor=RPCErrorRetryDecoratingBatchProcessor(
                TokenMetadataRetrievalBatchProcessor(
                    http_data_client=http_batch_data_client,
                    ipfs_data_client=ipfs_batch_data_client,
                    arweave_data_client=arweave_batch_client,
                    stats_service=stats_service,
                ),
                5,
            ),
            logger=logger,
            max_processors=token_metadata_retrieval_processors,
        )
    )

    processors.append(
        QueuedBatchProcessor(
            acquisition_strategy=QueuedAcquisitionStrategy(
                queue=token_persistence_queue,
                max_batch_size=dynamodb_batch_size,
                max_batch_wait=max_batch_wait_time,
            ),
            disposition_strategy=DevNullDispositionStrategy(),
            batch_processor=TokenPersistenceBatchProcessor(
                dynamodb=dynamodb,
                stats_service=stats_service,
                logger=logger,
                blockchain=blockchain,
            ),
            logger=logger,
            max_processors=token_persistence_processors,
        )
    )
    return processors


def __build_evm_direct_pipeline(
    stats_service: StatsService,
    dynamodb,
    logger: Logger,
    archive_node_uri: str,
    blockchain: str,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    rpc_batch_size: int,
    dynamodb_batch_size: int,
    http_batch_size: int,
    block_processors: int,
    transaction_processors: int,
    contract_processors: int,
    collection_persistence_processors: int,
    token_transfer_persistence_processors: int,
    token_metadata_uri_processors: int,
    token_metadata_retrieval_processors: int,
    token_persistence_processors: int,
):
    rpc_client = RPCClient(archive_node_uri)
    token_persistence_batch_processor = DirectBatchProcessor(
        disposition_strategy=DevNullDispositionStrategy(),
        batch_processor=TokenPersistenceBatchProcessor(
            dynamodb=dynamodb,
            stats_service=stats_service,
            logger=logger,
            blockchain=blockchain,
        ),
        batch_size=rpc_batch_size,
        max_batch_processor_instances=token_persistence_processors,
    )

    http_batch_data_client = HttpDataClient(http_metadata_timeout)
    ipfs_batch_data_client = IpfsDataClient(ipfs_node_uri, ipfs_metadata_timeout)
    arweave_batch_client = ArweaveDataClient(arweave_node_uri, arweave_metadata_timeout)
    token_metadata_retrieval_batch_processor = DirectBatchProcessor(
        disposition_strategy=token_persistence_batch_processor,
        batch_processor=RPCErrorRetryDecoratingBatchProcessor(
            TokenMetadataRetrievalBatchProcessor(
                http_data_client=http_batch_data_client,
                ipfs_data_client=ipfs_batch_data_client,
                arweave_data_client=arweave_batch_client,
                stats_service=stats_service,
            ),
            5,
        ),
        batch_size=http_batch_size,
        max_batch_processor_instances=token_metadata_retrieval_processors,
    )

    token_metadata_uri_batch_processor = DirectBatchProcessor(
        disposition_strategy=MetadataDirectDispositionStrategy(
            persistence_processor=token_persistence_batch_processor,
            capture_processor=token_metadata_retrieval_batch_processor,
        ),
        batch_processor=RPCErrorRetryDecoratingBatchProcessor(
            TokenMetadataUriBatchProcessor(
                rpc_client=rpc_client,
                stats_service=stats_service,
            ),
            5,
        ),
        batch_size=rpc_batch_size,
        max_batch_processor_instances=token_metadata_uri_processors,
    )

    token_transfer_persistence_batch_processor = DirectBatchProcessor(
        disposition_strategy=token_metadata_uri_batch_processor,
        batch_processor=TokenTransferPersistenceBatchProcessor(
            dynamodb=dynamodb,
            stats_service=stats_service,
            logger=logger,
            blockchain=blockchain,
        ),
        batch_size=dynamodb_batch_size,
        max_batch_processor_instances=token_transfer_persistence_processors,
    )

    collection_persistence_batch_processor = DirectBatchProcessor(
        disposition_strategy=DevNullDispositionStrategy(),
        batch_processor=CollectionPersistenceBatchProcessor(
            dynamodb=dynamodb,
            stats_service=stats_service,
            blockchain=blockchain,
        ),
        batch_size=dynamodb_batch_size,
        max_batch_processor_instances=collection_persistence_processors,
    )

    contract_batch_processor = DirectBatchProcessor(
        disposition_strategy=collection_persistence_batch_processor,
        batch_processor=RPCErrorRetryDecoratingBatchProcessor(
            ContractBatchProcessor(
                rpc_client=rpc_client,
                stats_service=stats_service,
            ),
            5,
        ),
        batch_size=rpc_batch_size,
        max_batch_processor_instances=contract_processors,
    )

    transaction_batch_processor = DirectBatchProcessor(
        disposition_strategy=TypedDirectDispositionStrategy(
            (ContractTransportObject, contract_batch_processor),
            (TokenTransportObject, token_transfer_persistence_batch_processor),
        ),
        batch_processor=RPCErrorRetryDecoratingBatchProcessor(
            TransactionBatchProcessor(
                rpc_client=rpc_client,
                stats_service=stats_service,
            ),
            5,
        ),
        batch_size=rpc_batch_size,
        max_batch_processor_instances=transaction_processors,
    )

    block_batch_processor = DirectBatchProcessor(
        disposition_strategy=transaction_batch_processor,
        batch_processor=RPCErrorRetryDecoratingBatchProcessor(
            BlockBatchProcessor(
                rpc_client=rpc_client,
                stats_service=stats_service,
            ),
            5,
        ),
        batch_size=rpc_batch_size,
        max_batch_processor_instances=block_processors,
    )

    return block_batch_processor


async def crawl_evm_blocks(
    stats_service: StatsService,
    logger: Logger,
    archive_node_uri: str,
    blockchain: str,
    dynamodb_uri: str,
    dynamodb_timeout: float,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    starting_block: int,
    ending_block: int,
    rpc_batch_size: int,
    dynamodb_batch_size: int,
    http_batch_size: int,
    max_batch_wait_time: int,
    block_processors: int,
    transaction_processors: int,
    contract_processors: int,
    collection_persistence_processors: int,
    token_transfer_persistence_processors: int,
    token_metadata_uri_processors: int,
    token_metadata_retrieval_processors: int,
    token_persistence_processors: int,
):
    config = Config(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    dynamo_kwargs = dict(config=config)
    if dynamodb_uri is not None:  # This would only be in non-deployed environments
        dynamo_kwargs["endpoint_url"] = dynamodb_uri
    session = aioboto3.Session()
    async with session.resource("dynamodb", **dynamo_kwargs) as dynamodb:
        rpc_max_queue_size = rpc_batch_size * 2
        dynamo_max_queue_size = dynamodb_batch_size * 2
        http_max_queue_size = http_batch_size * 2
        block_id_queue: Queue = Queue(maxsize=rpc_max_queue_size)
        processors = __build_evm_queue_based_pipeline(
            block_id_queue=block_id_queue,
            stats_service=stats_service,
            dynamodb=dynamodb,
            logger=logger,
            archive_node_uri=archive_node_uri,
            blockchain=blockchain,
            http_metadata_timeout=http_metadata_timeout,
            ipfs_node_uri=ipfs_node_uri,
            ipfs_metadata_timeout=ipfs_metadata_timeout,
            arweave_node_uri=arweave_node_uri,
            arweave_metadata_timeout=arweave_metadata_timeout,
            rpc_batch_size=rpc_batch_size,
            dynamodb_batch_size=dynamodb_batch_size,
            http_batch_size=http_batch_size,
            max_batch_wait_time=max_batch_wait_time,
            block_processors=block_processors,
            transaction_processors=transaction_processors,
            contract_processors=contract_processors,
            collection_persistence_processors=collection_persistence_processors,
            token_transfer_persistence_processors=token_transfer_persistence_processors,
            token_metadata_uri_processors=token_metadata_uri_processors,
            token_metadata_retrieval_processors=token_metadata_retrieval_processors,
            token_persistence_processors=token_persistence_processors,
            rpc_max_queue_size=rpc_max_queue_size,
            dynamo_max_queue_size=dynamo_max_queue_size,
            http_max_queue_size=http_max_queue_size,
        )
        processor_coroutines = [processor.run() for processor in processors]

        async def __queue_block_ids(queue: Queue, start_block: int, end_block: int):
            for block_id in range(start_block, end_block + 1):
                await queue.put(block_id)
            await queue.put(END_OF_RUN_MARKER)

        # Start all the processes on the event loop
        await asyncio.gather(
            *processor_coroutines,
            __queue_block_ids(block_id_queue, starting_block, ending_block),
        )


async def listen_for_and_process_new_evm_blocks(
    stats_service: StatsService,
    logger: Logger,
    archive_node_uri: str,
    blockchain: str,
    dynamodb_uri: str,
    dynamodb_timeout: float,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    rpc_batch_size: int,
    dynamodb_batch_size: int,
    http_batch_size: int,
    block_processors: int,
    transaction_processors: int,
    contract_processors: int,
    collection_persistence_processors: int,
    token_transfer_persistence_processors: int,
    token_metadata_uri_processors: int,
    token_metadata_retrieval_processors: int,
    token_persistence_processors: int,
    trail_blocks: int,
    process_interval: int,
):
    config = Config(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    dynamo_kwargs = dict(config=config)
    if dynamodb_uri is not None:  # This would only be in non-deployed environments
        dynamo_kwargs["endpoint_url"] = dynamodb_uri
    session = aioboto3.Session()
    async with session.resource("dynamodb", **dynamo_kwargs) as dynamodb:
        processor = __build_evm_direct_pipeline(
            stats_service=stats_service,
            dynamodb=dynamodb,
            logger=logger,
            archive_node_uri=archive_node_uri,
            blockchain=blockchain,
            http_metadata_timeout=http_metadata_timeout,
            ipfs_node_uri=ipfs_node_uri,
            ipfs_metadata_timeout=ipfs_metadata_timeout,
            arweave_node_uri=arweave_node_uri,
            arweave_metadata_timeout=arweave_metadata_timeout,
            rpc_batch_size=rpc_batch_size,
            dynamodb_batch_size=dynamodb_batch_size,
            http_batch_size=http_batch_size,
            block_processors=block_processors,
            transaction_processors=transaction_processors,
            contract_processors=contract_processors,
            collection_persistence_processors=collection_persistence_processors,
            token_transfer_persistence_processors=token_transfer_persistence_processors,
            token_metadata_uri_processors=token_metadata_uri_processors,
            token_metadata_retrieval_processors=token_metadata_retrieval_processors,
            token_persistence_processors=token_persistence_processors,
        )

        last_block_table = await dynamodb.Table(LastBlock.table_name)
        last_block_result = await last_block_table.get_item(Key={"blockchain": blockchain})
        last_block_processed = int(last_block_result.get("Item").get("block_id"))
        rpc_client = RPCClient(archive_node_uri)
        process_time = 0
        while True:
            try:
                block_number = await rpc_client.get_block_number()
                current_block_number = block_number.int_value - trail_blocks
                if last_block_processed < current_block_number:
                    start_block = last_block_processed + 1
                    blocks = [block_id for block_id in range(start_block, current_block_number + 1)]
                    start = time.perf_counter()
                    await processor(blocks[:])
                    end = time.perf_counter()
                    process_time = end - start
                    print(
                        datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S%z")
                        + f" - {start_block}:{current_block_number}"
                        f" - {process_time:0.3f}s"
                    )
                    last_block_processed = current_block_number

                    await last_block_table.put_item(
                        Item={
                            "blockchain": blockchain,
                            "block_id": last_block_processed,
                        }
                    )
                await asyncio.sleep(process_interval - process_time)
            except (ProtocolError, RPCError, asyncio.TimeoutError):
                logger.exception("A communication error occurred!")
                await asyncio.sleep(10)


async def set_last_block_id_for_block_chain(blockchain: str, last_block_id: int, dynamodb_uri: str):
    dynamo_kwargs = dict()
    if dynamodb_uri is not None:  # This would only be in non-deployed environments
        dynamo_kwargs["endpoint_url"] = dynamodb_uri
    session = aioboto3.Session()
    async with session.resource("dynamodb", **dynamo_kwargs) as dynamodb:
        last_block = await dynamodb.Table(LastBlock.table_name)
        await last_block.put_item(
            Item={
                "blockchain": blockchain,
                "block_id": last_block_id,
            }
        )
