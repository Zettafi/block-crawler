import asyncio
import datetime
import time
from asyncio import Queue
from logging import Logger
from typing import Union, Dict, List

import aioboto3
from botocore.config import Config

from chainconductor.blockcrawler.data_clients import (
    HttpDataClient,
    IpfsDataClient,
    ArweaveDataClient,
    ProtocolError,
    DataUriDataClient,
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
    TokenMetadataPersistenceBatchProcessor,
)
from chainconductor.blockcrawler.processors.direct_batch import (
    DirectBatchProcessor,
    TypedDirectDispositionStrategy,
    DirectDispositionStrategy,
)
from chainconductor.blockcrawler.processors.queued_batch import (
    QueuedAcquisitionStrategy,
    QueuedDispositionStrategy,
    TypeQueuedDispositionStrategy,
    DevNullDispositionStrategy,
    END_OF_RUN_MARKER,
    QueuedBatchProcessor,
)
from chainconductor.blockcrawler.stats import StatsService
from chainconductor.data.models import LastBlock
from chainconductor.web3.rpc import RPCClient, RPCError


def __build_evm_queue_based_pipeline(
    block_id_queue: Queue,
    stats_service: StatsService,
    dynamodb,
    s3_bucket,
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
    s3_batch_size: int,
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
    token_metadata_persistence_processors: int,
    rpc_max_queue_size: int,
    dynamo_max_queue_size: int,
    http_max_queue_size: int,
    rpc_error_retries: int,
):
    rpc_client = RPCClient(archive_node_uri)
    transaction_queue: Queue = Queue(maxsize=rpc_max_queue_size)
    token_transfer_persistence_queue: Queue = Queue(maxsize=rpc_max_queue_size)
    contract_queue: Queue = Queue(maxsize=rpc_max_queue_size)
    collection_persistence_queue: Queue = Queue(maxsize=dynamo_max_queue_size)
    token_metadata_uri_queue: Queue = Queue(maxsize=rpc_max_queue_size)
    token_metadata_retrieval_queue: Queue = Queue(maxsize=http_max_queue_size)
    token_persistence_queue: Queue = Queue(maxsize=dynamo_max_queue_size)
    token_metadata_persistence_queue: Queue = Queue(maxsize=dynamo_max_queue_size)
    processors: List = list()
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
                rpc_error_retries,
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
                rpc_error_retries,
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
                rpc_error_retries,
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
                logger=logger,
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
            disposition_strategy=QueuedDispositionStrategy(
                token_persistence_queue,
                token_metadata_retrieval_queue,
            ),
            batch_processor=RPCErrorRetryDecoratingBatchProcessor(
                TokenMetadataUriBatchProcessor(
                    rpc_client=rpc_client,
                    stats_service=stats_service,
                ),
                rpc_error_retries,
            ),
            logger=logger,
            max_processors=token_metadata_uri_processors,
        )
    )

    http_batch_data_client = HttpDataClient(http_metadata_timeout)
    ipfs_batch_data_client = IpfsDataClient(ipfs_node_uri, ipfs_metadata_timeout)
    arweave_batch_client = ArweaveDataClient(arweave_node_uri, arweave_metadata_timeout)
    data_uri_data_client = DataUriDataClient()
    processors.append(
        QueuedBatchProcessor(
            acquisition_strategy=QueuedAcquisitionStrategy(
                queue=token_metadata_retrieval_queue,
                max_batch_size=http_batch_size,
                max_batch_wait=max_batch_wait_time,
            ),
            disposition_strategy=QueuedDispositionStrategy(token_metadata_persistence_queue),
            batch_processor=TokenMetadataRetrievalBatchProcessor(
                stats_service=stats_service,
                http_data_client=http_batch_data_client,
                ipfs_data_client=ipfs_batch_data_client,
                arweave_data_client=arweave_batch_client,
                data_uri_data_client=data_uri_data_client,
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

    processors.append(
        QueuedBatchProcessor(
            acquisition_strategy=QueuedAcquisitionStrategy(
                queue=token_metadata_persistence_queue,
                max_batch_size=dynamodb_batch_size,
                max_batch_wait=max_batch_wait_time,
            ),
            disposition_strategy=DevNullDispositionStrategy(),
            batch_processor=TokenMetadataPersistenceBatchProcessor(
                s3_bucket=s3_bucket,
                s3_batch_size=s3_batch_size,
                stats_service=stats_service,
                logger=logger,
                blockchain=blockchain,
            ),
            logger=logger,
            max_processors=token_metadata_persistence_processors,
        )
    )

    return processors


def __build_evm_direct_pipeline(
    stats_service: StatsService,
    dynamodb,
    s3_bucket,
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
    s3_batch_size: int,
    http_batch_size: int,
    block_processors: int,
    transaction_processors: int,
    contract_processors: int,
    collection_persistence_processors: int,
    token_transfer_persistence_processors: int,
    token_metadata_uri_processors: int,
    token_metadata_retrieval_processors: int,
    token_persistence_processors: int,
    token_metadata_persistence_processors: int,
    rpc_error_retries: int,
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
        batch_size=dynamodb_batch_size,
        max_batch_processor_instances=token_persistence_processors,
    )

    token_metadata_persistence_batch_processor = DirectBatchProcessor(
        disposition_strategy=DevNullDispositionStrategy(),
        batch_processor=TokenMetadataPersistenceBatchProcessor(
            s3_bucket=s3_bucket,
            s3_batch_size=s3_batch_size,
            stats_service=stats_service,
            logger=logger,
            blockchain=blockchain,
        ),
        batch_size=dynamodb_batch_size,
        max_batch_processor_instances=token_metadata_persistence_processors,
    )

    http_batch_data_client = HttpDataClient(http_metadata_timeout)
    ipfs_batch_data_client = IpfsDataClient(ipfs_node_uri, ipfs_metadata_timeout)
    arweave_batch_client = ArweaveDataClient(arweave_node_uri, arweave_metadata_timeout)
    data_uri_data_client = DataUriDataClient()
    token_metadata_retrieval_batch_processor = DirectBatchProcessor(
        disposition_strategy=DirectDispositionStrategy(token_metadata_persistence_batch_processor),
        batch_processor=TokenMetadataRetrievalBatchProcessor(
            stats_service=stats_service,
            http_data_client=http_batch_data_client,
            ipfs_data_client=ipfs_batch_data_client,
            arweave_data_client=arweave_batch_client,
            data_uri_data_client=data_uri_data_client,
        ),
        batch_size=http_batch_size,
        max_batch_processor_instances=token_metadata_retrieval_processors,
    )

    token_metadata_uri_batch_processor = DirectBatchProcessor(
        disposition_strategy=DirectDispositionStrategy(
            token_persistence_batch_processor,
            token_metadata_retrieval_batch_processor,
        ),
        batch_processor=RPCErrorRetryDecoratingBatchProcessor(
            TokenMetadataUriBatchProcessor(
                rpc_client=rpc_client,
                stats_service=stats_service,
            ),
            rpc_error_retries,
        ),
        batch_size=rpc_batch_size,
        max_batch_processor_instances=token_metadata_uri_processors,
    )

    token_transfer_persistence_batch_processor = DirectBatchProcessor(
        disposition_strategy=DirectDispositionStrategy(token_metadata_uri_batch_processor),
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
            logger=logger,
            blockchain=blockchain,
        ),
        batch_size=dynamodb_batch_size,
        max_batch_processor_instances=collection_persistence_processors,
    )

    contract_batch_processor = DirectBatchProcessor(
        disposition_strategy=DirectDispositionStrategy(collection_persistence_batch_processor),
        batch_processor=RPCErrorRetryDecoratingBatchProcessor(
            ContractBatchProcessor(
                rpc_client=rpc_client,
                stats_service=stats_service,
            ),
            rpc_error_retries,
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
            rpc_error_retries,
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
            rpc_error_retries,
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
    aws_endpoint_url: str,
    dynamodb_timeout: float,
    s3_metadata_bucket: str,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    starting_block: int,
    ending_block: int,
    rpc_batch_size: int,
    rpc_error_retries: int,
    dynamodb_batch_size: int,
    s3_batch_size: int,
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
    token_metadata_persistence_processors: int,
):
    config = Config(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    resource_kwargs: Dict[str, Union[str, Config]] = dict(config=config)
    if aws_endpoint_url is not None:  # This would only be in non-deployed environments
        resource_kwargs["endpoint_url"] = aws_endpoint_url
    session = aioboto3.Session()
    async with session.resource("dynamodb", **resource_kwargs) as dynamodb:  # type: ignore
        async with session.resource("s3", **resource_kwargs) as s3_resource:  # type: ignore
            s3_bucket = await s3_resource.Bucket(s3_metadata_bucket)
            rpc_max_queue_size = rpc_batch_size * 2
            dynamo_max_queue_size = dynamodb_batch_size * 2
            http_max_queue_size = http_batch_size * 2
            block_id_queue: Queue = Queue(maxsize=rpc_max_queue_size)
            processors = __build_evm_queue_based_pipeline(
                block_id_queue=block_id_queue,
                stats_service=stats_service,
                dynamodb=dynamodb,
                s3_bucket=s3_bucket,
                logger=logger,
                archive_node_uri=archive_node_uri,
                blockchain=blockchain,
                http_metadata_timeout=http_metadata_timeout,
                ipfs_node_uri=ipfs_node_uri,
                ipfs_metadata_timeout=ipfs_metadata_timeout,
                arweave_node_uri=arweave_node_uri,
                arweave_metadata_timeout=arweave_metadata_timeout,
                rpc_batch_size=rpc_batch_size,
                rpc_error_retries=rpc_error_retries,
                dynamodb_batch_size=dynamodb_batch_size,
                s3_batch_size=s3_batch_size,
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
                token_metadata_persistence_processors=token_metadata_persistence_processors,
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
    s3_metadata_bucket: str,
    aws_endpoint_url: str,
    dynamodb_timeout: float,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    rpc_batch_size: int,
    rpc_error_retries: int,
    dynamodb_batch_size: int,
    s3_batch_size: int,
    http_batch_size: int,
    block_processors: int,
    transaction_processors: int,
    contract_processors: int,
    collection_persistence_processors: int,
    token_transfer_persistence_processors: int,
    token_metadata_uri_processors: int,
    token_metadata_retrieval_processors: int,
    token_metadata_persistence_processors: int,
    token_persistence_processors: int,
    trail_blocks: int,
    process_interval: int,
):
    config = Config(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    resource_kwargs: Dict[str, Union[str, Config]] = dict(config=config)
    if aws_endpoint_url is not None:  # This would only be in non-deployed environments
        resource_kwargs["endpoint_url"] = aws_endpoint_url
    session = aioboto3.Session()
    async with session.resource("dynamodb", **resource_kwargs) as dynamodb:  # type: ignore
        async with session.resource("s3", **resource_kwargs) as s3:  # type: ignore
            s3_bucket = await s3.Bucket(s3_metadata_bucket)
            processor = __build_evm_direct_pipeline(
                stats_service=stats_service,
                dynamodb=dynamodb,
                s3_bucket=s3_bucket,
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
                s3_batch_size=s3_batch_size,
                http_batch_size=http_batch_size,
                block_processors=block_processors,
                transaction_processors=transaction_processors,
                contract_processors=contract_processors,
                collection_persistence_processors=collection_persistence_processors,
                token_transfer_persistence_processors=token_transfer_persistence_processors,
                token_metadata_uri_processors=token_metadata_uri_processors,
                token_metadata_retrieval_processors=token_metadata_retrieval_processors,
                token_persistence_processors=token_persistence_processors,
                token_metadata_persistence_processors=token_metadata_persistence_processors,
                rpc_error_retries=rpc_error_retries,
            )

            last_block_table = await dynamodb.Table(LastBlock.table_name)
            last_block_result = await last_block_table.get_item(Key={"blockchain": blockchain})
            try:
                last_block_processed = int(last_block_result.get("Item").get("block_id"))
            except AttributeError:
                exit(
                    "Unable to determine the last block number processed. "
                    "Are you starting fresh and forget to seed?"
                )

            rpc_client = RPCClient(archive_node_uri)
            process_time: float = 0.0
            caught_up = False
            while True:
                try:
                    block_number = await rpc_client.get_block_number()
                    current_block_number = block_number.int_value - trail_blocks
                    if last_block_processed < current_block_number:
                        start_block = last_block_processed + 1
                        blocks = [
                            block_id for block_id in range(start_block, current_block_number + 1)
                        ]
                        if not caught_up and len(blocks) > 1:
                            print(f"Catching up {len(blocks)} blocks")
                        start = time.perf_counter()
                        await processor(blocks[:])
                        end = time.perf_counter()
                        process_time = end - start
                        blocks = stats_service.get_count(BlockBatchProcessor.PROCESSED_STAT)
                        transactions = stats_service.get_count(
                            TransactionBatchProcessor.PROCESSED_STAT
                        )
                        collections = stats_service.get_count(
                            CollectionPersistenceBatchProcessor.PROCESSED_STAT
                        )
                        transfers = stats_service.get_count(
                            TokenTransferPersistenceBatchProcessor.PROCESSED_STAT
                        )
                        tokens = stats_service.get_count(
                            TokenPersistenceBatchProcessor.PROCESSED_STAT
                        )
                        metadata = stats_service.get_count(
                            TokenMetadataPersistenceBatchProcessor.PROCESSED_STAT
                        )
                        metadata_error = stats_service.get_count(
                            TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_HTTP_TIMEOUT_STAT  # noqa E501
                            + TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_HTTP_ERROR_STAT  # noqa E501
                            + TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_IPFS_ERROR_STAT  # noqa E501
                            + TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_ARWEAVE_ERROR_STAT  # noqa E501
                            + TokenMetadataRetrievalBatchProcessor.METADATA_RETRIEVAL_DATA_URI_ERROR_STAT  # noqa E501
                        )
                        stats_service.reset()
                        print(
                            datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S%z")
                            + f" - {start_block}:{current_block_number}"
                            f" - {process_time:0.3f}s"
                            f" - blk:{blocks:,}"
                            f"/tx:{transactions:,}"
                            f"/col:{collections:,}"
                            f"/xfr:{transfers:,}"
                            f"/tk:{tokens:,}"
                            f"/mst:{metadata:,}"
                            f"/mer:{metadata_error:,}"
                        )
                        last_block_processed = current_block_number

                        await last_block_table.put_item(
                            Item={
                                "blockchain": blockchain,
                                "block_id": last_block_processed,
                            }
                        )
                    caught_up = True
                    await asyncio.sleep(process_interval - process_time)
                except (ProtocolError, RPCError, asyncio.TimeoutError):
                    logger.exception("A communication error occurred!")
                    await asyncio.sleep(10)


async def set_last_block_id_for_block_chain(
    blockchain: str, last_block_id: int, aws_endpoint_url: str
):
    resource_kwargs = dict()
    if aws_endpoint_url is not None:  # This would only be in non-deployed environments
        resource_kwargs["endpoint_url"] = aws_endpoint_url
    session = aioboto3.Session()
    async with session.resource("dynamodb", **resource_kwargs) as dynamodb:  # type: ignore
        last_block = await dynamodb.Table(LastBlock.table_name)
        await last_block.put_item(
            Item={
                "blockchain": blockchain,
                "block_id": last_block_id,
            }
        )


async def get_block(evm_node_uri):
    rpc = RPCClient(evm_node_uri)
    block_number = await rpc.get_block_number()
    return block_number.int_value
