import asyncio
import logging
import os
import pathlib
import time
from typing import Optional, Dict, Union, Iterable

import aioboto3
import click
import math
from botocore.config import Config as BotoConfig

import blockcrawler
from blockcrawler.core.bus import ParallelDataBus
from blockcrawler.core.click import HexIntParamType
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.stats import StatsService, StatsWriter
from blockcrawler.core.types import HexInt
from blockcrawler.evm.producers import BlockIDProducer
from blockcrawler.evm.rpc import EvmRpcClient
from blockcrawler.evm.services import BlockTimeCache, BlockTimeService
from blockcrawler.evm.transformers import (
    EvmBlockIdToEvmBlockAndEvmTransactionAndEvmTransactionHashTransformer,
    EvmTransactionToContractEvmTransactionReceiptTransformer,
)
from blockcrawler.nft.bin.shared import (
    Config,
    _get_data_version,
    BlockBoundTracker,
    _get_block_time_cache,
    _persist_block_time_cache,
    _get_load_stat_line,
)
from blockcrawler.nft.consumers import NftCollectionPersistenceConsumer
from blockcrawler.nft.data_services.dynamodb import DynamoDbDataService
from blockcrawler.nft.evm.consumers import (
    CollectionToEverythingElseErc721CollectionBasedConsumer,
    CollectionToEverythingElseErc1155CollectionBasedConsumer,
)
from blockcrawler.nft.evm.oracles import TokenTransactionTypeOracle, LogVersionOracle
from blockcrawler.nft.evm.transformers import EvmTransactionReceiptToNftCollectionTransformer


@click.command()
@click.argument("STARTING_BLOCK", type=HexIntParamType())
@click.argument("ENDING_BLOCK", type=HexIntParamType())
@click.argument("BLOCK_HEIGHT", type=HexIntParamType())
@click.option(
    "--increment-data-version",
    envvar="INCREMENT_DATA_VERSION",
    default=False,
    show_default=True,
    help="Increment the data version being processed. This ensures data integrity when "
    "reprocessing the blockchain. Running multiple instances may require the first instance "
    "to increase the version. Subsequent instances should not update the data version.",
)
@click.option(
    "--block-chunk-size",
    envvar="BLOCK_CHUNK_SIZE",
    default=1_000,
    show_default=True,
    help="The size of block range in which the entire range will be divided.",
)
@click.option(
    "--dynamodb-parallel-batches",
    envvar="PARALLEL_BATCHES",
    help="Number of DynamoDB batch writes to perform simultaneously",
    default=10,
    show_default=True,
)
@click.option(
    "--block-time-cache-filename",
    envvar="BLOCK_TIME_CACHE_FILENAME",
    default=f"{os.getcwd()}{os.sep}block-time-cache.csv",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False, path_type=pathlib.Path),
    help="Location and filename for the block time cache. Defaults location is the directory "
    "from which the command was run.",
)
@click.pass_obj
def load(
    config: Config,
    starting_block: HexInt,
    ending_block: HexInt,
    block_height: HexInt,
    block_chunk_size: int,
    increment_data_version: bool,
    dynamodb_parallel_batches: int,
    block_time_cache_filename: pathlib.Path,
):
    """
    Load NFT data in the fastest manner possible

    Load NFT collections from the BLOCKCHAIN from the STARTING_BLOCK to the
    ENDING_BLOCK. All event logs for a Collection will be processed up to the
    BLOCK_HEIGHT. Multiple runs will require the same data version and BLOCK_HEIGHT to
    ensure accurate data.
    """
    logger = logging.getLogger(blockcrawler.LOGGER_NAME)
    block_time_cache = _get_block_time_cache(block_time_cache_filename, logger)
    block_bound_tracker = BlockBoundTracker()
    stats_writer = StatsWriter(config.stats_service, LineWriter(block_bound_tracker))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    start = time.perf_counter()
    stats_task = loop.create_task(stats_writer.write_at_interval(60))
    try:
        loop.run_until_complete(
            run_load(
                blockchain=config.blockchain,
                starting_block=starting_block,
                ending_block=ending_block,
                block_height=block_height,
                increment_data_version=increment_data_version,
                block_chunk_size=block_chunk_size,
                stats_service=config.stats_service,
                block_time_cache=block_time_cache,
                evm_rpc_client=config.evm_rpc_client,
                boto3_session=aioboto3.Session(),
                dynamodb_endpoint_url=config.dynamodb_endpoint_url,
                dynamodb_timeout=config.dynamodb_timeout,
                table_prefix=config.table_prefix,
                dynamodb_parallel_batches=dynamodb_parallel_batches,
                block_bound_tracker=block_bound_tracker,
            )
        )
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user!")
    finally:
        stats_task.cancel()
        while loop.is_running():
            time.sleep(0.001)

        _persist_block_time_cache(logger, block_time_cache, block_time_cache_filename)
        end = time.perf_counter()
        runtime = end - start
        secs = runtime % 60
        all_mins = math.floor(runtime / 60)
        mins = all_mins % 60
        hours = math.floor(all_mins / 60)
        stats_writer.write_line()
        logger.info(
            f"Total Time: {hours}:{mins:02}:{secs:05.2F}"
            f" -- Blocks {starting_block.int_value:,} to {ending_block.int_value:,}"
            f" at block height {block_height.int_value:,}"
        )


class LineWriter:
    def __init__(self, block_bound_tracker: BlockBoundTracker) -> None:
        self.__block_bound_tracker = block_bound_tracker

    def __call__(self, stats_service: StatsService):
        low = self.__block_bound_tracker.low.int_value if self.__block_bound_tracker.low else None
        high = (
            self.__block_bound_tracker.high.int_value if self.__block_bound_tracker.high else None
        )
        return f"Blocks [{low:,}:{high:,}] -- {_get_load_stat_line(stats_service)}"


async def run_load(
    starting_block: HexInt,
    ending_block: HexInt,
    block_height: HexInt,
    increment_data_version: bool,
    block_chunk_size: int,
    stats_service: StatsService,
    block_time_cache: BlockTimeCache,
    evm_rpc_client: EvmRpcClient,
    boto3_session: aioboto3.Session,
    blockchain: BlockChain,
    dynamodb_endpoint_url: Optional[str],
    dynamodb_timeout: float,
    table_prefix: str,
    dynamodb_parallel_batches: int,
    block_bound_tracker: BlockBoundTracker,
) -> None:
    config = BotoConfig(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    base_resource_kwargs: Dict[str, Union[str, BotoConfig]] = {"config": config}

    token_transaction_type_oracle = TokenTransactionTypeOracle()
    log_version_oracle = LogVersionOracle()

    dynamodb_resource_kwargs = base_resource_kwargs.copy()
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        dynamodb_resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    async with boto3_session.resource(
        "dynamodb", **dynamodb_resource_kwargs
    ) as dynamodb:  # type: ignore
        data_service = DynamoDbDataService(
            dynamodb, stats_service, table_prefix, dynamodb_parallel_batches
        )
        data_version = await _get_data_version(  # noqa: F841
            dynamodb, blockchain, increment_data_version, table_prefix
        )

        async with evm_rpc_client:
            data_bus = ParallelDataBus()
            block_time_service = BlockTimeService(block_time_cache, evm_rpc_client)

            await data_bus.register(
                EvmBlockIdToEvmBlockAndEvmTransactionAndEvmTransactionHashTransformer(
                    data_bus=data_bus,
                    blockchain=blockchain,
                    rpc_client=evm_rpc_client,
                    block_time_service=block_time_service,
                ),
            )
            await data_bus.register(
                EvmTransactionToContractEvmTransactionReceiptTransformer(
                    data_bus=data_bus,
                    blockchain=blockchain,
                    rpc_client=evm_rpc_client,
                )
            )
            await data_bus.register(
                EvmTransactionReceiptToNftCollectionTransformer(
                    data_bus=data_bus,
                    blockchain=blockchain,
                    rpc_client=evm_rpc_client,
                    data_version=data_version,
                )
            )
            await data_bus.register(NftCollectionPersistenceConsumer(data_service))
            # Make sure batches are full as batches are 25 items
            dynamodb_write_batch_size = dynamodb_parallel_batches * 25
            # Make sure we don't exceed max hot partition value of 1,000
            dynamodb_max_concurrent_batches = math.floor(1_000 / dynamodb_write_batch_size)
            await data_bus.register(
                CollectionToEverythingElseErc721CollectionBasedConsumer(
                    data_service=data_service,
                    rpc_client=evm_rpc_client,
                    block_time_service=block_time_service,
                    log_version_oracle=log_version_oracle,
                    token_transaction_type_oracle=token_transaction_type_oracle,
                    max_block_height=block_height,
                    write_batch_size=dynamodb_write_batch_size,
                    max_concurrent_batch_writes=dynamodb_max_concurrent_batches,
                )
            )
            await data_bus.register(
                CollectionToEverythingElseErc1155CollectionBasedConsumer(
                    data_service=data_service,
                    rpc_client=evm_rpc_client,
                    block_time_service=block_time_service,
                    log_version_oracle=log_version_oracle,
                    token_transaction_type_oracle=token_transaction_type_oracle,
                    max_block_height=block_height,
                    write_batch_size=dynamodb_write_batch_size,
                    max_concurrent_batch_writes=dynamodb_max_concurrent_batches,
                )
            )

            if ending_block == starting_block:
                blocks: Iterable = [starting_block]
            else:
                blocks = [
                    HexInt(block_number)
                    for block_number in range(
                        ending_block.int_value, starting_block.int_value, -1 * block_chunk_size
                    )
                ]

            for block_chunk_start in blocks:
                block_chunk_end = block_chunk_start - block_chunk_size + 1
                if block_chunk_end < starting_block:
                    block_chunk_end = starting_block

                block_bound_tracker.low = block_chunk_end
                block_bound_tracker.high = block_chunk_start
                block_id_producer = BlockIDProducer(
                    blockchain, block_chunk_start, block_chunk_end, -1
                )
                async with data_bus:
                    await block_id_producer(data_bus)
