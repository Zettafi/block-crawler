import asyncio
import os
import pathlib
import time

import aioboto3
import click
import math

from blockcrawler.core.click import HexIntParamType
from blockcrawler.core.stats import StatsService, StatsWriter
from blockcrawler.core.types import HexInt
from blockcrawler.nft.bin import (
    BlockBoundTracker,
    Config,
    persist_block_time_cache,
    get_block_time_cache,
    get_load_stat_line,
)
from blockcrawler.nft.bin.commands import load_evm_contracts_by_block


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
    block_time_cache = get_block_time_cache(block_time_cache_filename, config)
    block_bound_tracker = BlockBoundTracker()
    stats_writer = StatsWriter(config.stats_service, LineWriter(block_bound_tracker))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    start = time.perf_counter()
    stats_task = loop.create_task(stats_writer.write_at_interval(60))
    try:
        loop.run_until_complete(
            load_evm_contracts_by_block(
                blockchain=config.blockchain,
                starting_block=starting_block,
                ending_block=ending_block,
                block_height=block_height,
                increment_data_version=increment_data_version,
                block_chunk_size=block_chunk_size,
                logger=config.logger,
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
        config.logger.info("Processing interrupted by user!")
    finally:
        stats_task.cancel()
        while loop.is_running():
            time.sleep(0.001)

        persist_block_time_cache(config, block_time_cache, block_time_cache_filename)
        end = time.perf_counter()
        runtime = end - start
        secs = runtime % 60
        all_mins = math.floor(runtime / 60)
        mins = all_mins % 60
        hours = math.floor(all_mins / 60)
        stats_writer.write_line()
        config.logger.info(
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
        return f"Blocks [{low,}:{high:,}] -- {get_load_stat_line(stats_service)}"
