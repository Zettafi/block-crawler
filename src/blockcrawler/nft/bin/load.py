import asyncio
import csv
import logging
import os
import pathlib
import time
from asyncio import CancelledError

import aioboto3
import click
import math

from blockcrawler import LOGGER_NAME
from blockcrawler.core.click import BlockChainParamType, HexIntParamType
from blockcrawler.core.rpc import RpcClient
from blockcrawler.core.stats import StatsService
from blockcrawler.core.types import HexInt
from blockcrawler.evm.services import MemoryBlockTimeCache
from blockcrawler.nft import data_services
from blockcrawler.nft.bin import BlockBoundTracker, Config
from blockcrawler.nft.bin.commands import load_evm_contracts_by_block


@click.command()
@click.argument("BLOCKCHAIN", type=BlockChainParamType())
@click.argument("STARTING_BLOCK", type=HexIntParamType())
@click.argument("ENDING_BLOCK", type=HexIntParamType())
@click.argument("BLOCK_HEIGHT", type=HexIntParamType())
@click.option(
    "--increment-data-version",
    envvar="INCREMENT_DATA_VERSION",
    default=False,
    show_default=True,
    help="Increment the data version being processed. This ensures data integrity when "
    "reprocessing the blockchain. Running multiple instances may require the first instance"
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
    default=20,
    show_default=True,
)
@click.option(
    "--block-time-cache-filename",
    envvar="BLOCK_TIME_CACHE_FILENAME",
    default=f"{os.getcwd()}{os.sep}block-time-cache.csv",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False, path_type=pathlib.Path),
    help="Location and filename for the block time cache. Defaults location is the directory"
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
    config.logger.info(f"Loading block time cache from file {block_time_cache_filename}")
    block_time_cache = MemoryBlockTimeCache()
    try:
        with open(block_time_cache_filename, "r") as file:
            for block_id, timestamp in csv.reader(file):
                block_time_cache.set_sync(int(block_id), int(timestamp))
        config.logger.info(f"Loaded block time cache from file {block_time_cache_filename}")
    except FileNotFoundError:
        config.logger.warning(f"File {block_time_cache_filename} does not exist.")

    block_bound_tracker = BlockBoundTracker()
    stats_writer = StatsWriter(config.stats_service, block_bound_tracker)
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

        config.logger.info(f"Saving block time cache to file {block_time_cache_filename}")
        with open(block_time_cache_filename, "w+") as file:
            csv_writer = csv.writer(file)
            for row in block_time_cache:
                csv_writer.writerow(row)
        config.logger.info(f"Saved block time cache to file {block_time_cache_filename}")

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


class StatsWriter:
    def __init__(self, stats_service: StatsService, block_bound_tracker: BlockBoundTracker) -> None:
        self.__stats_service = stats_service
        self.__block_bound_tracker = block_bound_tracker

    @staticmethod
    def __safe_average(count, total):
        return 0.0 if count == 0 else total / count

    def write_line(self):
        rpc_connect = self.__stats_service.get_count(RpcClient.STAT_CONNECT)
        rpc_reconnect = self.__stats_service.get_count(RpcClient.STAT_RECONNECT)
        rpc_connection_reset = self.__stats_service.get_count(RpcClient.STAT_CONNECTION_RESET)
        rpc_throttled = self.__stats_service.get_count(RpcClient.STAT_RESPONSE_TOO_MANY_REQUESTS)
        rpc_sent = self.__stats_service.get_count(RpcClient.STAT_REQUEST_SENT)
        rpc_delayed = self.__stats_service.get_count(RpcClient.STAT_REQUEST_DELAYED)
        rpc_received = self.__stats_service.get_count(RpcClient.STAT_RESPONSE_RECEIVED)
        rpc_request_ms = self.__stats_service.get_count(RpcClient.STAT_REQUEST_MS)
        rpc_request_ms_avg = self.__safe_average(rpc_received, rpc_request_ms)
        collection_count = self.__stats_service.get_count(data_services.STAT_COLLECTION_WRITE)
        collection_ms = self.__stats_service.get_count(data_services.STAT_COLLECTION_WRITE_MS)
        collection_ms_avg = self.__safe_average(collection_count, collection_ms)
        token_count = self.__stats_service.get_count(data_services.STAT_TOKEN_WRITE_BATCH)
        token_ms = self.__stats_service.get_count(data_services.STAT_TOKEN_WRITE_BATCH_MS)
        token_ms_avg = self.__safe_average(token_count, token_ms)
        transfer_count = self.__stats_service.get_count(
            data_services.STAT_TOKEN_TRANSFER_WRITE_BATCH
        )
        transfer_ms = self.__stats_service.get_count(
            data_services.STAT_TOKEN_TRANSFER_WRITE_BATCH_MS
        )
        transfer_ms_avg = self.__safe_average(transfer_count, transfer_ms)
        owner_count = self.__stats_service.get_count(data_services.STAT_TOKEN_OWNER_WRITE_BATCH)
        owner_ms = self.__stats_service.get_count(data_services.STAT_TOKEN_OWNER_WRITE_BATCH_MS)
        owner_ms_avg = self.__safe_average(owner_count, owner_ms)
        write_delayed = self.__stats_service.get_count(data_services.STAT_WRITE_DELAYED)
        logging.getLogger(LOGGER_NAME).info(
            f"Blocks ["
            f"{self.__block_bound_tracker.low.int_value:,}:"
            f"{self.__block_bound_tracker.high.int_value:,}"
            f"]"
            f" -- "
            f"Conn ["
            f"C:{rpc_connect:,} "
            f"X:{rpc_reconnect:,} "
            f"R:{rpc_connection_reset:,}"
            f"]"
            f" RPC ["
            f"S:{rpc_sent:,} "
            f"D:{rpc_delayed:,} "
            f"T:{rpc_throttled:,} "
            f"R:{rpc_received:,}/{rpc_request_ms_avg:,.0F}"
            f"]"
            f" -- "
            f"Write ["
            f"D:{write_delayed:,} "
            f"C:{collection_count:,}/{collection_ms_avg :,.0F} "
            f"T:{token_count :,}/{token_ms_avg :,.0F} "
            f"X:{transfer_count:,}/{transfer_ms_avg :,.0F} "
            f"O:{owner_count:,}/{owner_ms_avg :,.0F}"
            f"]"
        )

    async def write_at_interval(self, interval: int):
        try:
            while True:
                await asyncio.sleep(interval)
                self.write_line()
        except CancelledError:
            pass
