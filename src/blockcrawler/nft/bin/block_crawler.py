import asyncio
import csv
import dataclasses
import logging
import math
import os
import pathlib
import sys
import time
from asyncio import CancelledError
from logging import StreamHandler
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, List, Tuple

import click

from blockcrawler import LOGGER_NAME
from blockcrawler.core.click import BlockChainParamType, HexIntParamType
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.types import HexInt
from blockcrawler.core.rpc import RpcClient
from blockcrawler.evm.services import MemoryBlockTimeCache
from blockcrawler.core.stats import StatsService
from blockcrawler.nft import data_services
from blockcrawler.nft.bin.commands import (
    crawl_evm_blocks,
    load_evm_contracts_by_block,
    listen_for_and_process_new_evm_blocks,
    set_last_block_id_for_block_chain,
)

try:  # If dotenv in installed, use it load env vars
    # noinspection PyPackageRequirements
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


@click.group
def nft():
    """
    Loading and verifying NFTs
    """
    pass


@nft.command()
@click.argument("STARTING_BLOCK", type=HexInt)
@click.argument("ENDING_BLOCK", type=HexInt)
@click.option(
    "--blockchain",
    envvar="BLOCKCHAIN",
    help="Blockchain that will be processed",
    required=True,
    type=BlockChainParamType(),
)
@click.option(
    "--evm-archive-node-uri",
    envvar="EVM_ARCHIVE_NODE_URI",
    help="URI to access the archive node EVM RPC HTTP server",
)
@click.option(
    "--rpc-requests-per-second",
    envvar="RPC_REQUESTS_PER_SECOND",
    help="The maximum number of requests to process per second",
    default=None,
    type=int,
)
@click.option(
    "--dynamodb-endpoint-url",
    envvar="AWS_DYNAMODB_ENDPOINT_URL",
    help="Override URL for connecting to Amazon DynamoDB",
)
@click.option(
    "--s3-endpoint-url",
    envvar="AWS_S3_ENDPOINT_URL",
    help="Override URL for connecting to Amazon S3",
)
@click.option(
    "--dynamodb-timeout",
    envvar="DYNAMODB_TIMEOUT",
    default=5.0,
    help="Maximum time in seconds to wait for connect or response from DynamoDB",
)
@click.option(
    "--dynamodb-region",
    envvar="AWS_REGION",
    help="AWS region for DynamoDB",
)
@click.option("--table-prefix", envvar="TABLE_PREFIX", help="Prefix for table names", default="")
@click.option(
    "--increment-data-version",
    envvar="INCREMENT_DATA_VERSION",
    default=True,
    show_default=True,
    help="Increment the data version being processed. This ensures data integrity when "
    "reprocessing the blockchain. If you are running multiple crawlers, set the first "
    'to "True" to increment the version and the rest to "False" to use the version '
    "set by the first crawler.",
)
@click.option(
    "--debug/--no-debug",
    envvar="DEBUG",
    default=False,
    show_default=True,
    help="Show debug messages in the console.",
)
def crawl(
    starting_block: HexInt,
    ending_block: HexInt,
    blockchain: BlockChain,
    evm_archive_node_uri: str,
    rpc_requests_per_second: Optional[int],
    dynamodb_timeout: float,
    dynamodb_endpoint_url: str,
    dynamodb_region: str,
    table_prefix: str,
    increment_data_version: bool,
    debug: bool,
):
    """
    Crawl blocks and store the data.

    Crawl the block in the BLOCKCHAIN from the STARTING_BLOCK to the
    ENDING_BLOCK , parse the data we want to collect and put that data in the database
    """
    log_handler = StreamHandler(sys.stdout)
    logger = logging.getLogger(LOGGER_NAME)
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    stats_service = StatsService()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            crawl_evm_blocks(
                logger=logger,
                stats_service=stats_service,
                archive_node_uri=evm_archive_node_uri,
                rpc_requests_per_second=rpc_requests_per_second,
                blockchain=blockchain,
                dynamodb_endpoint_url=dynamodb_endpoint_url,
                dynamodb_region=dynamodb_region,
                dynamodb_timeout=dynamodb_timeout,
                table_prefix=table_prefix,
                starting_block=starting_block,
                ending_block=ending_block,
                increment_data_version=increment_data_version,
            )
        )
    except KeyboardInterrupt:
        pass


@nft.command()
@click.option(
    "--blockchain",
    envvar="BLOCKCHAIN",
    help="Blockchain that will be processed",
    required=True,
    type=BlockChainParamType(),
)
@click.option(
    "--evm-archive-node-uri",
    envvar="EVM_ARCHIVE_NODE_URI",
    help="URI to access the archive node EVM RPC HTTP server",
)
@click.option(
    "--rpc-requests-per-second",
    envvar="RPC_REQUESTS_PER_SECOND",
    help="The maximum number of requests to process per second",
    default=None,
    type=int,
)
@click.option(
    "--dynamodb-endpoint-url",
    envvar="AWS_DYNAMODB_ENDPOINT_URL",
    help="Override URL for connecting to Amazon DynamoDB",
)
@click.option(
    "--dynamodb-timeout",
    envvar="DYNAMODB_TIMEOUT",
    default=5.0,
    help="Maximum time in seconds to wait for connect or response from DynamoDB",
)
@click.option(
    "--dynamodb-region",
    envvar="AWS_DYNAMODB_REGION",
    help="AWS region for DynamoDB",
)
@click.option(
    "--trail-blocks",
    envvar="TRAIL_BOCKS",
    default=1,
    show_default=True,
    help="Trail the last block by this many blocks.",
)
@click.option(
    "--process-interval",
    envvar="PROCESS_INTERVAL",
    default=10.0,
    show_default=True,
    help="Minimum interval in seconds between block processing actions.",
)
@click.option(
    "--debug/--no-debug",
    envvar="DEBUG",
    default=False,
    show_default=True,
    help="Show debug messages in the console.",
)
def tail(
    evm_archive_node_uri: str,
    rpc_requests_per_second: Optional[int],
    blockchain: BlockChain,
    dynamodb_endpoint_url: str,
    dynamodb_region: str,
    dynamodb_timeout: float,
    table_prefix: str,
    trail_blocks: int,
    process_interval: int,
    debug: bool,
):
    """
    Process new blocks in the blockchain
    Listen for incoming blocks in the blockchain, parse the data we want to collect
    and store that data in the database
    """
    log_handler = StreamHandler(sys.stdout)
    logger = logging.getLogger("block_crawler")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.addHandler(log_handler)
    stats_service = StatsService()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            listen_for_and_process_new_evm_blocks(
                logger=logger,
                stats_service=stats_service,
                archive_node_uri=evm_archive_node_uri,
                rpc_requests_per_second=rpc_requests_per_second,
                blockchain=blockchain,
                dynamodb_endpoint_url=dynamodb_endpoint_url,
                dynamodb_region=dynamodb_region,
                dynamodb_timeout=dynamodb_timeout,
                table_prefix=table_prefix,
                trail_blocks=trail_blocks,
                process_interval=process_interval,
            )
        )
    except KeyboardInterrupt:
        pass


@nft.command()
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
    default=10_000,
    show_default=True,
    help="The size of block range in which the entire range will be divided.",
)
@click.option(
    "--evm-rpc-nodes",
    envvar="EVM_RPC_NODES",
    help="RPC Node URI and the number of connections",
    multiple=True,
    type=click.Tuple([str, int]),
)
@click.option(
    "--rpc-requests-per-second",
    envvar="RPC_REQUESTS_PER_SECOND",
    help="The maximum number of requests to process per second",
    default=None,
    type=int,
)
@click.option(
    "--dynamodb-endpoint-url",
    envvar="AWS_DYNAMODB_ENDPOINT_URL",
    help="Override URL for connecting to Amazon DynamoDB",
)
@click.option(
    "--dynamodb-timeout",
    envvar="DYNAMODB_TIMEOUT",
    default=5.0,
    help="Maximum time in seconds to wait for connect or response from DynamoDB",
)
@click.option(
    "--dynamodb-region",
    envvar="AWS_DYNAMODB_REGION",
    help="AWS region for DynamoDB",
)
@click.option("--table-prefix", envvar="TABLE_PREFIX", help="Prefix for table names", default="")
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
@click.option(
    "--log-file",
    envvar="LOG_FILE",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False, path_type=pathlib.Path),
    multiple=True,
    help="Location and filename for a log.",
)
@click.option(
    "--debug/--no-debug",
    envvar="DEBUG",
    default=False,
    show_default=True,
    help="Show debug messages in the console.",
)
def load(
    blockchain: BlockChain,
    starting_block: HexInt,
    ending_block: HexInt,
    block_height: HexInt,
    block_chunk_size: int,
    increment_data_version: bool,
    evm_rpc_nodes: List[Tuple[str, int]],
    rpc_requests_per_second: Optional[int],
    dynamodb_timeout: float,
    dynamodb_endpoint_url: str,
    dynamodb_region: str,
    table_prefix: str,
    dynamodb_parallel_batches: int,
    block_time_cache_filename: pathlib.Path,
    log_file: List[pathlib.Path],
    debug: bool,
):
    """
    Load NFT data in the fastest manner possible

    Load NFT collections from the BLOCKCHAIN from the STARTING_BLOCK to the
    ENDING_BLOCK. All event logs for a Collection will be processed up to the
    BLOCK_HEIGHT. Multiple runs will require the same data version and BLOCK_HEIGHT to
    ensure accurate data.
    """
    logger = logging.getLogger(LOGGER_NAME)
    handlers: List[logging.Handler] = [StreamHandler()]
    for filename in log_file:
        handlers.append(TimedRotatingFileHandler(filename, when="D"))
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logging.basicConfig(format="%(asctime)s %(message)s", handlers=handlers)

    logger.info(f"Loading block time cache from file {block_time_cache_filename}")
    block_time_cache = MemoryBlockTimeCache()
    try:
        with open(block_time_cache_filename, "r") as file:
            for block_id, timestamp in csv.reader(file):
                block_time_cache.set_sync(int(block_id), int(timestamp))
        logger.info(f"Loaded block time cache from file {block_time_cache_filename}")
    except FileNotFoundError:
        logger.warning(f"File {block_time_cache_filename} does not exist.")

    stats_service = StatsService()
    block_bound_tracker = BlockBoundTracker()
    stats_writer = StatsWriter(stats_service, block_bound_tracker)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    start = time.perf_counter()
    stats_task = loop.create_task(stats_writer.write_at_interval(60))
    try:
        loop.run_until_complete(
            load_evm_contracts_by_block(
                blockchain=blockchain,
                starting_block=starting_block,
                ending_block=ending_block,
                block_height=block_height,
                increment_data_version=increment_data_version,
                block_chunk_size=block_chunk_size,
                logger=logger,
                stats_service=stats_service,
                block_time_cache=block_time_cache,
                archive_nodes=evm_rpc_nodes,
                rpc_requests_per_second=rpc_requests_per_second,
                dynamodb_endpoint_url=dynamodb_endpoint_url,
                dynamodb_region=dynamodb_region,
                dynamodb_timeout=dynamodb_timeout,
                table_prefix=table_prefix,
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

        logger.info(f"Saving block time cache to file {block_time_cache_filename}")
        with open(block_time_cache_filename, "w+") as file:
            csv_writer = csv.writer(file)
            for row in block_time_cache:
                csv_writer.writerow(row)
        logger.info(f"Saved block time cache to file {block_time_cache_filename}")

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


@nft.command()
@click.argument("LAST_BLOCK_ID", type=HexInt)
@click.option(
    "--dynamodb-endpoint-url",
    envvar="AWS_DYNAMODB_ENDPOINT_URL",
    help="Override URL for connecting to Amazon Web Services",
)
@click.option(
    "--blockchain",
    envvar="BLOCKCHAIN",
    help="Blockchain that will be processed",
    required=True,
    type=BlockChainParamType(),
)
@click.option("--table-prefix", envvar="TABLE_PREFIX", help="Prefix for table names", default="")
def seed(blockchain, last_block_id: HexInt, dynamodb_endpoint_url, table_prefix):
    """
    Set the LAST_BLOCK_ID processed for the blockchain in the database. The
    listen command will use ths value to process blocks after this bock.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        set_last_block_id_for_block_chain(
            blockchain.value, last_block_id, dynamodb_endpoint_url, table_prefix
        )
    )


@dataclasses.dataclass
class BlockBoundTracker:
    low: Optional[HexInt] = HexInt(0)
    high: Optional[HexInt] = HexInt(0)


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


if __name__ == "__main__":
    nft()
