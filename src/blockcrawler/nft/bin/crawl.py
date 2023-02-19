import asyncio
import logging
import time
from asyncio import CancelledError
from logging import Logger
from typing import Dict, Union, Iterable

import aioboto3
import click
import math
from botocore.config import Config as BotoConfig

from blockcrawler import LOGGER_NAME
from blockcrawler.core.bus import SignalManager
from blockcrawler.core.click import HexIntParamType
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.stats import StatsService
from blockcrawler.core.types import HexInt
from blockcrawler.evm.producers import BlockIDProducer
from blockcrawler.evm.rpc import EvmRpcClient
from blockcrawler.nft.bin.shared import (
    Config,
    _get_data_version,
    BlockBoundTracker,
    _get_crawl_stat_line,
    _evm_block_crawler_data_bus_factory,
)


@click.command()
@click.argument("STARTING_BLOCK", type=HexIntParamType())
@click.argument("ENDING_BLOCK", type=HexIntParamType())
@click.option(
    "--increment-data-version",
    envvar="INCREMENT_DATA_VERSION",
    default=False,
    show_default=True,
    help="Increment the data version being processed. This ensures data integrity when "
    "reprocessing the blockchain. If you are running multiple crawlers, set the first "
    'to "True" to increment the version and the rest to "False" to use the version '
    "set by the first crawler.",
)
@click.option(
    "--block-chunk-size",
    envvar="BLOCK_CHUNK_SIZE",
    default=1_000,
    show_default=True,
    help="The size of block range in which the entire range will be divided.",
)
@click.pass_obj
def crawl(
    config: Config,
    starting_block: HexInt,
    ending_block: HexInt,
    increment_data_version: bool,
    block_chunk_size: int,
):
    """
    Crawl blocks and store the data.

    Crawl the block in the BLOCKCHAIN from the STARTING_BLOCK to the
    ENDING_BLOCK , parse the data we want to collect and put that data in the database
    """

    block_bound_tracker = BlockBoundTracker()
    stats_writer = StatsWriter(config.stats_service, block_bound_tracker)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    start = time.perf_counter()
    stats_task = loop.create_task(stats_writer.write_at_interval(60))
    try:
        loop.run_until_complete(
            run_crawl(
                logger=config.logger,
                stats_service=config.stats_service,
                rpc_client=config.evm_rpc_client,
                boto3_session=aioboto3.Session(),
                blockchain=config.blockchain,
                dynamodb_endpoint_url=config.dynamodb_endpoint_url,
                dynamodb_timeout=config.dynamodb_timeout,
                table_prefix=config.table_prefix,
                starting_block=starting_block,
                ending_block=ending_block,
                block_chunk_size=block_chunk_size,
                increment_data_version=increment_data_version,
                block_bound_tracker=block_bound_tracker,
            )
        )
    except KeyboardInterrupt:
        config.logger.info("Processing interrupted by user!")
    finally:
        stats_task.cancel()
        while loop.is_running():
            time.sleep(0.001)

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
        )


class StatsWriter:
    def __init__(self, stats_service: StatsService, block_bound_tracker: BlockBoundTracker) -> None:
        self.__stats_service = stats_service
        self.__block_bound_tracker = block_bound_tracker

    def write_line(self):
        stat_line = _get_crawl_stat_line(self.__stats_service)
        log_line = (
            f"Blocks ["
            f"{self.__block_bound_tracker.low.int_value:,}:"
            f"{self.__block_bound_tracker.high.int_value:,}"
            f"]"
            f" -- "
            f"{stat_line}"
        )
        logging.getLogger(LOGGER_NAME).info(log_line)

    async def write_at_interval(self, interval: int):
        try:
            while True:
                await asyncio.sleep(interval)
                self.write_line()
        except CancelledError:
            pass


async def run_crawl(
    logger: Logger,
    stats_service: StatsService,
    rpc_client: EvmRpcClient,
    boto3_session: aioboto3.Session,
    blockchain: BlockChain,
    dynamodb_endpoint_url: str,
    dynamodb_timeout: float,
    table_prefix: str,
    starting_block: HexInt,
    ending_block: HexInt,
    block_chunk_size: int,
    increment_data_version: bool,
    block_bound_tracker: BlockBoundTracker,
):
    config = BotoConfig(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    base_resource_kwargs: Dict[str, Union[str, BotoConfig]] = {"config": config}

    dynamodb_resource_kwargs = base_resource_kwargs.copy()
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        dynamodb_resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    async with boto3_session.resource(
        "dynamodb", **dynamodb_resource_kwargs
    ) as dynamodb:  # type: ignore
        data_version = await _get_data_version(  # noqa: F841
            dynamodb, blockchain, increment_data_version, table_prefix
        )

        async with rpc_client:
            data_bus = await _evm_block_crawler_data_bus_factory(
                stats_service=stats_service,
                dynamodb=dynamodb,
                table_prefix=table_prefix,
                logger=logger,
                rpc_client=rpc_client,
                blockchain=blockchain,
                data_version=data_version,
            )

            if ending_block == starting_block:
                blocks: Iterable = [starting_block]
            else:
                blocks = [
                    HexInt(block_number)
                    for block_number in range(
                        starting_block.int_value, ending_block.int_value, block_chunk_size
                    )
                ]
            with SignalManager() as signal_manager:
                for block_chunk_start in blocks:
                    block_chunk_end = block_chunk_start + block_chunk_size - 1
                    if block_chunk_end > ending_block:
                        block_chunk_end = ending_block

                    if signal_manager.interrupted:
                        break
                    block_bound_tracker.low = block_chunk_start
                    block_bound_tracker.high = block_chunk_end
                    block_id_producer = BlockIDProducer(
                        blockchain, block_chunk_start, block_chunk_end
                    )
                    async with data_bus:
                        await block_id_producer(data_bus)
