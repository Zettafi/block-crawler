import asyncio
import time
from logging import Logger
from typing import Dict, Union, cast

import aioboto3
import boto3
import click
from boto3.dynamodb.table import TableResource
from botocore.config import Config as BotoConfig

from blockcrawler.core.bus import SignalManager
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.stats import StatsService
from blockcrawler.core.types import HexInt
from blockcrawler.evm.data_packages import EvmBlockIDDataPackage
from blockcrawler.evm.rpc import EvmRpcClient

from blockcrawler.nft.bin.shared import (
    Config,
    _get_data_version,
    _get_crawl_stat_line,
    _update_latest_block,
    _evm_block_crawler_data_bus_factory,
)
from blockcrawler.nft.data.models import BlockCrawlerConfig


@click.command()
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
@click.pass_obj
def tail(
    config: Config,
    trail_blocks: int,
    process_interval: int,
):
    """
    Process new blocks in the blockchain
    Listen for incoming blocks in the blockchain, parse the data we want to collect
    and store that data in the database
    """
    config.logger.info("Process initializing")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        run_tail(
            logger=config.logger,
            stats_service=config.stats_service,
            evm_rpc_client=config.evm_rpc_client,
            boto3_session=aioboto3.Session(),
            blockchain=config.blockchain,
            dynamodb_endpoint_url=config.dynamodb_endpoint_url,
            dynamodb_timeout=config.dynamodb_timeout,
            table_prefix=config.table_prefix,
            trail_blocks=trail_blocks,
            process_interval=process_interval,
        )
    )
    while loop.is_running():
        time.sleep(0.001)
    config.logger.info("Process complete")


async def run_tail(
    logger: Logger,
    stats_service: StatsService,
    evm_rpc_client: EvmRpcClient,
    boto3_session: boto3.Session,
    blockchain: BlockChain,
    dynamodb_endpoint_url: str,
    dynamodb_timeout: float,
    table_prefix: str,
    trail_blocks: int,
    process_interval: int,
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
            dynamodb, blockchain, False, table_prefix
        )
        async with evm_rpc_client:
            data_bus = await _evm_block_crawler_data_bus_factory(
                stats_service=stats_service,
                dynamodb=dynamodb,
                table_prefix=table_prefix,
                logger=logger,
                rpc_client=evm_rpc_client,
                blockchain=blockchain,
                data_version=data_version,
            )

            last_block_table: TableResource = await dynamodb.Table(
                f"{table_prefix}{BlockCrawlerConfig.table_name}"
            )
            last_block_result = await last_block_table.get_item(
                Key={"blockchain": blockchain.value}
            )
            try:
                last_block_processed = HexInt(
                    int(last_block_result.get("Item").get("last_block_id"))
                )
            except AttributeError:
                logger.error(
                    f"Unable to retrieve last_block_id for blockchain "
                    f"{blockchain.value} from {last_block_table.table_name}"
                )
                exit(1)

            logger.info(
                f"Starting tail of {blockchain.value} trailing {trail_blocks} blocks "
                f"with {process_interval} sec interval"
            )
            with SignalManager() as signal_manager:
                block_processing = last_block_processed + 1  # Start processing the next block
                current_block_number = block_processing  # Set equal to trigger get_block_number
                total_process_time: float = float(
                    process_interval
                )  # Set equal to make initial delay 0
                while not signal_manager.interrupted:
                    stats_service.reset()
                    if block_processing >= current_block_number:
                        # If we've processed all the blocks we know of, see if new blocks exist

                        # Determine the remaining portion of the interval
                        sleep_time = process_interval - total_process_time
                        if sleep_time > 0.0:
                            # Sleep for the remaining portion of the interval
                            await asyncio.sleep(sleep_time)
                        total_process_time = 0.0
                        block_number = await evm_rpc_client.get_block_number()
                        current_block_number = block_number - trail_blocks
                    if last_block_processed < current_block_number:
                        start: float = time.perf_counter()
                        async with data_bus:
                            await data_bus.send(EvmBlockIDDataPackage(blockchain, block_processing))
                        end: float = time.perf_counter()
                        process_time: float = end - start
                        total_process_time += process_time
                        logger.info(
                            f"{block_processing.int_value}/{current_block_number.int_value}"
                            f" - {process_time:0.3f}s - {_get_crawl_stat_line(stats_service)}"
                        )

                        await _update_latest_block(
                            boto3_session,
                            cast(str, blockchain.value),
                            block_processing,
                            dynamodb_endpoint_url,
                            table_prefix,
                        )
                        block_processing += 1

                    else:
                        logger.warning(
                            f"No blocks to process -- current: {current_block_number.int_value}"
                            f" -- last processed: {last_block_processed.int_value}"
                        )
