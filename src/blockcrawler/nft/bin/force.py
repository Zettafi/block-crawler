"""
Command for force loading a collection
"""
import asyncio
import logging
import os
import pathlib
import time
from typing import Dict, Union

import aioboto3
import click
import math
from botocore.config import Config as BotoConfig
from hexbytes import HexBytes

from blockcrawler.core.bus import ParallelDataBus
from blockcrawler.core.click import (
    AddressParamType,
    HexIntParamType,
    EthereumCollectionTypeParamType,
    HexBytesParamType,
)
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.stats import StatsService, StatsWriter
from blockcrawler.core.types import HexInt, Address
from blockcrawler.evm.rpc import EvmRpcClient
from blockcrawler.evm.services import BlockTimeCache, BlockTimeService
from blockcrawler.nft.bin import (
    Config,
    get_block_time_cache,
    persist_block_time_cache,
    get_load_stat_line,
)
from blockcrawler.nft.bin.commands import get_data_version
from blockcrawler.nft.consumers import NftCollectionPersistenceConsumer
from blockcrawler.nft.data_packages import ForceLoadCollectionDataPackage
from blockcrawler.nft.data_services.dynamodb import DynamoDbDataService
from blockcrawler.nft.entities import CollectionType
from blockcrawler.nft.evm.consumers import (
    CollectionToEverythingElseErc721CollectionBasedConsumer,
    CollectionToEverythingElseErc1155CollectionBasedConsumer,
)
from blockcrawler.nft.evm.oracles import TokenTransactionTypeOracle, LogVersionOracle
from blockcrawler.nft.evm.transformers import (
    EvmForceLoadContractTransformer,
)


async def load_evm_contract_by_force(
    blockchain: BlockChain,
    collection_id: Address,
    creation_transaction_hash: HexBytes,
    block_height: HexInt,
    collection_type: CollectionType,
    logger: logging.Logger,
    stats_service: StatsService,
    block_time_cache: BlockTimeCache,
    evm_rpc_client: EvmRpcClient,
    boto3_session: aioboto3.Session,
    dynamodb_endpoint_url: str,
    dynamodb_timeout: float,
    table_prefix: str,
    dynamodb_parallel_batches: int,
):
    config = BotoConfig(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    base_resource_kwargs: Dict[str, Union[str, Config]] = {"config": config}

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
        data_version = await get_data_version(  # noqa: F841
            dynamodb, blockchain, False, table_prefix
        )

        async with evm_rpc_client:
            data_bus = ParallelDataBus(logger)
            block_time_service = BlockTimeService(block_time_cache, evm_rpc_client)

            await data_bus.register(
                EvmForceLoadContractTransformer(
                    data_bus=data_bus,
                    rpc_client=evm_rpc_client,
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

            async with data_bus:
                await data_bus.send(
                    ForceLoadCollectionDataPackage(
                        blockchain=blockchain,
                        collection_id=collection_id,
                        transaction_hash=creation_transaction_hash,
                        data_version=data_version,
                        default_collection_type=collection_type,
                    )
                )


@click.command
@click.argument("COLLECTION_ID", type=AddressParamType(), required=True)
@click.argument("CREATION_TX_HASH", type=HexBytesParamType(), required=True)
@click.argument("BLOCK_HEIGHT", type=HexIntParamType(), required=True)
@click.argument(
    "DEFAULT_COLLECTION_TYPE",
    type=EthereumCollectionTypeParamType(),
    required=False,
    default=CollectionType("UNKNOWN"),
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
def force_load(
    config: Config,
    collection_id: Address,
    creation_tx_hash: HexBytes,
    block_height: HexInt,
    default_collection_type: CollectionType,
    dynamodb_parallel_batches: int,
    block_time_cache_filename: pathlib.Path,
):
    """
    Load NFT data for a collection regardless of what type can be determined

    Load and NFT collection by the COLLECTION_ID from the BLOCKCHAIN. All event logs
    for a Collection will be processed up to the BLOCK_HEIGHT. Information from
    the transaction hash in which it was created, CREATION_TX_HASH, will be used
    to determine the creation data. The collection type will default to COLLECTION_TYPE
    if it cannot be determined.
    """
    block_time_cache = get_block_time_cache(block_time_cache_filename, config)

    stats_writer = StatsWriter(config.stats_service, get_load_stat_line)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    start = time.perf_counter()
    stats_task = loop.create_task(stats_writer.write_at_interval(60))
    try:
        loop.run_until_complete(
            load_evm_contract_by_force(
                blockchain=config.blockchain,
                collection_id=collection_id,
                creation_transaction_hash=creation_tx_hash,
                block_height=block_height,
                collection_type=default_collection_type,
                logger=config.logger,
                stats_service=config.stats_service,
                block_time_cache=block_time_cache,
                evm_rpc_client=config.evm_rpc_client,
                boto3_session=aioboto3.Session(),
                dynamodb_endpoint_url=config.dynamodb_endpoint_url,
                dynamodb_timeout=config.dynamodb_timeout,
                table_prefix=config.table_prefix,
                dynamodb_parallel_batches=dynamodb_parallel_batches,
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
            f" at block height {block_height.int_value:,}"
        )
