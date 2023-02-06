import asyncio
import datetime
import math
import time
from logging import Logger
from typing import Union, Dict, cast, Optional, List, Iterable, Tuple

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from blockcrawler.nft.bin import BlockBoundTracker
from blockcrawler.nft.consumers import (
    NftCollectionPersistenceConsumer,
    NftTokenMintPersistenceConsumer,
    NftTokenTransferPersistenceConsumer,
    NftTokenQuantityUpdatingConsumer,
    NftMetadataUriUpdatingConsumer,
)
from blockcrawler.nft.data_services.dynamodb import DynamoDbDataService
from blockcrawler.nft.entities import BlockChain
from blockcrawler.nft.evm.consumers import (
    CollectionToEverythingElseErc721CollectionBasedConsumer,
    CollectionToEverythingElseErc1155CollectionBasedConsumer,
)
from blockcrawler.nft.evm.oracles import LogVersionOracle, TokenTransactionTypeOracle
from blockcrawler.nft.evm.transformers import (
    EvmTransactionReceiptToNftCollectionTransformer,
    EvmLogErc721TransferToNftTokenTransferTransformer,
    EvmLogErc1155TransferSingleToNftTokenTransferTransformer,
    EvmLogErc1155TransferToNftTokenTransferTransformer,
    EvmLogErc1155UriEventToNftTokenMetadataUriUpdatedTransformer,
    Erc721TokenTransferToNftTokenMetadataUriUpdatedTransformer,
)
from blockcrawler.core.bus import ParallelDataBus
from blockcrawler.core.types import HexInt
from blockcrawler.evm.services import BlockTimeService, BlockTimeCache
from blockcrawler.core.stats import StatsService
from blockcrawler.nft.data.models import BlockCrawlerConfig, Tokens
from blockcrawler.evm.producers import BlockIDProducer
from blockcrawler.evm.rpc import EvmRpcClient, ConnectionPoolingEvmRpcClient
from blockcrawler.evm.transformers import (
    BlockIdToEvmBlockTransformer,
    EvmBlockToEvmTransactionHashTransformer,
    EvmTransactionHashToEvmTransactionReceiptTransformer,
    EvmBlockIdToEvmBlockAndEvmTransactionAndEvmTransactionHashTransformer,
    EvmTransactionToContractEvmTransactionReceiptTransformer,
)


async def __evm_block_crawler_data_bus_factory(
    stats_service: StatsService,
    dynamodb,
    table_prefix: str,
    logger: Logger,
    rpc_client: EvmRpcClient,
    blockchain: BlockChain,
    data_version: int,
):
    data_bus = ParallelDataBus(logger)
    data_service = DynamoDbDataService(dynamodb, stats_service, table_prefix)
    await data_bus.register(
        BlockIdToEvmBlockTransformer(
            data_bus=data_bus, blockchain=blockchain, rpc_client=rpc_client
        ),
    )
    await data_bus.register(EvmBlockToEvmTransactionHashTransformer(data_bus))
    await data_bus.register(
        EvmTransactionHashToEvmTransactionReceiptTransformer(
            data_bus=data_bus, blockchain=blockchain, rpc_client=rpc_client
        ),
    )
    await data_bus.register(
        EvmTransactionReceiptToNftCollectionTransformer(
            data_bus=data_bus,
            blockchain=blockchain,
            rpc_client=rpc_client,
            data_version=data_version,
        )
    )
    await data_bus.register(NftCollectionPersistenceConsumer(data_service))
    token_transaction_type_oracle = TokenTransactionTypeOracle()
    log_version_oracle = LogVersionOracle()
    await data_bus.register(
        EvmLogErc721TransferToNftTokenTransferTransformer(
            data_bus=data_bus,
            data_version=data_version,
            transaction_type_oracle=token_transaction_type_oracle,
            version_oracle=log_version_oracle,
        )
    )
    await data_bus.register(
        EvmLogErc1155TransferSingleToNftTokenTransferTransformer(
            data_bus=data_bus,
            data_version=data_version,
            transaction_type_oracle=token_transaction_type_oracle,
            version_oracle=log_version_oracle,
        )
    )
    await data_bus.register(
        EvmLogErc1155TransferToNftTokenTransferTransformer(
            data_bus=data_bus,
            data_version=data_version,
            transaction_type_oracle=token_transaction_type_oracle,
            version_oracle=log_version_oracle,
        )
    )
    await data_bus.register(
        EvmLogErc1155UriEventToNftTokenMetadataUriUpdatedTransformer(
            data_bus=data_bus,
            log_version_oracle=log_version_oracle,
            data_version=data_version,
        )
    )
    await data_bus.register(NftTokenTransferPersistenceConsumer(data_service))
    tokens_table_resource = await dynamodb.Table(table_prefix + Tokens.table_name)
    await data_bus.register(NftTokenMintPersistenceConsumer(data_service))
    await data_bus.register(NftTokenQuantityUpdatingConsumer(tokens_table_resource))
    await data_bus.register(
        Erc721TokenTransferToNftTokenMetadataUriUpdatedTransformer(
            data_bus=data_bus,
            rpc_client=rpc_client,
        )
    )
    await data_bus.register(NftMetadataUriUpdatingConsumer(tokens_table_resource))
    await data_bus.register(
        EvmLogErc1155UriEventToNftTokenMetadataUriUpdatedTransformer(
            data_bus=data_bus,
            log_version_oracle=log_version_oracle,
            data_version=data_version,
        )
    )
    return data_bus


async def get_data_version(
    dynamodb, blockchain: BlockChain, increment_data_version: bool, table_prefix: str
):
    config_table = await dynamodb.Table(table_prefix + BlockCrawlerConfig.table_name)
    if increment_data_version:
        try:
            result = await config_table.update_item(
                Key={"blockchain": blockchain.value},
                UpdateExpression="SET data_version = data_version + :inc",
                ExpressionAttributeValues={":inc": 1},
                ReturnValues="UPDATED_NEW",
            )
        except ClientError as e:
            if e.response.get("Error", {}).get("Code", None) == "ValidationException":
                result = await config_table.update_item(
                    Key={"blockchain": blockchain.value},
                    UpdateExpression="SET data_version = :version",
                    ExpressionAttributeValues={":version": 1},
                    ReturnValues="UPDATED_NEW",
                )
            else:
                raise
        version = result["Attributes"]["data_version"]
    else:
        result = await config_table.get_item(
            Key={"blockchain": blockchain.value},
        )
        version = result["Item"]["data_version"]
    return version


async def crawl_evm_blocks(
    logger: Logger,
    stats_service: StatsService,
    archive_node_uri: str,
    rpc_requests_per_second: Optional[int],
    blockchain: BlockChain,
    dynamodb_endpoint_url: str,
    dynamodb_region: str,
    dynamodb_timeout: float,
    table_prefix: str,
    starting_block: HexInt,
    ending_block: HexInt,
    increment_data_version: bool,
):
    session = aioboto3.Session()

    config = Config(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    base_resource_kwargs: Dict[str, Union[str, Config]] = {"config": config}

    dynamodb_resource_kwargs = base_resource_kwargs.copy()
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        dynamodb_resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    if dynamodb_region is not None:
        dynamodb_resource_kwargs["region_name"] = dynamodb_region
    async with session.resource("dynamodb", **dynamodb_resource_kwargs) as dynamodb:  # type: ignore
        data_version = await get_data_version(  # noqa: F841
            dynamodb, blockchain, increment_data_version, table_prefix
        )

        async with EvmRpcClient(
            archive_node_uri, stats_service, rpc_requests_per_second
        ) as rpc_client:
            # TODO: Process chunks and allow for graceful stop
            # TODO: Report on progress
            data_bus = await __evm_block_crawler_data_bus_factory(
                stats_service=stats_service,
                dynamodb=dynamodb,
                table_prefix=table_prefix,
                logger=logger,
                rpc_client=rpc_client,
                blockchain=blockchain,
                data_version=data_version,
            )
            # await data_bus.register(
            #     DebugConsumer(
            #         lambda package: isinstance(package, NftTokenMetadataUriUpdatedDataPackage)
            #     )
            # )

            block_id_producer = BlockIDProducer(blockchain, starting_block, ending_block)
            async with data_bus:
                await block_id_producer(data_bus)


async def listen_for_and_process_new_evm_blocks(
    logger: Logger,
    stats_service: StatsService,
    archive_node_uri: str,
    rpc_requests_per_second: Optional[int],
    blockchain: BlockChain,
    dynamodb_endpoint_url: str,
    dynamodb_region: str,
    dynamodb_timeout: float,
    table_prefix: str,
    trail_blocks: int,
    process_interval: int,
):
    session = aioboto3.Session()

    config = Config(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    base_resource_kwargs: Dict[str, Union[str, Config]] = {"config": config}

    dynamodb_resource_kwargs = base_resource_kwargs.copy()
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        dynamodb_resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    if dynamodb_region is not None:
        dynamodb_resource_kwargs["region_name"] = dynamodb_region
    async with session.resource("dynamodb", **dynamodb_resource_kwargs) as dynamodb:  # type: ignore
        data_version = await get_data_version(  # noqa: F841
            dynamodb, blockchain, False, table_prefix
        )
        async with EvmRpcClient(
            provider_url=archive_node_uri,
            stats_service=stats_service,
            requests_per_second=rpc_requests_per_second,
        ) as rpc_client:
            data_bus = await __evm_block_crawler_data_bus_factory(
                stats_service=stats_service,
                dynamodb=dynamodb,
                table_prefix=table_prefix,
                logger=logger,
                rpc_client=rpc_client,
                blockchain=blockchain,
                data_version=data_version,
            )

            last_block_table = await dynamodb.Table(BlockCrawlerConfig.table_name)
            last_block_result = await last_block_table.get_item(
                Key={"blockchain": blockchain.value}
            )
            try:
                last_block_processed = HexInt(
                    int(last_block_result.get("Item").get("last_block_id"))
                )
            except AttributeError:
                exit(
                    "Unable to determine the last block number processed. "
                    "Are you starting fresh and forgot to seed?"
                )

            process_time: float = 0.0
            caught_up = False
            while True:
                # TODO: Gracefully handle shutdown
                block_number = await rpc_client.get_block_number()
                current_block_number = block_number - trail_blocks
                if last_block_processed < current_block_number:
                    start_block = last_block_processed + 1
                    block_ids = current_block_number - start_block + 1
                    if not caught_up and block_ids > 1:
                        print(f"Catching up {block_ids} blocks")
                    start = time.perf_counter()
                    block_id_producer = BlockIDProducer(
                        blockchain, start_block, current_block_number
                    )
                    async with data_bus:
                        await block_id_producer(data_bus)

                    end = time.perf_counter()
                    process_time = end - start
                    print(
                        datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S%z")
                        + f" - {start_block}:{current_block_number}"
                        f" - {process_time:0.3f}s"
                        f" - blk:{block_ids:,}"
                    )
                    last_block_processed = current_block_number

                    await set_last_block_id_for_block_chain(
                        cast(str, blockchain.value),
                        last_block_processed,
                        dynamodb_endpoint_url,
                        table_prefix,
                    )
                caught_up = True
                await asyncio.sleep(process_interval - process_time)


async def set_last_block_id_for_block_chain(
    blockchain: str, last_block_id: HexInt, dynamodb_endpoint_url: str, table_prefix: str
):
    resource_kwargs = {}
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    session = aioboto3.Session()
    async with session.resource("dynamodb", **resource_kwargs) as dynamodb:  # type: ignore
        block_crawler_config = await dynamodb.Table(table_prefix + BlockCrawlerConfig.table_name)
        await block_crawler_config.update_item(
            Key={"blockchain": blockchain},
            UpdateExpression="SET last_block_id = :block_id",
            ExpressionAttributeValues={":block_id": last_block_id.int_value},
        )


async def load_evm_contracts_by_block(
    starting_block: HexInt,
    ending_block: HexInt,
    block_height: HexInt,
    increment_data_version: bool,
    block_chunk_size: int,
    logger: Logger,
    stats_service: StatsService,
    block_time_cache: BlockTimeCache,
    archive_nodes: List[Tuple[str, int]],
    rpc_requests_per_second: Optional[int],
    blockchain: BlockChain,
    dynamodb_endpoint_url: Optional[str],
    dynamodb_region: str,
    dynamodb_timeout: float,
    table_prefix: str,
    dynamodb_parallel_batches: int,
    block_bound_tracker: BlockBoundTracker,
) -> None:
    session = aioboto3.Session()

    config = Config(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    base_resource_kwargs: Dict[str, Union[str, Config]] = {"config": config}

    token_transaction_type_oracle = TokenTransactionTypeOracle()
    log_version_oracle = LogVersionOracle()

    dynamodb_resource_kwargs = base_resource_kwargs.copy()
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        dynamodb_resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    if dynamodb_region is not None:
        dynamodb_resource_kwargs["region_name"] = dynamodb_region
    async with session.resource("dynamodb", **dynamodb_resource_kwargs) as dynamodb:  # type: ignore
        data_service = DynamoDbDataService(
            dynamodb, stats_service, table_prefix, dynamodb_parallel_batches
        )
        data_version = await get_data_version(  # noqa: F841
            dynamodb, blockchain, increment_data_version, table_prefix
        )

        rpc_clients: List[EvmRpcClient] = []
        for uri, instances in archive_nodes:
            for _ in range(instances):
                rpc_clients.append(EvmRpcClient(uri, stats_service, rpc_requests_per_second))
        async with ConnectionPoolingEvmRpcClient(rpc_clients) as rpc_client:
            data_bus = ParallelDataBus(logger)
            block_time_service = BlockTimeService(block_time_cache, rpc_client)

            await data_bus.register(
                EvmBlockIdToEvmBlockAndEvmTransactionAndEvmTransactionHashTransformer(
                    data_bus=data_bus,
                    rpc_client=rpc_client,
                    block_time_service=block_time_service,
                ),
            )
            await data_bus.register(
                EvmTransactionToContractEvmTransactionReceiptTransformer(
                    data_bus=data_bus,
                    rpc_client=rpc_client,
                )
            )
            await data_bus.register(
                EvmTransactionReceiptToNftCollectionTransformer(
                    data_bus=data_bus,
                    blockchain=blockchain,
                    rpc_client=rpc_client,
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
                    rpc_client=rpc_client,
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
                    rpc_client=rpc_client,
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
