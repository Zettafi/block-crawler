import asyncio
import datetime
import time
from logging import Logger
from typing import Union, Dict, cast, Optional, List, Tuple

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .consumers import (
    NftCollectionPersistenceConsumer,
    NftTokenMintPersistenceConsumer,
    NftTokenTransferPersistenceConsumer,
    NftTokenQuantityUpdatingConsumer,
    NftMetadataUriUpdatingConsumer,
    NftTokenMetadataPersistingConsumer,
)
from .data_services.dynamodb import DynamoDbDataService
from .entities import BlockChain
from .evm.consumers import (
    CollectionToEverythingElseErc721CollectionBasedConsumer,
    CollectionToEverythingElseErc1155CollectionBasedConsumer,
)
from .evm.oracles import LogVersionOracle, TokenTransactionTypeOracle
from .evm.transformers import (
    EvmTransactionReceiptToNftCollectionTransformer,
    EvmLogErc721TransferToNftTokenTransferTransformer,
    EvmLogErc1155TransferSingleToNftTokenTransferTransformer,
    EvmLogErc1155TransferToNftTokenTransferTransformer,
    EvmLogErc1155UriEventToNftTokenMetadataUriUpdatedTransformer,
    Erc721TokenTransferToNftTokenMetadataUriUpdatedTransformer,
)
from ..core.bus import ParallelDataBus
from ..core.data_clients import HttpDataClient, IpfsDataClient, ArweaveDataClient, DataUriDataClient
from ..core.entities import HexInt
from ..core.services import BlockTimeService
from ..core.stats import StatsService
from ..core.storage_clients import S3StorageClient, StorageClientContext
from ..data.models import BlockCrawlerConfig, Tokens
from ..evm.producers import BlockIDProducer
from ..evm.rpc import EvmRpcClient, MultiProviderEvmRpcClient
from ..evm.transformers import (
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
    storage_client_context: StorageClientContext,
    logger: Logger,
    rpc_client: EvmRpcClient,
    blockchain: BlockChain,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
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
    http_data_client = HttpDataClient(http_metadata_timeout, stats_service)
    ipfs_data_client = IpfsDataClient(ipfs_node_uri, ipfs_metadata_timeout, stats_service)
    arweave_data_client = ArweaveDataClient(
        arweave_node_uri, arweave_metadata_timeout, stats_service
    )
    data_uri_data_client = DataUriDataClient(stats_service)
    await data_bus.register(
        NftTokenMetadataPersistingConsumer(
            http_client=http_data_client,
            ipfs_client=ipfs_data_client,
            arweave_client=arweave_data_client,
            data_uri_client=data_uri_data_client,
            storage_client_context=storage_client_context,
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
    s3_endpoint_url: str,
    s3_region: str,
    dynamodb_timeout: float,
    table_prefix: str,
    s3_metadata_bucket: str,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    starting_block: int,
    ending_block: int,
    increment_data_version: bool,
):
    session = aioboto3.Session()

    config = Config(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    base_resource_kwargs: Dict[str, Union[str, Config]] = dict(config=config)

    dynamodb_resource_kwargs = base_resource_kwargs.copy()
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        dynamodb_resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    if dynamodb_region is not None:
        dynamodb_resource_kwargs["region_name"] = dynamodb_region
    async with session.resource("dynamodb", **dynamodb_resource_kwargs) as dynamodb:  # type: ignore
        data_version = await get_data_version(  # noqa: F841
            dynamodb, blockchain, increment_data_version, table_prefix
        )

        async with S3StorageClient(
            bucket=s3_metadata_bucket,
            stats_service=stats_service,
            endpoint_url=s3_endpoint_url,
            region_name=s3_region,
        ) as storage_client_context:
            async with EvmRpcClient(
                archive_node_uri, stats_service, rpc_requests_per_second
            ) as rpc_client:
                # TODO: Process chunks and allow for graceful stop
                # TODO: Report on progress
                data_bus = await __evm_block_crawler_data_bus_factory(
                    stats_service=stats_service,
                    dynamodb=dynamodb,
                    table_prefix=table_prefix,
                    storage_client_context=storage_client_context,
                    logger=logger,
                    rpc_client=rpc_client,
                    blockchain=blockchain,
                    http_metadata_timeout=http_metadata_timeout,
                    ipfs_node_uri=ipfs_node_uri,
                    ipfs_metadata_timeout=ipfs_metadata_timeout,
                    arweave_node_uri=arweave_node_uri,
                    arweave_metadata_timeout=arweave_metadata_timeout,
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
    s3_endpoint_url: str,
    s3_region: str,
    s3_metadata_bucket: str,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    trail_blocks: int,
    process_interval: int,
):
    session = aioboto3.Session()

    config = Config(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    base_resource_kwargs: Dict[str, Union[str, Config]] = dict(config=config)

    dynamodb_resource_kwargs = base_resource_kwargs.copy()
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        dynamodb_resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    if dynamodb_region is not None:
        dynamodb_resource_kwargs["region_name"] = dynamodb_region
    async with session.resource("dynamodb", **dynamodb_resource_kwargs) as dynamodb:  # type: ignore
        data_version = await get_data_version(  # noqa: F841
            dynamodb, blockchain, False, table_prefix
        )
        async with S3StorageClient(
            bucket=s3_metadata_bucket,
            stats_service=stats_service,
            endpoint_url=s3_endpoint_url,
            region_name=s3_region,
        ) as storage_client_context:

            async with EvmRpcClient(
                provider_url=archive_node_uri,
                stats_service=stats_service,
                requests_per_second=rpc_requests_per_second,
            ) as rpc_client:
                data_bus = await __evm_block_crawler_data_bus_factory(
                    stats_service=stats_service,
                    dynamodb=dynamodb,
                    table_prefix=table_prefix,
                    storage_client_context=storage_client_context,
                    logger=logger,
                    rpc_client=rpc_client,
                    blockchain=blockchain,
                    http_metadata_timeout=http_metadata_timeout,
                    ipfs_node_uri=ipfs_node_uri,
                    ipfs_metadata_timeout=ipfs_metadata_timeout,
                    arweave_node_uri=arweave_node_uri,
                    arweave_metadata_timeout=arweave_metadata_timeout,
                    data_version=data_version,
                )

                last_block_table = await dynamodb.Table(BlockCrawlerConfig.table_name)
                last_block_result = await last_block_table.get_item(
                    Key={"blockchain": blockchain.value}
                )
                try:
                    last_block_processed = int(last_block_result.get("Item").get("last_block_id"))
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
                    current_block_number = block_number.int_value - trail_blocks
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
    blockchain: str, last_block_id: int, dynamodb_endpoint_url: str, table_prefix: str
):
    resource_kwargs = dict()
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    session = aioboto3.Session()
    async with session.resource("dynamodb", **resource_kwargs) as dynamodb:  # type: ignore
        block_crawler_config = await dynamodb.Table(table_prefix + BlockCrawlerConfig.table_name)
        await block_crawler_config.update_item(
            Key={"blockchain": blockchain},
            UpdateExpression="SET last_block_id = :block_id",
            ExpressionAttributeValues={":block_id": last_block_id},
        )


async def get_block(evm_node_uri, stats_service: StatsService):
    rpc = EvmRpcClient(evm_node_uri, stats_service)
    async with rpc:
        block_number = await rpc.get_block_number()
    return block_number.int_value


async def load_evm_contracts_by_block(
    starting_block: int,
    ending_block: int,
    block_height: int,
    increment_data_version: bool,
    block_chunk_size: int,
    logger: Logger,
    stats_service: StatsService,
    block_times: List[Tuple[int, int]],
    archive_nodes: List[str],
    rpc_requests_per_second: Optional[int],
    rpc_connection_pool_size: int,
    blockchain: BlockChain,
    dynamodb_endpoint_url: str,
    dynamodb_region: str,
    dynamodb_timeout: float,
    table_prefix: str,
    block_bound_tracker,
) -> BlockTimeService:
    session = aioboto3.Session()

    config = Config(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    base_resource_kwargs: Dict[str, Union[str, Config]] = dict(config=config)

    token_transaction_type_oracle = TokenTransactionTypeOracle()
    log_version_oracle = LogVersionOracle()

    dynamodb_resource_kwargs = base_resource_kwargs.copy()
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        dynamodb_resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    if dynamodb_region is not None:
        dynamodb_resource_kwargs["region_name"] = dynamodb_region
    async with session.resource("dynamodb", **dynamodb_resource_kwargs) as dynamodb:  # type: ignore
        data_service = DynamoDbDataService(dynamodb, stats_service, table_prefix)
        data_version = await get_data_version(  # noqa: F841
            dynamodb, blockchain, increment_data_version, table_prefix
        )

        async with MultiProviderEvmRpcClient(
            archive_nodes, stats_service, rpc_requests_per_second
        ) as rpc_client:
            data_bus = ParallelDataBus(logger)
            block_time_service = BlockTimeService(rpc_client)

            try:
                while True:
                    block_id, timestamp = block_times.pop()
                    await block_time_service.set_block_timestamp(block_id, timestamp)
            except IndexError:
                pass

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
            await data_bus.register(
                CollectionToEverythingElseErc721CollectionBasedConsumer(
                    data_bus=data_bus,
                    data_service=data_service,
                    rpc_client=rpc_client,
                    block_time_service=block_time_service,
                    log_version_oracle=log_version_oracle,
                    token_transaction_type_oracle=token_transaction_type_oracle,
                    max_block_height=HexInt(block_height),
                )
            )
            await data_bus.register(
                CollectionToEverythingElseErc1155CollectionBasedConsumer(
                    data_bus=data_bus,
                    data_service=data_service,
                    rpc_client=rpc_client,
                    block_time_service=block_time_service,
                    log_version_oracle=log_version_oracle,
                    token_transaction_type_oracle=token_transaction_type_oracle,
                    max_block_height=HexInt(block_height),
                )
            )

            for block_chunk_start in range(ending_block, starting_block, -1 * block_chunk_size):
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

                # TODO: Update block times cache

            return block_time_service
