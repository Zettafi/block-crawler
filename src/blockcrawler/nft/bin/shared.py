import csv
import dataclasses
import logging
from logging import Logger
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from blockcrawler.core.bus import ParallelDataBus
from blockcrawler.core.rpc import RpcClient
from blockcrawler.core.stats import StatsService, _safe_average
from blockcrawler.core.types import HexInt
from blockcrawler.evm.rpc import EvmRpcClient
from blockcrawler.evm.services import MemoryBlockTimeCache, BlockTimeCache
from blockcrawler.evm.transformers import (
    BlockIdToEvmBlockTransformer,
    EvmBlockToEvmTransactionHashTransformer,
    EvmTransactionHashToEvmTransactionReceiptTransformer,
    EvmTransactionReceiptToEvmLogTransformer,
)
from blockcrawler.nft import data_services
from blockcrawler.nft.consumers import (
    NftCollectionPersistenceConsumer,
    NftTokenTransferPersistenceConsumer,
    NftTokenMintPersistenceConsumer,
    NftTokenQuantityUpdatingConsumer,
    NftMetadataUriUpdatingConsumer,
    OwnerPersistingConsumer,
    NftCurrentOwnerUpdatingConsumer,
)
from blockcrawler.nft.data.models import BlockCrawlerConfig
from blockcrawler.nft.data_services.dynamodb import DynamoDbDataService
from blockcrawler.nft.entities import BlockChain
from blockcrawler.nft.evm.oracles import TokenTransactionTypeOracle, LogVersionOracle
from blockcrawler.nft.evm.transformers import (
    EvmTransactionReceiptToNftCollectionTransformer,
    EvmLogErc721TransferToNftTokenTransferTransformer,
    EvmLogErc1155TransferSingleToNftTokenTransferTransformer,
    EvmLogErc1155TransferBatchToNftTokenTransferTransformer,
    EvmLogErc1155UriEventToNftTokenMetadataUriUpdatedTransformer,
    Erc721TokenTransferToNftTokenMetadataUriUpdatedTransformer,
)


@dataclasses.dataclass
class BlockBoundTracker:
    low: Optional[HexInt] = HexInt(0)
    high: Optional[HexInt] = HexInt(0)


@dataclasses.dataclass
class Config:
    evm_rpc_client: EvmRpcClient
    stats_service: StatsService
    blockchain: BlockChain
    dynamodb_timeout: float
    dynamodb_endpoint_url: str
    table_prefix: str
    logger: logging.Logger


async def _evm_block_crawler_data_bus_factory(
    stats_service: StatsService,
    dynamodb,
    table_prefix: str,
    logger: Logger,
    rpc_client: EvmRpcClient,
    blockchain: BlockChain,
    data_version: int,
    raise_on_exception: bool,
):
    data_bus = ParallelDataBus(logger, raise_on_exception=raise_on_exception)
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
    await data_bus.register(EvmTransactionReceiptToEvmLogTransformer(data_bus))
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
        EvmLogErc1155TransferBatchToNftTokenTransferTransformer(
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
    await data_bus.register(NftTokenMintPersistenceConsumer(data_service))
    await data_bus.register(NftTokenQuantityUpdatingConsumer(data_service))
    await data_bus.register(
        Erc721TokenTransferToNftTokenMetadataUriUpdatedTransformer(
            data_bus=data_bus,
            rpc_client=rpc_client,
        )
    )
    await data_bus.register(
        EvmLogErc1155UriEventToNftTokenMetadataUriUpdatedTransformer(
            data_bus=data_bus,
            log_version_oracle=log_version_oracle,
            data_version=data_version,
        )
    )
    await data_bus.register(NftMetadataUriUpdatingConsumer(data_service))
    await data_bus.register(NftCurrentOwnerUpdatingConsumer(data_service))
    await data_bus.register(OwnerPersistingConsumer(data_service))
    return data_bus


async def _get_data_version(
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
            # noinspection PyUnresolvedReferences
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


def _get_crawl_stat_line(stats_service: StatsService) -> str:
    rpc_connect = stats_service.get_count(RpcClient.STAT_CONNECT)
    rpc_reconnect = stats_service.get_count(RpcClient.STAT_RECONNECT)
    rpc_connection_reset = stats_service.get_count(RpcClient.STAT_CONNECTION_RESET)
    rpc_throttled = stats_service.get_count(RpcClient.STAT_RESPONSE_TOO_MANY_REQUESTS)
    rpc_sent = stats_service.get_count(RpcClient.STAT_REQUEST_SENT)
    rpc_delayed = stats_service.get_count(RpcClient.STAT_REQUEST_DELAYED)
    rpc_received = stats_service.get_count(RpcClient.STAT_RESPONSE_RECEIVED)
    rpc_request_ms = stats_service.get_count(RpcClient.STAT_REQUEST_MS)
    rpc_request_ms_avg = _safe_average(rpc_received, rpc_request_ms)
    collection_count = stats_service.get_count(data_services.STAT_COLLECTION_WRITE)
    collection_ms = stats_service.get_count(data_services.STAT_COLLECTION_WRITE_MS)
    collection_ms_avg = _safe_average(collection_count, collection_ms)
    token_count = stats_service.get_count(data_services.STAT_TOKEN_WRITE)
    token_ms = stats_service.get_count(data_services.STAT_TOKEN_WRITE_MS)
    token_ms_avg = _safe_average(token_count, token_ms)
    token_update_count = stats_service.get_count(data_services.STAT_TOKEN_UPDATE)
    token_update_ms = stats_service.get_count(data_services.STAT_TOKEN_UPDATE_MS)
    token_update_avg = _safe_average(token_update_count, token_update_ms)
    transfer_count = stats_service.get_count(data_services.STAT_TOKEN_TRANSFER_WRITE)
    transfer_ms = stats_service.get_count(data_services.STAT_TOKEN_TRANSFER_WRITE_MS)
    transfer_ms_avg = _safe_average(transfer_count, transfer_ms)
    owner_update_count = stats_service.get_count(data_services.STAT_TOKEN_OWNER_UPDATE)
    owner_update_ms = stats_service.get_count(data_services.STAT_TOKEN_OWNER_UPDATE_MS)
    owner_update_ms_avg = _safe_average(owner_update_count, owner_update_ms)
    write_delayed = stats_service.get_count(data_services.STAT_WRITE_DELAYED)
    stat_line = (
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
        f"TU:{token_update_count :,}/{token_update_avg :,.0F} "
        f"X:{transfer_count:,}/{transfer_ms_avg :,.0F} "
        f"OU:{owner_update_count:,}/{owner_update_ms_avg :,.0F}"
        f"]"
    )
    return stat_line


def _get_block_time_cache(block_time_cache_filename: Path, config: Config):
    config.logger.info(f"Loading block time cache from file {block_time_cache_filename}")
    block_time_cache = MemoryBlockTimeCache()
    try:
        with open(block_time_cache_filename, "r") as file:
            for block_id, timestamp in csv.reader(file):
                block_time_cache.set_sync(int(block_id), int(timestamp))
        config.logger.info(f"Loaded block time cache from file {block_time_cache_filename}")
    except FileNotFoundError:
        config.logger.warning(f"File {block_time_cache_filename} does not exist.")
    return block_time_cache


def _persist_block_time_cache(
    config: Config, block_time_cache: BlockTimeCache, block_time_cache_filename: Path
):
    config.logger.info(f"Saving block time cache to file {block_time_cache_filename}")
    with open(block_time_cache_filename, "w+") as file:
        csv_writer = csv.writer(file)
        for row in block_time_cache:
            csv_writer.writerow(row)
    config.logger.info(f"Saved block time cache to file {block_time_cache_filename}")


def _get_load_stat_line(stats_service: StatsService) -> str:
    rpc_connect = stats_service.get_count(RpcClient.STAT_CONNECT)
    rpc_reconnect = stats_service.get_count(RpcClient.STAT_RECONNECT)
    rpc_connection_reset = stats_service.get_count(RpcClient.STAT_CONNECTION_RESET)
    rpc_throttled = stats_service.get_count(RpcClient.STAT_RESPONSE_TOO_MANY_REQUESTS)
    rpc_sent = stats_service.get_count(RpcClient.STAT_REQUEST_SENT)
    rpc_delayed = stats_service.get_count(RpcClient.STAT_REQUEST_DELAYED)
    rpc_received = stats_service.get_count(RpcClient.STAT_RESPONSE_RECEIVED)
    rpc_request_ms = stats_service.get_count(RpcClient.STAT_REQUEST_MS)
    rpc_request_ms_avg = _safe_average(rpc_received, rpc_request_ms)
    collection_count = stats_service.get_count(data_services.STAT_COLLECTION_WRITE)
    collection_ms = stats_service.get_count(data_services.STAT_COLLECTION_WRITE_MS)
    collection_ms_avg = _safe_average(collection_count, collection_ms)
    token_count = stats_service.get_count(data_services.STAT_TOKEN_WRITE_BATCH)
    token_ms = stats_service.get_count(data_services.STAT_TOKEN_WRITE_BATCH_MS)
    token_ms_avg = _safe_average(token_count, token_ms)
    transfer_count = stats_service.get_count(data_services.STAT_TOKEN_TRANSFER_WRITE_BATCH)
    transfer_ms = stats_service.get_count(data_services.STAT_TOKEN_TRANSFER_WRITE_BATCH_MS)
    transfer_ms_avg = _safe_average(transfer_count, transfer_ms)
    owner_count = stats_service.get_count(data_services.STAT_TOKEN_OWNER_WRITE_BATCH)
    owner_ms = stats_service.get_count(data_services.STAT_TOKEN_OWNER_WRITE_BATCH_MS)
    owner_ms_avg = _safe_average(owner_count, owner_ms)
    write_delayed = stats_service.get_count(data_services.STAT_WRITE_DELAYED)
    return (
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


async def _update_latest_block(
    boto3_session: boto3.Session,
    blockchain: str,
    last_block_id: HexInt,
    dynamodb_endpoint_url: str,
    table_prefix: str,
):
    resource_kwargs = {}
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    async with boto3_session.resource("dynamodb", **resource_kwargs) as dynamodb:  # type: ignore
        block_crawler_config = await dynamodb.Table(table_prefix + BlockCrawlerConfig.table_name)
        await block_crawler_config.update_item(
            Key={"blockchain": blockchain},
            UpdateExpression="SET last_block_id = :block_id",
            ExpressionAttributeValues={":block_id": last_block_id.int_value},
        )
