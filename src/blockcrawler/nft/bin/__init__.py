import dataclasses
import logging
from typing import Optional

from blockcrawler.core.entities import BlockChain
from blockcrawler.core.rpc import RpcClient
from blockcrawler.core.stats import StatsService, safe_average

from blockcrawler.core.types import HexInt
from blockcrawler.evm.rpc import EvmRpcClient
from blockcrawler.nft import data_services


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


def get_stat_line(stats_service: StatsService) -> str:
    rpc_connect = stats_service.get_count(RpcClient.STAT_CONNECT)
    rpc_reconnect = stats_service.get_count(RpcClient.STAT_RECONNECT)
    rpc_connection_reset = stats_service.get_count(RpcClient.STAT_CONNECTION_RESET)
    rpc_throttled = stats_service.get_count(RpcClient.STAT_RESPONSE_TOO_MANY_REQUESTS)
    rpc_sent = stats_service.get_count(RpcClient.STAT_REQUEST_SENT)
    rpc_delayed = stats_service.get_count(RpcClient.STAT_REQUEST_DELAYED)
    rpc_received = stats_service.get_count(RpcClient.STAT_RESPONSE_RECEIVED)
    rpc_request_ms = stats_service.get_count(RpcClient.STAT_REQUEST_MS)
    rpc_request_ms_avg = safe_average(rpc_received, rpc_request_ms)
    collection_count = stats_service.get_count(data_services.STAT_COLLECTION_WRITE)
    collection_ms = stats_service.get_count(data_services.STAT_COLLECTION_WRITE_MS)
    collection_ms_avg = safe_average(collection_count, collection_ms)
    token_count = stats_service.get_count(data_services.STAT_TOKEN_WRITE)
    token_ms = stats_service.get_count(data_services.STAT_TOKEN_WRITE_MS)
    token_ms_avg = safe_average(token_count, token_ms)
    token_update_count = stats_service.get_count(data_services.STAT_TOKEN_UPDATE)
    token_update_ms = stats_service.get_count(data_services.STAT_TOKEN_UPDATE_MS)
    token_update_avg = safe_average(token_update_count, token_update_ms)
    transfer_count = stats_service.get_count(data_services.STAT_TOKEN_TRANSFER_WRITE)
    transfer_ms = stats_service.get_count(data_services.STAT_TOKEN_TRANSFER_WRITE_MS)
    transfer_ms_avg = safe_average(transfer_count, transfer_ms)
    owner_count = stats_service.get_count(data_services.STAT_TOKEN_OWNER_WRITE)
    owner_ms = stats_service.get_count(data_services.STAT_TOKEN_OWNER_WRITE_MS)
    owner_ms_avg = safe_average(owner_count, owner_ms)
    owner_update_count = stats_service.get_count(data_services.STAT_TOKEN_OWNER_UPDATE)
    owner_update_ms = stats_service.get_count(data_services.STAT_TOKEN_OWNER_UPDATE_MS)
    owner_update_ms_avg = safe_average(owner_update_count, owner_update_ms)
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
        f"O:{owner_count:,}/{owner_ms_avg :,.0F} "
        f"OU:{owner_update_count:,}/{owner_update_ms_avg :,.0F}"
        f"]"
    )
    return stat_line
