import dataclasses
import logging
from typing import Optional

from blockcrawler.core.entities import BlockChain
from blockcrawler.core.stats import StatsService

from blockcrawler.core.types import HexInt
from blockcrawler.evm.rpc import EvmRpcClient


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
