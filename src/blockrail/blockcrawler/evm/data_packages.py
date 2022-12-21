from dataclasses import dataclass

from hexbytes import HexBytes

from blockrail.blockcrawler.core.bus import DataPackage
from blockrail.blockcrawler.core.entities import BlockChain, HexInt
from blockrail.blockcrawler.evm.types import EvmBlock, EvmTransactionReceipt, EvmLog, EvmTransaction


@dataclass
class EvmBlockDataPackage(DataPackage):
    blockchain: BlockChain
    block: EvmBlock


@dataclass
class EvmTransactionHashDataPackage(DataPackage):
    blockchain: BlockChain
    hash: HexBytes
    block: EvmBlock


@dataclass
class EvmTransactionReceiptDataPackage(DataPackage):
    blockchain: BlockChain
    transaction_receipt: EvmTransactionReceipt
    block: EvmBlock


@dataclass
class EvmLogDataPackage(DataPackage):
    blockchain: BlockChain
    log: EvmLog
    transaction_receipt: EvmTransactionReceipt
    block: EvmBlock


@dataclass
class EvmBlockIDDataPackage(DataPackage):
    """
    Data package for placing Block IDs on the Data Bus
    """

    blockchain: BlockChain
    block_id: HexInt


@dataclass
class EvmTransactionDataPackage(DataPackage):
    blockchain: BlockChain
    transaction: EvmTransaction
    block: EvmBlock
