from dataclasses import dataclass

from hexbytes import HexBytes

from blockrail.blockcrawler.core.bus import DataPackage
from blockrail.blockcrawler.core.entities import BlockChain
from blockrail.blockcrawler.evm.types import EVMBlock, EVMTransactionReceipt, EVMLog


@dataclass
class EVMBlockDataPackage(DataPackage):
    blockchain: BlockChain
    block: EVMBlock


@dataclass
class EVMTransactionHashDataPackage(DataPackage):
    blockchain: BlockChain
    hash: HexBytes
    block: EVMBlock


@dataclass
class EvmTransactionReceiptDataPackage(DataPackage):
    blockchain: BlockChain
    transaction_receipt: EVMTransactionReceipt
    block: EVMBlock


@dataclass
class EvmLogDataPackage(DataPackage):
    blockchain: BlockChain
    log: EVMLog
    transaction_receipt: EVMTransactionReceipt
    block: EVMBlock
