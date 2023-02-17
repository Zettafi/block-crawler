"""Data packages for sending data to the data bus"""

from dataclasses import dataclass

from hexbytes import HexBytes

from blockcrawler.core.bus import DataPackage
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.types import HexInt
from blockcrawler.evm.types import EvmBlock, EvmTransactionReceipt, EvmLog, EvmTransaction


@dataclass
class EvmBlockDataPackage(DataPackage):
    """Data package for EVM blocks"""

    blockchain: BlockChain
    """Blockchain to which the block belongs"""

    block: EvmBlock
    """Block"""


@dataclass
class EvmTransactionHashDataPackage(DataPackage):
    """Data package for EVM transaction hashes"""

    blockchain: BlockChain
    """Blockchain to which the block belongs"""

    hash: HexBytes
    """Transaction hash"""

    block: EvmBlock
    """Block from which the transaction hash originated"""


@dataclass
class EvmTransactionReceiptDataPackage(DataPackage):
    """Data package for EVM transaction receipts"""

    blockchain: BlockChain
    """Blockchain to which the block belongs"""

    transaction_receipt: EvmTransactionReceipt
    """Transaction receipt"""

    block: EvmBlock
    """Block from which the transaction receipt originated"""


@dataclass
class EvmLogDataPackage(DataPackage):
    """Data package for EVM logs"""

    blockchain: BlockChain
    """Blockchain to which the block belongs"""

    log: EvmLog
    transaction_receipt: EvmTransactionReceipt
    """Transaction receipt from which the log originated"""

    block: EvmBlock
    """Block from which the transaction containing the log originated"""


@dataclass
class EvmBlockIDDataPackage(DataPackage):
    """
    Data package for placing Block IDs on the Data Bus
    """

    blockchain: BlockChain
    """Blockchain to which the block ID belongs"""

    block_id: HexInt
    """Block ID"""


@dataclass
class EvmTransactionDataPackage(DataPackage):
    """Data package for EVM transactions"""

    blockchain: BlockChain
    """Blockchain to which the block belongs"""

    transaction: EvmTransaction
    """Transaction"""

    block: EvmBlock
    """Block from which the transaction originated"""
