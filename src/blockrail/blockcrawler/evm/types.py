from binascii import unhexlify
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, NewType

from hexbytes import HexBytes

from blockrail.blockcrawler.core.entities import HexInt

Address = NewType("Address", str)


class Erc165InterfaceID(Enum):
    ERC721 = "0x80ac58cd"
    ERC721_TOKEN_RECEIVER = "0x150b7a02"
    ERC721_METADATA = "0x5b5e139f"
    ERC721_ENUMERABLE = "0x780e9d63"
    ERC998_ERC721_TOP_DOWN = "0xcde244d9"
    ERC998_ERC721_TOP_DOWN_ENUMERABLE = "0xa344afe4"
    ERC998_ERC721_BOTTOM_UP = "0xa1b23002"
    ERC998_ERC721_BOTTOM_UP_ENUMERABLE = "0x8318b539"
    ERC998_ERC20_TOP_DOWN = "0x7294ffed"
    ERC998_ERC20_TOP_DOWN_ENUMERABLE = "0xc5fd96cd"
    ERC998_ERC20_BOTTOM_UP = "0xffafa991"
    ERC1155 = "0xd9b67a26"
    ERC1155_TOKEN_RECEIVER = "0x4e2312e0"
    ERC1155_METADATA_URI = "0x0e89341c"

    @property
    def bytes(self) -> bytes:
        return unhexlify(self.value[2:])

    @classmethod
    def from_value(cls, value: str):
        for item in cls:
            if item.value == value:
                return item


@dataclass(frozen=True)
class EvmTransaction:
    block_hash: HexBytes
    block_number: HexInt
    from_: Address
    gas: HexInt
    gas_price: HexInt
    hash: HexBytes
    input: HexBytes
    nonce: HexInt
    transaction_index: HexInt
    v: HexInt
    r: HexBytes
    s: HexBytes
    to_: Optional[Address] = None
    value: Optional[HexInt] = None

    def __hash__(self) -> int:
        return (self.__class__.__name__ + self.hash.hex()).__hash__()


@dataclass(frozen=True)
class EvmBlock:
    number: HexInt
    hash: HexBytes
    parent_hash: HexBytes
    nonce: HexBytes
    sha3_uncles: HexBytes
    logs_bloom: HexInt
    transactions_root: HexBytes
    state_root: HexBytes
    receipts_root: HexBytes
    miner: Address
    mix_hash: HexBytes
    difficulty: HexInt
    total_difficulty: HexInt
    extra_data: HexBytes
    size: HexInt
    gas_limit: HexInt
    gas_used: HexInt
    timestamp: HexInt
    transaction_hashes: List[HexBytes]
    uncles: List[HexBytes]
    transactions: Optional[List[EvmTransaction]]

    def __hash__(self) -> int:
        return (self.__class__.__name__ + self.hash.hex()).__hash__()


@dataclass(frozen=True)
class EvmLog:
    removed: bool
    log_index: HexInt
    transaction_index: HexInt
    transaction_hash: HexBytes
    block_hash: HexBytes
    block_number: HexInt
    data: HexBytes
    topics: List[HexBytes]
    address: Optional[Address] = None

    def __hash__(self) -> int:
        return (
            self.__class__.__name__
            + self.block_number.hex_value
            + self.transaction_index.hex_value
            + self.log_index.hex_value
        ).__hash__()


@dataclass(frozen=True)
class EvmTransactionReceipt:
    transaction_hash: HexBytes
    transaction_index: HexInt
    block_hash: HexBytes
    block_number: HexInt
    from_: Address
    cumulative_gas_used: HexInt
    gas_used: HexInt
    logs: List[EvmLog]
    logs_bloom: HexInt
    status: Optional[HexInt] = None
    to_: Optional[Address] = None
    contract_address: Optional[Address] = None
    root: Optional[HexBytes] = None

    def __hash__(self) -> int:
        return (self.__class__.__name__ + self.transaction_hash.hex()).__hash__()
