from binascii import unhexlify
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Optional, NewType

from hexbytes import HexBytes

from blockrail.blockcrawler.core.entities import HexInt

Address = NewType("Address", str)


class ERC165InterfaceID(Enum):
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


@dataclass(unsafe_hash=True, frozen=True)
class EVMBlock:
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
    transactions: List[HexBytes]
    uncles: List[HexBytes]


@dataclass(unsafe_hash=True, frozen=True)
class EVMLog:
    removed: bool
    log_index: HexInt
    transaction_index: HexInt
    transaction_hash: HexBytes
    block_hash: HexBytes
    block_number: HexInt
    data: HexBytes
    topics: List[HexBytes]
    address: Optional[Address] = None


@dataclass(unsafe_hash=True, frozen=True)
class EVMTransactionReceipt:
    transaction_hash: HexBytes
    transaction_index: HexInt
    block_hash: HexBytes
    block_number: HexInt
    from_: Address
    cumulative_gas_used: HexInt
    gas_used: HexInt
    logs: List[EVMLog]
    logs_bloom: HexInt
    status: Optional[HexInt] = None
    to_: Optional[Address] = None
    contract_address: Optional[Address] = None
    root: Optional[HexBytes] = None


class Metadata:  # pragma: no cover
    pass


class ERC1155Metadata(Metadata):  # pragma: no cover
    def __init__(
        self, name: str, description: str, image: str, properties: Dict[str, object]
    ) -> None:
        pass
