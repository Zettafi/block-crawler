from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from hexbytes import HexBytes

from blockcrawler.core.types import Address, HexInt


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
        return HexBytes(self.value)

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


@dataclass(frozen=True)
class Function:
    function_signature_hash: HexBytes
    description: str
    param_types: List[str]
    return_types: List[str]
    is_view: bool


@dataclass(frozen=True)
class Event:
    event_signature_hash: HexBytes
    description: str
    indexed_param_types: List[str]
    non_indexed_param_types: List[str]


class Erc165Functions:
    SUPPORTS_INTERFACE = Function(
        HexBytes("0x01ffc9a7"),
        "supportsInterface(bytes4)->(bool)",
        ["bytes4"],
        ["bool"],
        True,
    )


class Erc721TokenReceiverFunctions:
    ON_ERC721_RECEIVED = Function(
        HexBytes("0x01ffc9a7"),
        "onERC721Received(address,address,uint256,bytes)->(bytes4)",
        ["address", "address", "uint256", "bytes"],
        ["bytes4"],
        True,
    )


class Erc721Functions:
    BALANCE_OF_ADDRESS = Function(
        HexBytes("0x70a08231"),
        "balanceOf(address)->(uint256)",
        ["address"],
        ["bool"],
        True,
    )
    OWNER_OF_TOKEN = Function(
        HexBytes("0x6352211e"),
        "ownerOf(uint256)->(address)",
        ["uint256"],
        ["address"],
        True,
    )
    GET_APPROVED_TOKEN = Function(
        HexBytes("0x081812fc"),
        "getApproved(uint256)->(address)",
        ["uint256"],
        ["address"],
        True,
    )
    IS_APPROVED_FOR_ALL = Function(
        HexBytes("0xe985e9c5"),
        "isApprovedForAll(address,address)",
        ["address", "address"],
        ["bool"],
        True,
    )
    SAFE_TRANSFER_FROM_WITH_DATA = Function(
        HexBytes("0xb88d4fde"),
        "safeTransferFrom(address,address,uint256,bytes)",
        ["address", "address", "uint256", "bytes"],
        [],
        False,
    )
    SAFE_TRANSFER_FROM_WITHOUT_DATA = Function(
        HexBytes("0x42842e0e"),
        "safeTransferFrom(address,address,uint256)",
        ["address", "address", "uint256"],
        [],
        False,
    )
    TRANSFER_FROM = Function(
        HexBytes("0x42842e0e"),
        "safeTransferFrom(address,address,uint256)",
        ["address", "address", "uint256"],
        [],
        False,
    )


class Erc721MetadataFunctions:
    NAME = Function(
        HexBytes("0x06fdde03"),
        "name()->(string)",
        [],
        ["string"],
        True,
    )
    SYMBOL = Function(
        HexBytes("0x95d89b41"),
        "symbol()->(string)",
        [],
        ["string"],
        True,
    )
    TOKEN_URI = Function(
        HexBytes("0xc87b56dd"),
        "tokenURI(uint256)->(string)",
        ["uint256"],
        ["string"],
        True,
    )


class Erc721EnumerableFunctions:
    TOTAL_SUPPLY = Function(
        HexBytes("0x18160ddd"),
        "totalSupply()->(uint256)",
        [],
        ["uint256"],
        True,
    )
    # noinspection SpellCheckingInspection
    TOKEN_BY_INDEX = Function(
        HexBytes("0x4f6ccce7"),
        "tokenByIndex(uint256)->uint(256)",
        ["uint256"],
        ["uint256"],
        True,
    )
    TOKEN_OF_OWNER_BY_INDEX = Function(
        HexBytes("0x2f745c59"),
        "tokenOfOwnerByIndex(address,uint256)->(uint256)",
        ["address", "uint256"],
        ["uint256"],
        True,
    )


class Erc1155MetadataUriFunctions:  # pragma: no cover
    URI = Function(
        HexBytes("0x0e89341c"),
        "uri(uint256)->(string)",
        ["uint256"],
        ["string"],
        True,
    )


class AdditionalFunctions:  # pragma: no cover
    OWNER = Function(
        HexBytes("0x8da5cb5b"),
        "owner()->(address)",
        [],
        ["address"],
        True,
    )


class Erc721Events:  # pragma: no cover
    TRANSFER = Event(
        HexBytes("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
        "Transfer(address, address indexed, uint256 indexed)",
        ["address", "address", "uint256"],
        [],
    )


class Erc1155Events:  # pragma: no cover
    # noinspection SpellCheckingInspection
    TRANSFER_SINGLE = Event(
        HexBytes("0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"),
        "TransferSingle(address indexed, address indexed, address indexed, uint256, uint256)",
        ["address", "address", "address"],
        ["uint256", "uint256"],
    )
    TRANSFER_BATCH = Event(
        HexBytes("0xf5f16c58bf69e14e9fa06e742215b42aa896de1c15af339f09e3360557089f43"),
        "TransferMultiple(address indexed, address indexed, address indexed, uint256[], uint256[])",
        ["address", "address", "address"],
        ["uint256[]", "uint256[]"],
    )
    URI = Event(
        HexBytes("0x4a4d592fe8a6f7daf17876002d0f8dfb179fa30f514ee60bf4aa407ebb0fb326"),
        "URI(string, unit256 indexed)",
        ["uint256"],
        ["string"],
    )
