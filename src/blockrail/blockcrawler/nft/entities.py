from dataclasses import dataclass
from enum import Enum
from typing import Optional, NewType

from hexbytes import HexBytes

from ..core.entities import HexInt, BlockChain, Entity
from ..evm.types import Address

CollectionType = NewType("CollectionType", str)


class EthereumCollectionType:
    ERC721 = CollectionType("ERC721")
    ERC1155 = CollectionType("ERC1155")


@dataclass(unsafe_hash=True, frozen=True)
class Collection(Entity):
    blockchain: BlockChain
    collection_id: Address
    creator: Address
    block_created: HexInt
    owner: Optional[Address]
    name: Optional[str]
    symbol: Optional[str]
    specification: CollectionType
    date_created: HexInt
    total_supply: Optional[HexInt]
    data_version: int


class TokenTransactionType(Enum):
    MINT = "mint"
    BURN = "burn"
    TRANSFER = "transfer"


@dataclass(unsafe_hash=True, frozen=True)
class Token(Entity):
    blockchain: BlockChain
    collection_id: Address
    token_id: HexInt
    quantity: HexInt
    data_version: int
    original_owner: Optional[Address] = None
    current_owner: Optional[Address] = None
    current_owner_version: Optional[str] = None
    mint_block: Optional[HexInt] = None
    mint_date: Optional[HexInt] = None
    metadata_url: Optional[str] = None
    metadata_url_version: Optional[str] = None


@dataclass(unsafe_hash=True, frozen=True)
class TokenMetadata:
    blockchain: BlockChain
    collection_id: Address
    token_id: HexInt
    content: str
    content_type: str


@dataclass(unsafe_hash=True, frozen=True)
class TokenTransfer(Entity):
    blockchain: BlockChain
    data_version: int
    collection_id: Address
    token_id: HexInt
    timestamp: HexInt
    transaction_type: TokenTransactionType
    from_: Address
    to_: Address
    quantity: HexInt
    block_id: HexInt
    transaction_hash: HexBytes
    transaction_index: HexInt
    log_index: HexInt
    attribute_version: HexInt


@dataclass(unsafe_hash=True, frozen=True)
class TokenOwner(Entity):
    blockchain: BlockChain
    collection_id: Address
    token_id = HexInt
    account = Address
    quantity = HexInt
    data_version = int
