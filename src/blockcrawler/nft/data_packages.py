from dataclasses import dataclass

from blockcrawler.core.bus import DataPackage
from blockcrawler.core.entities import HexInt, BlockChain
from blockcrawler.evm.types import Address
from blockcrawler.nft.entities import Collection, TokenTransfer, Token


@dataclass
class CollectionDataPackage(DataPackage):
    collection: Collection


@dataclass
class TokenDataPackage(DataPackage):
    token: Token


@dataclass
class TokenTransferDataPackage(DataPackage):
    token_transfer: TokenTransfer


@dataclass
class TokenMetadataUriUpdatedDataPackage(DataPackage):
    blockchain: BlockChain
    collection_id: Address
    token_id: HexInt
    metadata_uri: str
    metadata_uri_version: HexInt
    data_version: int
