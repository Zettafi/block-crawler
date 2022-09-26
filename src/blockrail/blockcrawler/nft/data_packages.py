from dataclasses import dataclass

from blockrail.blockcrawler.core.bus import DataPackage
from blockrail.blockcrawler.core.entities import HexInt, BlockChain
from blockrail.blockcrawler.evm.types import Address
from blockrail.blockcrawler.nft.entities import Collection, TokenTransfer


@dataclass
class NftCollectionDataPackage(DataPackage):
    collection: Collection


@dataclass
class NftTokenTransferDataPackage(DataPackage):
    token_transfer: TokenTransfer


@dataclass
class NftTokenMetadataUriUpdatedDataPackage(DataPackage):
    blockchain: BlockChain
    collection_id: Address
    token_id: HexInt
    metadata_uri: str
    metadata_uri_version: HexInt
    data_version: int
