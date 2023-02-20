import abc
from typing import List

from blockcrawler.core.entities import BlockChain
from blockcrawler.core.types import Address, HexInt
from blockcrawler.nft.entities import Collection, Token, TokenTransfer, TokenOwner


class DataServiceException(Exception):
    pass


class DataVersionTooOldException(DataServiceException):
    pass


class DataService(abc.ABC):
    @abc.abstractmethod
    async def write_collection(self, collection: Collection) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def write_token(self, token: Token):
        raise NotImplementedError

    @abc.abstractmethod
    async def update_token_metadata_url(
        self,
        blockchain: BlockChain,
        collection_id: Address,
        token_id: HexInt,
        metadata_url: str,
        metadata_url_version: HexInt,
        data_version: int,
    ):
        raise NotImplementedError

    @abc.abstractmethod
    async def update_token_quantity(
        self,
        blockchain: BlockChain,
        collection_id: Address,
        token_id: HexInt,
        quantity: int,
        data_version: int,
    ):
        raise NotImplementedError

    @abc.abstractmethod
    async def update_token_current_owner(
        self,
        blockchain: BlockChain,
        collection_id: Address,
        token_id: HexInt,
        owner: Address,
        owner_version: HexInt,
        data_version: int,
    ):
        raise NotImplementedError

    @abc.abstractmethod
    async def write_token_batch(self, tokens: List[Token]):
        raise NotImplementedError

    @abc.abstractmethod
    async def write_token_transfer(self, token_transfer: TokenTransfer) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def write_token_transfer_batch(self, token_transfers: List[TokenTransfer]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def update_token_owner(self, token_owner: TokenOwner) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_token_owner_with_zero_tokens(
        self, blockchain: BlockChain, collection_id: Address, token_id: HexInt, account: Address
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def write_token_owner_batch(self, token_owners: List[TokenOwner]) -> None:
        raise NotImplementedError


STAT_WRITE_DELAYED = "data_write_delayed"
STAT_TOKEN_OWNER_WRITE_BATCH = "token_owner_write_batch"
STAT_TOKEN_OWNER_WRITE_BATCH_MS = "token_owner_write_batch_ms"
STAT_TOKEN_OWNER_UPDATE = "token_owner_update"
STAT_TOKEN_OWNER_UPDATE_MS = "token_owner_update_ms"
STAT_TOKEN_OWNER_DELETE_ZERO = "token_owner_delete_zero"
STAT_TOKEN_OWNER_DELETE_ZERO_MS = "token_owner_delete_zero_ms"
STAT_TOKEN_OWNER_UPDATE_DATA_TOO_OLD = "token_owner_update_data_too_old"
STAT_TOKEN_TRANSFER_WRITE_BATCH = "token_transfer_write_batch"
STAT_TOKEN_TRANSFER_WRITE_BATCH_MS = "token_transfer_write_batch_ms"
STAT_TOKEN_TRANSFER_WRITE_DATA_TOO_OLD = "token_transfer_write_data_too_old"
STAT_TOKEN_TRANSFER_WRITE = "token_transfer_write"
STAT_TOKEN_TRANSFER_WRITE_MS = "token_transfer_write_ms"
STAT_TOKEN_WRITE_BATCH = "token_write_batch"
STAT_TOKEN_WRITE_BATCH_MS = "token_write_batch_ms"
STAT_TOKEN_WRITE_DATA_TOO_OLD = "token_write_data_too_old"
STAT_TOKEN_WRITE = "token_write"
STAT_TOKEN_WRITE_MS = "token_write_ms"
STAT_TOKEN_UPDATE = "token_update"
STAT_TOKEN_UPDATE_MS = "token_update_ms"
STAT_TOKEN_URI_UPDATE = "token_uri_update"
STAT_TOKEN_URI_UPDATE_MS = "token_uri_update_ms"
STAT_TOKEN_URI_UPDATE_DATA_TOO_OLD = "token_uri_update_data_too_old"
STAT_TOKEN_QUANTITY_UPDATE = "token_quantity_update"
STAT_TOKEN_QUANTITY_UPDATE_MS = "token_quantity_update_ms"
STAT_TOKEN_QUANTITY_UPDATE_DATA_TOO_OLD = "token_quantity_update_data_too_old"
STAT_TOKEN_CURRENT_OWNER_UPDATE = "token_current_owner_update"
STAT_TOKEN_CURRENT_OWNER_UPDATE_MS = "token_current_owner_update_ms"
STAT_TOKEN_CURRENT_OWNER_UPDATE_DATA_TOO_OLD = "token_current_owner_update_data_too_old"
STAT_COLLECTION_WRITE = "collection_write"
STAT_COLLECTION_WRITE_MS = "collection_write_ms"
STAT_COLLECTION_WRITE_DATA_TOO_OLD = "collection_write_data_too_old"
