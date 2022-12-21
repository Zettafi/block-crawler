import abc
from typing import List

from blockrail.blockcrawler.nft.entities import Collection, Token, TokenTransfer, TokenOwner


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
    async def write_token_batch(self, tokens: List[Token]):
        raise NotImplementedError

    @abc.abstractmethod
    async def write_token_transfer(self, token_transfer: TokenTransfer) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def write_token_transfer_batch(self, token_transfers: List[TokenTransfer]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def write_token_owner(self, token_owner: TokenOwner) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def write_token_owner_batch(self, token_owners: List[TokenOwner]) -> None:
        raise NotImplementedError


STAT_TOKEN_OWNER_WRITE_BATCH = "token_owner_write_batch"
STAT_TOKEN_OWNER_WRITE_BATCH_MS = "token_owner_write_batch_ms"
STAT_TOKEN_OWNER_WRITE_DATA_TOO_OLD = "token_owner_write_data_too_old"
STAT_TOKEN_OWNER_WRITE = "token_owner_write"
STAT_TOKEN_OWNER_WRITE_MS = "token_owner_write_ms"
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
STAT_COLLECTION_WRITE = "collection_write"
STAT_COLLECTION_WRITE_MS = "collection_write_ms"
STAT_COLLECTION_WRITE_DATA_TOO_OLD = "collection_write_data_too_old"
