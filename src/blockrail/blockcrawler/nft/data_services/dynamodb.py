import asyncio
from typing import List

from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.table import TableResource

from blockrail.blockcrawler.nft.entities import TokenOwner, TokenTransfer, Token, Collection
from . import (
    DataVersionTooOldException,
    DataService,
    STAT_TOKEN_OWNER_WRITE_BATCH,
    STAT_TOKEN_OWNER_WRITE_DATA_TOO_OLD,
    STAT_TOKEN_OWNER_WRITE,
    STAT_TOKEN_TRANSFER_WRITE_BATCH,
    STAT_TOKEN_TRANSFER_WRITE_DATA_TOO_OLD,
    STAT_TOKEN_TRANSFER_WRITE,
    STAT_TOKEN_WRITE_BATCH,
    STAT_TOKEN_WRITE_DATA_TOO_OLD,
    STAT_TOKEN_WRITE,
    STAT_COLLECTION_WRITE_DATA_TOO_OLD,
    STAT_COLLECTION_WRITE,
    STAT_TOKEN_OWNER_WRITE_MS,
    STAT_COLLECTION_WRITE_MS,
    STAT_TOKEN_WRITE_MS,
    STAT_TOKEN_WRITE_BATCH_MS,
    STAT_TOKEN_TRANSFER_WRITE_MS,
    STAT_TOKEN_TRANSFER_WRITE_BATCH_MS,
    STAT_TOKEN_OWNER_WRITE_BATCH_MS,
)
from ...core.stats import StatsService


class DynamoDbDataService(DataService):
    def __init__(
        self, dynamodb, stats_service: StatsService, table_prefix: str = "", parallel_batches=1
    ) -> None:
        self.__dynamodb = dynamodb
        self.__stats_service = stats_service
        self.__table_prefix = table_prefix
        self.__parallel_batches = parallel_batches

    async def write_collection(self, collection: Collection) -> None:
        with self.__stats_service.ms_counter(STAT_COLLECTION_WRITE_MS):
            item = {
                "blockchain": collection.blockchain.value,
                "collection_id": collection.collection_id,
                "block_created": collection.block_created.hex_value,
                "creator": collection.creator,
                "date_created": collection.date_created.hex_value,
                "specification": collection.specification,
                "data_version": collection.data_version,
            }
            if collection.total_supply is not None:
                item["total_supply"] = collection.total_supply.hex_value
            if collection.owner is not None:
                item["owner"] = collection.owner
            if collection.name is not None and len(collection.name) > 0:
                item["name"] = collection.name
                item["name_lower"] = collection.name.lower()
            if collection.symbol is not None:
                item["symbol"] = collection.symbol

            table = await self.__get_table("collection")
            try:
                await table.put_item(
                    Item=item,
                    ConditionExpression=Attr("data_version").not_exists()
                    | Attr("data_version").lte(collection.data_version),
                )
                self.__stats_service.increment(STAT_COLLECTION_WRITE, 1)
            except self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                self.__stats_service.increment(STAT_COLLECTION_WRITE_DATA_TOO_OLD, 1)
                raise DataVersionTooOldException()

    async def write_token(self, token: Token):
        with self.__stats_service.ms_counter(STAT_TOKEN_WRITE_MS):
            token_table = await self.__get_table("token")
            try:
                await token_table.put_item(
                    Item=self.__get_token_item(token),
                    ConditionExpression=Attr("data_version").not_exists()
                    | Attr("data_version").lte(token.data_version),
                )
                self.__stats_service.increment(STAT_TOKEN_WRITE, 1)
            except self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                self.__stats_service.increment(STAT_TOKEN_WRITE_DATA_TOO_OLD, 1)
                raise DataVersionTooOldException()

    async def write_token_batch(self, tokens: List[Token]):
        with self.__stats_service.ms_counter(STAT_TOKEN_WRITE_BATCH_MS):
            token_table = await self.__get_table("token")
            token_items = [self.__get_token_item(token) for token in tokens]
            await self.__write_batch(token_table, token_items)
            self.__stats_service.increment(STAT_TOKEN_WRITE_BATCH, len(token_items))

    async def write_token_transfer(self, token_transfer: TokenTransfer):
        with self.__stats_service.ms_counter(STAT_TOKEN_TRANSFER_WRITE_MS):
            token_transfer_table = await self.__get_table("tokentransfers")
            try:
                await token_transfer_table.put_item(
                    Item=self.__get_token_transfer_item(token_transfer),
                    ConditionExpression=Attr("data_version").not_exists()
                    | Attr("data_version").lte(token_transfer.data_version),
                )
                self.__stats_service.increment(STAT_TOKEN_TRANSFER_WRITE, 1)
            except self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                self.__stats_service.increment(STAT_TOKEN_TRANSFER_WRITE_DATA_TOO_OLD, 1)
                raise DataVersionTooOldException()

    async def write_token_transfer_batch(self, token_transfers: List[TokenTransfer]) -> None:
        with self.__stats_service.ms_counter(STAT_TOKEN_TRANSFER_WRITE_BATCH_MS):
            token_transfer_table = await self.__get_table("tokentransfers")
            token_transfer_items = [
                self.__get_token_transfer_item(token_transfer) for token_transfer in token_transfers
            ]
            await self.__write_batch(token_transfer_table, token_transfer_items)
            self.__stats_service.increment(
                STAT_TOKEN_TRANSFER_WRITE_BATCH, len(token_transfer_items)
            )

    async def write_token_owner(self, token_owner: TokenOwner):
        with self.__stats_service.ms_counter(STAT_TOKEN_OWNER_WRITE_MS):
            token_owner_table = await self.__get_table("owner")
            try:
                await token_owner_table.put_item(
                    Item=self.__get_token_owner(token_owner),
                    ConditionExpression=Attr("data_version").not_exists()
                    | Attr("data_version").lte(token_owner.data_version),
                )
                self.__stats_service.increment(STAT_TOKEN_OWNER_WRITE, 1)
            except self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                self.__stats_service.increment(STAT_TOKEN_OWNER_WRITE_DATA_TOO_OLD, 1)
                raise DataVersionTooOldException()

    async def write_token_owner_batch(self, token_owners: List[TokenOwner]) -> None:
        with self.__stats_service.ms_counter(STAT_TOKEN_OWNER_WRITE_BATCH_MS):
            token_owner_table = await self.__get_table("owner")
            token_owner_items = [
                self.__get_token_owner(token_owner) for token_owner in token_owners
            ]
            await self.__write_batch(token_owner_table, token_owner_items)
            self.__stats_service.increment(STAT_TOKEN_OWNER_WRITE_BATCH, len(token_owner_items))

    async def __get_table(self, table_name: str) -> TableResource:
        return await self.__dynamodb.Table(f"{self.__table_prefix}{table_name}")

    async def __write_batch(self, table_resource, items):
        async def __write_batch_to_dynamo(batch_items):
            async with table_resource.batch_writer() as batch_writer:
                for batch_item in batch_items:
                    await batch_writer.put_item(
                        Item=batch_item,
                    )

        items_ = items[:]
        batches = [[] for _ in range(self.__parallel_batches)]
        try:
            while True:
                for batch in batches:
                    batch.append(items_.pop(0))
        except IndexError:
            pass  # No more items_

        batch_writes = [__write_batch_to_dynamo(batch) for batch in batches if batch]
        await asyncio.gather(*batch_writes)

    @staticmethod
    def __get_token_item(token: Token) -> dict:
        return dict(
            blockchain_collection_id=f"{token.blockchain.value}" f"::{token.collection_id}",
            token_id=token.token_id.hex_value,
            mint_date=token.mint_date.int_value,
            mint_block=token.mint_block.hex_value,
            original_owner=token.original_owner,
            current_owner=token.current_owner,
            current_owner_version=token.attribute_version.hex_value,
            quantity=token.quantity.int_value,
            data_version=token.data_version,
            metadata_url=token.metadata_url
            if token.metadata_url and len(token.metadata_url) < 2049
            else None,
        )

    @staticmethod
    def __get_token_transfer_item(token_transfer: TokenTransfer) -> dict:
        return dict(
            blockchain_collection_id=f"{token_transfer.blockchain.value}"
            f"::{token_transfer.collection_id}",
            transaction_log_index_hash=token_transfer.attribute_version.hex_value,
            collection_id=token_transfer.collection_id,
            token_id=token_transfer.token_id.hex_value,
            timestamp=token_transfer.timestamp.int_value,
            block_id=token_transfer.block_id.padded_hex(8),
            transaction_type=token_transfer.transaction_type.value,
            from_account=token_transfer.from_,
            to_account=token_transfer.to_,
            quantity=token_transfer.quantity.hex_value,
            transaction_hash=token_transfer.transaction_hash.hex(),
            transaction_index=token_transfer.transaction_index.hex_value,
            log_index=token_transfer.log_index.hex_value,
            data_version=token_transfer.data_version,
        )

    @staticmethod
    def __get_token_owner(token_owner: TokenOwner) -> dict:
        return dict(
            blockchain_account=f"{token_owner.blockchain.value}::{token_owner.account}",
            collection_id_token_id=f"{token_owner.collection_id}::{token_owner.token_id}",
            collection_id=token_owner.collection_id,
            token_id=token_owner.token_id.hex_value,
            account=token_owner.account,
            quantity=token_owner.quantity.int_value,
            data_version=token_owner.data_version,
        )
