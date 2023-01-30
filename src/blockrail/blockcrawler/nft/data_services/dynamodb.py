import asyncio
from typing import List, Dict, cast

from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.table import TableResource
from math import floor

from blockrail.blockcrawler.nft.entities import TokenOwner, TokenTransfer, Token, Collection
from . import (
    DataVersionTooOldException,
    DataService,
    STAT_WRITE_DELAYED,
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
        self,
        dynamodb,
        stats_service: StatsService,
        table_prefix: str = "",
        parallel_batches=1,
        maximum_account_items_per_second=None,
        maximum_table_items_per_second=40_000,
        maximum_partition_items_per_second=1_000,
    ) -> None:
        self.__dynamodb = dynamodb
        self.__stats_service = stats_service
        self.__table_prefix = table_prefix
        self.__parallel_batches = parallel_batches
        self.__this_second: int = 0
        self.__maximum_account_items_per_second = maximum_account_items_per_second
        self.__account_items_this_second: int = 0
        self.__maximum_table_items_per_second: int = maximum_table_items_per_second
        self.__table_items_this_second: Dict[str, int] = {}
        self.__maximum_partition_items_per_second: int = maximum_partition_items_per_second
        self.__partition_items_this_second: Dict[str, int] = {}

    async def write_collection(self, collection: Collection) -> None:
        item = {
            "blockchain": collection.blockchain.value,
            "collection_id": collection.collection_id,
            "block_created": collection.block_created.hex_value,
            "creator_account": collection.creator,
            "date_created": collection.date_created.int_value,
            "specification": collection.specification,
            "data_version": collection.data_version,
        }
        if collection.total_supply is not None:
            item["total_supply"] = collection.total_supply.hex_value
        if collection.owner is not None:
            item["owner_account"] = collection.owner
        if collection.name is not None and len(collection.name) > 0:
            item["collection_name"] = collection.name
            item["name_lower"] = collection.name.lower()[:1024]
        if collection.symbol is not None:
            item["symbol"] = collection.symbol

        await self.__write_item(
            "collection",
            cast(str, item["blockchain"]),
            item,
            collection.data_version,
            STAT_COLLECTION_WRITE,
            STAT_COLLECTION_WRITE_MS,
            STAT_COLLECTION_WRITE_DATA_TOO_OLD,
        )

    async def write_token(self, token: Token):
        item = self.__get_token_item(token)
        await self.__write_item(
            "token",
            item["blockchain_collection_id"],
            item,
            token.data_version,
            STAT_TOKEN_WRITE,
            STAT_TOKEN_WRITE_MS,
            STAT_TOKEN_WRITE_DATA_TOO_OLD,
        )

    async def write_token_batch(self, tokens: List[Token]):
        token_items = [self.__get_token_item(token) for token in tokens]
        await self.__write_batch(
            "token",
            "blockchain_collection_id",
            token_items,
            STAT_TOKEN_WRITE_BATCH,
            STAT_TOKEN_WRITE_BATCH_MS,
        )

    async def write_token_transfer(self, token_transfer: TokenTransfer):
        item = self.__get_token_transfer_item(token_transfer)
        await self.__write_item(
            "tokentransfers",
            item["blockchain_collection_id"],
            item,
            token_transfer.data_version,
            STAT_TOKEN_TRANSFER_WRITE,
            STAT_TOKEN_TRANSFER_WRITE_MS,
            STAT_TOKEN_TRANSFER_WRITE_DATA_TOO_OLD,
        )

    async def write_token_transfer_batch(self, token_transfers: List[TokenTransfer]) -> None:
        token_transfer_items = [
            self.__get_token_transfer_item(token_transfer) for token_transfer in token_transfers
        ]
        await self.__write_batch(
            "tokentransfers",
            "blockchain_collection_id",
            token_transfer_items,
            STAT_TOKEN_TRANSFER_WRITE_BATCH,
            STAT_TOKEN_TRANSFER_WRITE_BATCH_MS,
        )

    async def write_token_owner(self, token_owner: TokenOwner):
        item = self.__get_token_owner(token_owner)
        await self.__write_item(
            "owner",
            item["blockchain_account"],
            item,
            token_owner.data_version,
            STAT_TOKEN_OWNER_WRITE,
            STAT_TOKEN_OWNER_WRITE_MS,
            STAT_TOKEN_OWNER_WRITE_DATA_TOO_OLD,
        )

    async def write_token_owner_batch(self, token_owners: List[TokenOwner]) -> None:
        token_owner_items = [self.__get_token_owner(token_owner) for token_owner in token_owners]
        await self.__write_batch(
            "owner",
            "blockchain_account",
            token_owner_items,
            STAT_TOKEN_OWNER_WRITE_BATCH,
            STAT_TOKEN_OWNER_WRITE_BATCH_MS,
        )

    async def __get_table(self, table_name: str) -> TableResource:
        return await self.__dynamodb.Table(f"{self.__table_prefix}{table_name}")

    async def __write_item(
        self,
        table_name: str,
        partition__key_value: str,
        item: dict,
        data_version: int,
        increment_stat: str,
        timer_stat: str,
        too_old_stat: str,
    ):
        with self.__stats_service.ms_counter(timer_stat):
            table_resource = await self.__get_table(table_name)
            try:
                await self.__wait_for_ready_to_send(table_name, partition__key_value)
                await table_resource.put_item(
                    Item=item,
                    ConditionExpression=Attr("data_version").not_exists()
                    | Attr("data_version").lte(data_version),
                )
                self.__stats_service.increment(increment_stat, 1)
            except self.__dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                self.__stats_service.increment(too_old_stat, 1)
                raise DataVersionTooOldException()

    async def __write_batch(
        self,
        table_name: str,
        partition_key,
        items: List[dict],
        increment_stat: str,
        timer_stat: str,
    ):
        table_resource = await self.__get_table(table_name)

        async def __write_batch_to_dynamo(batch_items):
            async with table_resource.batch_writer() as batch_writer:
                for batch_item in batch_items:
                    await self.__wait_for_ready_to_send(table_name, batch_item[partition_key])
                    with self.__stats_service.ms_counter(timer_stat):
                        await batch_writer.put_item(
                            Item=batch_item,
                        )
                    self.__stats_service.increment(increment_stat)

        items_ = items[:]
        batches: List[List[Dict]] = [[] for _ in range(self.__parallel_batches)]
        try:
            while True:
                for batch in batches:
                    batch.append(items_.pop(0))
        except IndexError:
            pass  # No more items_

        batch_writes = [__write_batch_to_dynamo(batch) for batch in batches if batch]
        await asyncio.gather(*batch_writes)

    async def __wait_for_ready_to_send(self, table: str, partition: str):
        if (
            not self.__maximum_account_items_per_second
            and not self.__maximum_table_items_per_second
            and not self.__maximum_partition_items_per_second
        ):
            return  # If we have no limits, short circuit and return

        loop = asyncio.get_running_loop()
        second = floor(loop.time())
        delayed = False

        if self.__this_second < second:
            # If we have a new second, reset all counters
            self.__this_second = second
            self.__account_items_this_second = 0
            self.__table_items_this_second.clear()
            self.__partition_items_this_second.clear()

        if self.__maximum_account_items_per_second:
            self.__account_items_this_second += 1

        if self.__maximum_table_items_per_second:
            if table in self.__table_items_this_second:
                self.__table_items_this_second[table] += 1
            else:
                self.__table_items_this_second[table] = 1

        if self.__maximum_partition_items_per_second:
            if partition in self.__partition_items_this_second:
                self.__partition_items_this_second[partition] += 1
            else:
                self.__partition_items_this_second[partition] = 1

        # We're adding parallel batches as this is the maximum number in flight that
        # are not counted in the same second with the current logic. It's way easier to
        # this than rework the logic to account for second changeover right now.
        # TODO: Account for second changeover in the counting if items for the second
        while second == self.__this_second and (
            (
                self.__maximum_account_items_per_second
                and self.__account_items_this_second
                > self.__maximum_account_items_per_second + self.__parallel_batches
            )
            or (
                self.__maximum_table_items_per_second
                and self.__table_items_this_second[table]
                > self.__maximum_table_items_per_second + self.__parallel_batches
            )
            or (
                self.__maximum_partition_items_per_second
                and self.__partition_items_this_second[partition] + self.__parallel_batches
                > self.__maximum_partition_items_per_second
            )
        ):
            await asyncio.sleep(0)
            delayed = True

        if delayed:
            self.__stats_service.increment(STAT_WRITE_DELAYED)

    @staticmethod
    def __get_token_item(token: Token) -> dict:
        item = {
            "blockchain_collection_id": f"{token.blockchain.value}" f"::{token.collection_id}",
            "token_id": token.token_id.hex_value,
            "mint_timestamp": token.mint_date.int_value,
            "mint_block_id": token.mint_block.hex_value,
            "quantity": token.quantity.int_value,
            "data_version": token.data_version,
        }
        if token.original_owner:
            item["original_owner_account"] = token.original_owner
        if token.current_owner:
            item["current_owner_account"] = token.current_owner
            item["current_owner_version"] = token.attribute_version.hex_value
        if token.metadata_url and len(token.metadata_url) < 2049:
            item["metadata_url"] = token.metadata_url
            item["metadata_url_version"] = token.attribute_version.hex_value
        return item

    @staticmethod
    def __get_token_transfer_item(token_transfer: TokenTransfer) -> dict:
        return {
            "blockchain_collection_id": f"{token_transfer.blockchain.value}"
            f"::{token_transfer.collection_id}",
            "transaction_log_index_hash": token_transfer.attribute_version.hex_value,
            "collection_id": token_transfer.collection_id,
            "token_id": token_transfer.token_id.hex_value,
            "transaction_timestamp": token_transfer.timestamp.int_value,
            "block_id": token_transfer.block_id.padded_hex(8),
            "transaction_type": token_transfer.transaction_type.value,
            "from_account": token_transfer.from_,
            "to_account": token_transfer.to_,
            "quantity": token_transfer.quantity.hex_value,
            "transaction_hash": token_transfer.transaction_hash.hex(),
            "transaction_index": token_transfer.transaction_index.hex_value,
            "log_index": token_transfer.log_index.hex_value,
            "data_version": token_transfer.data_version,
        }

    @staticmethod
    def __get_token_owner(token_owner: TokenOwner) -> dict:
        return {
            "blockchain_account": f"{token_owner.blockchain.value}::{token_owner.account}",
            "collection_id_token_id": f"{token_owner.collection_id}::{token_owner.token_id}",
            "collection_id": token_owner.collection_id,
            "token_id": token_owner.token_id.hex_value,
            "account": token_owner.account,
            "quantity": token_owner.quantity.int_value,
            "data_version": token_owner.data_version,
        }
