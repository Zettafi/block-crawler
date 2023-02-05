import re
from typing import Optional, cast, Tuple, List, Dict, Any

import backoff
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.table import TableResource
from botocore.exceptions import ClientError

from blockcrawler.core.bus import DataPackage, Consumer, ConsumerError
from blockcrawler.core.data_clients import (
    HttpDataClient,
    IpfsDataClient,
    ArweaveDataClient,
    DataUriDataClient,
    DataClient,
    UnsupportedProtocolError,
    DataReader,
    ResourceNotFoundProtocolError,
    InvalidRequestProtocolError,
    TooManyRequestsProtocolError,
    ProtocolTimeoutError,
)
from blockcrawler.core.entities import HexInt
from blockcrawler.core.storage_clients import StorageClientContext
from blockcrawler.nft.data_services import DataVersionTooOldException, DataService
from blockcrawler.evm.types import Address
from blockcrawler.nft.data_packages import (
    CollectionDataPackage,
    TokenTransferDataPackage,
    TokenMetadataUriUpdatedDataPackage,
)
from blockcrawler.nft.entities import TokenTransactionType, Token


class NftCollectionPersistenceConsumer(Consumer):
    def __init__(self, data_service: DataService) -> None:
        self.__data_service = data_service

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, CollectionDataPackage):
            return

        try:
            await self.__data_service.write_collection(data_package.collection)
        except DataVersionTooOldException:
            pass  # It's okay if it doesn't write


class NftTokenMintPersistenceConsumer(Consumer):
    def __init__(self, data_service: DataService) -> None:
        self.__data_service = data_service

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, TokenTransferDataPackage):
            return

        token_transfer = data_package.token_transfer
        if token_transfer.transaction_type != TokenTransactionType.MINT:
            return

        token = Token(
            blockchain=token_transfer.blockchain,
            collection_id=token_transfer.collection_id,
            token_id=token_transfer.token_id,
            data_version=token_transfer.data_version,
            original_owner=token_transfer.to_,
            current_owner=token_transfer.to_,
            mint_block=token_transfer.block_id,
            mint_date=token_transfer.timestamp,
            quantity=HexInt(0),
            attribute_version=token_transfer.attribute_version,
        )

        try:
            await self.__data_service.write_token(token)
        except DataVersionTooOldException:
            pass  # It's okay if it doesn't write


class NftTokenTransferPersistenceConsumer(Consumer):
    def __init__(self, data_service: DataService) -> None:
        self.__data_service = data_service

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, TokenTransferDataPackage):
            return

        token_transfer = data_package.token_transfer

        try:
            await self.__data_service.write_token_transfer(token_transfer)
        except DataVersionTooOldException:
            pass  # It's okay if it doesn't write


class NftTokenQuantityUpdatingConsumer(Consumer):
    def __init__(self, tokens_table: TableResource) -> None:
        self.__tokens_table = tokens_table

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, TokenTransferDataPackage):
            return

        token_transfer = data_package.token_transfer
        if token_transfer.transaction_type == TokenTransactionType.MINT:
            quantity = token_transfer.quantity.int_value
        elif token_transfer.transaction_type == TokenTransactionType.BURN:
            quantity = -token_transfer.quantity.int_value
        else:
            return

        try:
            await self.__update_quantity(quantity, token_transfer)
        except Exception as e:
            raise ConsumerError(
                f"Failed to add {quantity} to quantity for token "
                f"{token_transfer.token_id.hex_value} in collection "
                f"{token_transfer.collection_id}as no token with"
                f" data version {token_transfer.data_version} exists: {token_transfer}"
                f" -- Cause {e}"
            )

    @backoff.on_exception(backoff.expo, ClientError, max_tries=5)
    async def __update_quantity(self, quantity, token_transfer):
        await self.__tokens_table.update_item(
            Key={
                "blockchain_collection_id": f"{token_transfer.blockchain.value}"
                f"::{token_transfer.collection_id}",
                "token_id": token_transfer.token_id.hex_value,
            },
            UpdateExpression="ADD quantity :q",
            ExpressionAttributeValues={":q": quantity},
            ConditionExpression=Attr("data_version").eq(token_transfer.data_version),
        )


class NftMetadataUriUpdatingConsumer(Consumer):
    def __init__(self, tokens_table: TableResource) -> None:
        self.__tokens_table = tokens_table

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, TokenMetadataUriUpdatedDataPackage):
            return

        await self.__update_metadata_uri(data_package)

    @backoff.on_exception(backoff.expo, ClientError, max_tries=5)  # TODO: Do not retry 404
    async def __update_metadata_uri(self, data_package: TokenMetadataUriUpdatedDataPackage):
        blockchain_collection_id = (
            f"{data_package.blockchain.value}" f"::{data_package.collection_id}"
        )
        try:

            await self.__tokens_table.update_item(
                Key={
                    "blockchain_collection_id": blockchain_collection_id,
                    "token_id": data_package.token_id.hex_value,
                },
                UpdateExpression="SET metadata_uri = :metadata_uri, "
                "metadata_uri_version = :metadata_uri_version",
                ExpressionAttributeValues={
                    ":metadata_uri": data_package.metadata_uri,
                    ":data_version": data_package.data_version,
                    ":metadata_uri_version": data_package.metadata_uri_version.hex_value,
                },
                ConditionExpression="data_version = :data_version"
                " AND (attribute_not_exists(metadata_uri_version)"
                " OR metadata_uri_version <= :metadata_uri_version)",
            )
        except self.__tokens_table.meta.client.exceptions.ConditionalCheckFailedException:
            result = await self.__tokens_table.get_item(
                Key={
                    "blockchain_collection_id": blockchain_collection_id,
                    "token_id": data_package.token_id.hex_value,
                }
            )

            if (
                "Item" in result
                and "metadata_uri_version" in result["Item"]
                and HexInt(result["Item"]["metadata_uri_version"]).hex_value
                > data_package.metadata_uri_version.hex_value
            ):
                # Metadata URI version in the table is greater than the current, don't update it
                pass
            else:
                # Either the token does not exist yet or the data_version is not correct, re-raise
                raise ConsumerError(
                    f"Failed to update metadata URI for token {data_package.token_id.int_value}"
                    f" in collection {data_package.collection_id} as no token found with"
                    f" data version {data_package.data_version} -- Data Package: {data_package}"
                    f" -- Tokens Table Record: {result}"
                )


class NftTokenMetadataPersistingConsumer(Consumer):
    PROTOCOL_MATCH_REGEX = re.compile("^(?:([^:]+)://|(data):).+$")
    HTTP_IPFS_MATCH_REGEX = re.compile("^https?://.+/ipfs/(.+)$")

    def __init__(
        self,
        http_client: HttpDataClient,
        ipfs_client: IpfsDataClient,
        arweave_client: ArweaveDataClient,
        data_uri_client: DataUriDataClient,
        storage_client_context: StorageClientContext,
    ) -> None:
        self.__http_client = http_client
        self.__ipfs_client = ipfs_client
        self.__arweave_client = arweave_client
        self.__data_uri_client = data_uri_client
        self.__storage_client_context = storage_client_context

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, TokenMetadataUriUpdatedDataPackage):
            return

        try:
            uri = self.__sanitize_uri(data_package.metadata_uri)
            data_client = self.__get_data_client_for_uri(uri)

            async with self.get_metadata(data_client, uri) as (content_type, data_reader):
                key = (
                    f"{data_package.blockchain.value}"
                    f"/{data_package.collection_id}"
                    f"/{data_package.token_id.hex_value}"
                    f"/{data_package.metadata_uri_version.hex_value}"
                )
                try:
                    await self.__storage_client_context.store(
                        key,
                        cast(DataReader, data_reader),
                        cast(str, content_type),
                    )
                except Exception as e:
                    raise ConsumerError(
                        f"Failed to store Token metadata " f"{data_package} -- Cause {e}"
                    )
        except (ConsumerError, TooManyRequestsProtocolError):
            raise
        except (
            ResourceNotFoundProtocolError,
            InvalidRequestProtocolError,
            UnsupportedProtocolError,
        ):
            pass
        except Exception as e:
            raise ConsumerError(
                f"Failed to retrieve metadata for Token URI " f"{data_package} -- Cause {e}"
            )

    @backoff.on_exception(
        backoff.runtime,
        TooManyRequestsProtocolError,
        value=lambda r: r.retry_after,
        jitter=None,
    )
    @backoff.on_exception(
        backoff.expo,
        ProtocolTimeoutError,
        max_value=16,
        jitter=backoff.full_jitter,
    )
    def get_metadata(self, data_client, uri):
        return data_client.get(uri)

    def __get_data_client_for_uri(self, uri: str) -> DataClient:
        proto = self.__get_protocol_from_uri(uri)
        if proto in ("https", "http"):
            data_client: DataClient = self.__http_client
        elif proto == "ipfs":
            data_client = self.__ipfs_client
        elif proto == "ar":
            data_client = self.__arweave_client
        elif proto == "data":
            data_client = self.__data_uri_client
        else:
            raise UnsupportedProtocolError(f"Protocol for URI {uri} is not supported!")
        return data_client

    def __get_protocol_from_uri(self, uri: str) -> Optional[str]:
        match = self.PROTOCOL_MATCH_REGEX.fullmatch(uri)
        if match and match.group(1):
            protocol = match.group(1)
        elif match and match.group(2):
            protocol = match.group(2)
        else:
            protocol = None
        return protocol

    @classmethod
    def __sanitize_uri(cls, uri: str):
        match = cls.HTTP_IPFS_MATCH_REGEX.match(uri)
        if match:
            sanitized = "ipfs://" + match.group(1)
        else:
            sanitized = uri
        return sanitized


class CurrentOwnerPersistingConsumer(Consumer):
    def __init__(self, owners_table: TableResource) -> None:
        self.__owners_table = owners_table

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, TokenTransferDataPackage):
            return

        token_transfer = data_package.token_transfer
        blockchain = token_transfer.blockchain.value
        transfers: List[Tuple[Address, int]] = []
        if token_transfer.transaction_type in (
            TokenTransactionType.MINT,
            TokenTransactionType.TRANSFER,
        ):
            transfers.append((token_transfer.to_, token_transfer.quantity.int_value))
        if token_transfer.transaction_type in (
            TokenTransactionType.BURN,
            TokenTransactionType.TRANSFER,
        ):
            transfers.append((token_transfer.from_, -token_transfer.quantity.int_value))

        for address, quantity in transfers:
            table_key = {
                "blockchain_account": f"{blockchain}::{address}",
                "collection_id_token_id": f"{token_transfer.collection_id}"
                f"::{token_transfer.token_id.hex_value}",
            }
            error_template = (
                f"Failed to add {quantity} to quantity for owner {address} "
                f"and token {token_transfer.token_id.hex_value}as no "
                f"owner/token with data version less than or equal to "
                f"{token_transfer.data_version} exists: {token_transfer}"
                f" -- Cause: %s"
            )

            update_params = {
                "Key": table_key,
                "UpdateExpression": "SET collection_id = :cid"
                ",token_id = :tid"
                ",account = :a"
                ",data_version = :dv"
                " ADD quantity :q",
                "ExpressionAttributeValues": {
                    ":cid": str(token_transfer.collection_id),
                    ":tid": token_transfer.token_id.hex_value,
                    ":a": address,
                    ":q": quantity,
                    ":dv": token_transfer.data_version,
                },
                "ConditionExpression": (
                    Attr("data_version").not_exists()
                    | Attr("data_version").eq(token_transfer.data_version)
                ),
            }
            try:
                await self.__owners_table.update_item(**update_params)

                try:
                    await self.__owners_table.delete_item(
                        Key=table_key,
                        ConditionExpression=(Attr("quantity").eq(0)),
                    )
                except self.__owners_table.meta.client.exceptions.ConditionalCheckFailedException:
                    pass  # Not worried if it wasn't 0.

            except self.__owners_table.meta.client.exceptions.ConditionalCheckFailedException as e:
                result = await self.__owners_table.get_item(Key=table_key)

                if (
                    "Item" not in result
                    or result["Item"]["data_version"] >= token_transfer.data_version
                ):
                    raise ConsumerError(error_template.format(e))

                item: Dict[str, Any] = table_key.copy()
                item["collection_id"] = str(token_transfer.collection_id)
                item["token_id"] = token_transfer.token_id.hex_value
                item["account"] = address
                item["quantity"] = quantity
                item["data_version"] = token_transfer.data_version
                try:
                    await self.__owners_table.put_item(
                        Item=item,
                        ConditionExpression=Attr("data_version").lt(token_transfer.data_version),
                    )
                except self.__owners_table.meta.client.exceptions.ConditionalCheckFailedException:
                    try:
                        await self.__owners_table.update_item(**update_params)
                    except Exception as e:
                        raise ConsumerError(error_template.format(e))
                except Exception as e:
                    raise ConsumerError(error_template.format(e))
            except ConsumerError:
                raise
            except Exception as e:
                raise ConsumerError(error_template.format(e))
