from typing import Tuple, List

import backoff
from botocore.exceptions import ClientError

from blockcrawler.core.bus import DataPackage, Consumer
from blockcrawler.core.types import Address, HexInt
from blockcrawler.nft.data_packages import (
    CollectionDataPackage,
    TokenTransferDataPackage,
    TokenMetadataUriUpdatedDataPackage,
)
from blockcrawler.nft.data_services import (
    DataVersionTooOldException,
    DataService,
)
from blockcrawler.nft.entities import TokenTransactionType, Token, TokenTransfer, TokenOwner


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

        await self.__write_token(token)

    @backoff.on_exception(backoff.expo, ClientError, max_tries=5)
    async def __write_token(self, token: Token):
        await self.__data_service.write_token(token)


class NftTokenTransferPersistenceConsumer(Consumer):
    def __init__(self, data_service: DataService) -> None:
        self.__data_service = data_service

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, TokenTransferDataPackage):
            return

        await self.__write_toke_transfer(data_package.token_transfer)

    @backoff.on_exception(backoff.expo, ClientError, max_tries=5)
    async def __write_toke_transfer(self, token_transfer: TokenTransfer):
        await self.__data_service.write_token_transfer(token_transfer)


class NftTokenQuantityUpdatingConsumer(Consumer):
    def __init__(self, data_service: DataService) -> None:
        self.__data_service: DataService = data_service

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

        await self.__update_quantity(quantity, token_transfer)

    @backoff.on_exception(backoff.expo, ClientError, max_tries=5)
    async def __update_quantity(self, quantity, token_transfer):
        await self.__data_service.update_token_quantity(
            blockchain=token_transfer.blockchain,
            collection_id=token_transfer.collection_id,
            token_id=token_transfer.token_id,
            quantity=quantity,
            data_version=token_transfer.data_version,
        )


class NftMetadataUriUpdatingConsumer(Consumer):
    def __init__(self, data_service: DataService) -> None:
        self.__data_service = data_service

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, TokenMetadataUriUpdatedDataPackage):
            return
        await self.__update_metadata_url(data_package)

    @backoff.on_exception(backoff.expo, ClientError, max_tries=5)
    async def __update_metadata_url(self, data_package: TokenMetadataUriUpdatedDataPackage):
        await self.__data_service.update_token_metadata_url(
            blockchain=data_package.blockchain,
            collection_id=data_package.collection_id,
            token_id=data_package.token_id,
            metadata_url=data_package.metadata_url,
            metadata_url_version=data_package.metadata_url_version,
            data_version=data_package.data_version,
        )


class NftCurrentOwnerUpdatingConsumer(Consumer):
    def __init__(self, data_service: DataService) -> None:
        self.__data_service = data_service

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, TokenTransferDataPackage):
            return

        await self.__update_token_owner(data_package.token_transfer)

    @backoff.on_exception(backoff.expo, ClientError, max_tries=5)
    async def __update_token_owner(self, token_transfer: TokenTransfer):
        await self.__data_service.update_token_current_owner(
            blockchain=token_transfer.blockchain,
            collection_id=token_transfer.collection_id,
            token_id=token_transfer.token_id,
            owner=token_transfer.to_,
            owner_version=token_transfer.attribute_version,
            data_version=token_transfer.data_version,
        )


class OwnerPersistingConsumer(Consumer):
    def __init__(self, data_service: DataService) -> None:
        self.__data_service = data_service

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, TokenTransferDataPackage):
            return

        token_transfer = data_package.token_transfer
        transfers: List[Tuple[Address, HexInt]] = []
        if token_transfer.transaction_type in (
            TokenTransactionType.MINT,
            TokenTransactionType.TRANSFER,
        ):
            transfers.append((token_transfer.to_, token_transfer.quantity))
        if token_transfer.transaction_type in (
            TokenTransactionType.BURN,
            TokenTransactionType.TRANSFER,
        ):
            transfers.append((token_transfer.from_, HexInt(0x0) - token_transfer.quantity))

        for address, quantity in transfers:
            await self.__data_service.update_token_owner(
                TokenOwner(
                    blockchain=token_transfer.blockchain,
                    account=address,
                    collection_id=token_transfer.collection_id,
                    token_id=token_transfer.token_id,
                    quantity=quantity,
                    data_version=token_transfer.data_version,
                )
            )
            await self.__data_service.delete_token_owner_with_zero_tokens(
                blockchain=token_transfer.blockchain,
                collection_id=token_transfer.collection_id,
                token_id=token_transfer.token_id,
                account=address,
            )
