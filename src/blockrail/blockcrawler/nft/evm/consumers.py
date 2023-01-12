import asyncio
from abc import ABC
from typing import List, Dict, cast, Tuple

from eth_abi import decode

from blockrail.blockcrawler.core.bus import DataPackage, Transformer, DataBus, ConsumerError
from blockrail.blockcrawler.core.entities import HexInt
from blockrail.blockcrawler.core.rpc import RpcServerError, RpcDecodeError
from blockrail.blockcrawler.core.services import BlockTimeService
from blockrail.blockcrawler.evm.rpc import EvmRpcClient, EthCall
from blockrail.blockcrawler.evm.types import Address, EvmLog
from blockrail.blockcrawler.evm.util import Erc721Events, Erc1155Events, Erc721MetadataFunctions
from blockrail.blockcrawler.nft.data_packages import (
    CollectionDataPackage,
    TokenMetadataUriUpdatedDataPackage,
)
from blockrail.blockcrawler.nft.data_services import DataService
from blockrail.blockcrawler.nft.entities import (
    EthereumCollectionType,
    TokenTransfer,
    TokenTransactionType,
    Token,
    TokenOwner,
    Collection,
)
from blockrail.blockcrawler.nft.evm.oracles import TokenTransactionTypeOracle, LogVersionOracle


class CollectionToEverythingElseCollectionBasedConsumerBaseClass(Transformer, ABC):
    def __init__(
        self,
        data_bus: DataBus,
        data_service: DataService,
        block_time_service: BlockTimeService,
        log_version_oracle: LogVersionOracle,
    ):
        super().__init__(data_bus)
        self.__data_service = data_service
        self.__block_time_service = block_time_service
        self.__log_version_oracle = log_version_oracle

    async def _process_data(
        self,
        metadata_uri_updates: List[TokenMetadataUriUpdatedDataPackage],
        token_owners: List[TokenOwner],
        token_transfers: List[TokenTransfer],
        tokens: List[Token],
    ):
        batches = list()
        if tokens:
            batches.append(self.__data_service.write_token_batch(tokens))
        if token_transfers:
            batches.append(self.__data_service.write_token_transfer_batch(token_transfers))
        if token_owners:
            batches.append(self.__data_service.write_token_owner_batch(token_owners))
        for metadata_uri_update in metadata_uri_updates:
            batches.append(self._get_data_bus().send(metadata_uri_update))
        await asyncio.gather(*batches)

    async def _process_token_and_transfers(
        self,
        tokens: Dict[HexInt, Token],
        token_transfers: List[TokenTransfer],
        collection: Collection,
        log: EvmLog,
        transaction_type: TokenTransactionType,
        from_address: Address,
        to_address: Address,
        token_id: HexInt,
        quantity: HexInt,
    ):
        block_time = await self.__block_time_service.get_block_timestamp(log.block_number)
        attribute_version = self.__log_version_oracle.version_from_log(log)

        token_transfers.append(
            TokenTransfer(
                blockchain=collection.blockchain,
                data_version=collection.data_version,
                collection_id=collection.collection_id,
                collection_type=collection.specification,
                token_id=token_id,
                timestamp=block_time,
                transaction_type=transaction_type,
                from_=from_address,
                to_=to_address,
                quantity=quantity,
                block_id=log.block_number,
                transaction_hash=log.transaction_hash,
                transaction_index=log.transaction_index,
                log_index=log.log_index,
                attribute_version=attribute_version,
            )
        )

        current_owner = (
            to_address if collection.specification is EthereumCollectionType.ERC721 else None
        )
        if transaction_type == TokenTransactionType.MINT:
            if token_id in tokens:
                token_quantity = tokens[token_id].quantity + quantity
                original_owner = tokens[token_id].original_owner
            else:
                token_quantity = quantity
                original_owner = to_address
            tokens[token_id] = Token(
                blockchain=collection.blockchain,
                collection_id=collection.collection_id,
                token_id=token_id,
                data_version=collection.data_version,
                original_owner=original_owner,
                current_owner=current_owner,
                mint_block=log.block_number,
                mint_date=block_time,
                quantity=token_quantity,
                attribute_version=attribute_version,
            )

        elif transaction_type == TokenTransactionType.TRANSFER:
            if token_id in tokens:
                old_token = tokens[token_id]
                tokens[token_id] = Token(
                    blockchain=old_token.blockchain,
                    collection_id=old_token.collection_id,
                    token_id=old_token.token_id,
                    data_version=old_token.data_version,
                    original_owner=old_token.original_owner,
                    current_owner=current_owner,
                    mint_block=old_token.mint_block,
                    mint_date=old_token.mint_date,
                    quantity=old_token.quantity,
                    attribute_version=old_token.attribute_version,
                    metadata_url=old_token.metadata_url,
                )

        elif transaction_type == TokenTransactionType.BURN:
            if token_id in tokens:
                old_token = tokens[token_id]
                tokens[token_id] = Token(
                    blockchain=old_token.blockchain,
                    collection_id=old_token.collection_id,
                    token_id=old_token.token_id,
                    data_version=old_token.data_version,
                    original_owner=old_token.original_owner,
                    current_owner=current_owner,
                    mint_block=old_token.mint_block,
                    mint_date=old_token.mint_date,
                    quantity=old_token.quantity - quantity,
                    attribute_version=attribute_version,
                    metadata_url=old_token.metadata_url,
                )


class CollectionToEverythingElseErc721CollectionBasedConsumer(
    CollectionToEverythingElseCollectionBasedConsumerBaseClass
):
    def __init__(
        self,
        data_bus: DataBus,
        data_service: DataService,
        rpc_client: EvmRpcClient,
        block_time_service: BlockTimeService,
        token_transaction_type_oracle: TokenTransactionTypeOracle,
        log_version_oracle: LogVersionOracle,
        max_block_height: HexInt,
    ) -> None:
        super().__init__(
            data_bus=data_bus,
            data_service=data_service,
            block_time_service=block_time_service,
            log_version_oracle=log_version_oracle,
        )
        self.__rpc_client = rpc_client
        self.__token_transaction_type_oracle = token_transaction_type_oracle
        self.__max_block_height = max_block_height

    async def receive(self, data_package: DataPackage):
        if (
            not isinstance(data_package, CollectionDataPackage)
            or data_package.collection.specification is not EthereumCollectionType.ERC721
        ):
            return

        try:
            tokens: Dict[HexInt, Token] = dict()
            token_transfers: List[TokenTransfer] = list()
            token_owners: Dict[HexInt, TokenOwner] = dict()
            metadata_uri_updates: Dict[HexInt, TokenMetadataUriUpdatedDataPackage] = dict()

            async for log in self.__rpc_client.get_logs(
                topics=[Erc721Events.TRANSFER.event_signature_hash.hex()],
                from_block=data_package.collection.block_created,
                to_block=self.__max_block_height,
                address=data_package.collection.collection_id,
            ):
                if len(log.topics) != 4:
                    continue  # Ignore other same signature events such as ERC-20 Transfer

                from_address = Address(
                    decode(
                        ["address"],
                        log.topics[1],
                    )[0]
                )
                to_address = Address(
                    decode(
                        ["address"],
                        log.topics[2],
                    )[0]
                )
                token_id = HexInt(
                    decode(
                        ["uint256"],
                        log.topics[3],
                    )[0]
                )
                transaction_type = self.__token_transaction_type_oracle.type_from_log(log)

                await self._process_token_and_transfers(
                    tokens=tokens,
                    token_transfers=token_transfers,
                    collection=data_package.collection,
                    log=log,
                    transaction_type=transaction_type,
                    from_address=from_address,
                    to_address=to_address,
                    token_id=token_id,
                    quantity=HexInt(1),
                )

                await self.__process_token_owners(
                    token_owners, data_package.collection, to_address, token_id, transaction_type
                )

            calls = list()
            for token_id, token in tokens.items():
                calls.append(
                    self.__rpc_client.call(
                        EthCall(
                            from_=None,
                            to=token.collection_id,
                            function=Erc721MetadataFunctions.TOKEN_URI,
                            parameters=[token.token_id.int_value],
                            block=token.mint_block.hex_value,
                        )
                    )
                )

            results = await asyncio.gather(*calls, return_exceptions=True)
            for (token_id, token), result in zip(tokens.items(), results):
                if (
                    isinstance(result, RpcServerError)
                    and result.error_code in (-32000, 3)
                    # -32000 is generic catch-all for execution reverted
                    # 3 is query for non-existent token
                    or isinstance(result, RpcDecodeError)
                ):
                    continue
                elif isinstance(result, Exception):
                    raise result
                else:
                    metadata_url = cast(Tuple, result)[0]

                metadata_uri_updates[token_id] = TokenMetadataUriUpdatedDataPackage(
                    blockchain=token.blockchain,
                    collection_id=token.collection_id,
                    token_id=token_id,
                    metadata_uri=metadata_url,
                    metadata_uri_version=token.attribute_version,
                    data_version=token.data_version,
                )
                tokens[token_id] = Token(
                    blockchain=token.blockchain,
                    collection_id=token.collection_id,
                    token_id=token.token_id,
                    data_version=token.data_version,
                    original_owner=token.original_owner,
                    current_owner=token.current_owner,
                    mint_block=token.mint_block,
                    mint_date=token.mint_date,
                    quantity=token.quantity,
                    attribute_version=token.attribute_version,
                    metadata_url=metadata_url,
                )

            await self._process_data(
                metadata_uri_updates=[value for value in metadata_uri_updates.values()],
                token_owners=[value for value in token_owners.values()],
                token_transfers=token_transfers,
                tokens=[value for value in tokens.values()],
            )
        except Exception as e:
            raise ConsumerError(
                f"Error processing {data_package.collection.specification} "
                f"collection {data_package.collection.collection_id} "
                f"created in block {data_package.collection.block_created} "
                f"-- {e}"
            )

    @staticmethod
    async def __process_token_owners(
        token_owners: Dict[HexInt, TokenOwner],
        collection: Collection,
        to_address: Address,
        token_id: HexInt,
        transaction_type: TokenTransactionType,
    ):
        if transaction_type == TokenTransactionType.MINT:
            token_owners[token_id] = TokenOwner(
                blockchain=collection.blockchain,
                collection_id=collection.collection_id,
                token_id=token_id,
                account=to_address,
                quantity=HexInt(1),
                data_version=collection.data_version,
            )
        elif transaction_type == TokenTransactionType.TRANSFER:
            if token_id in token_owners:
                old_token_owner = token_owners[token_id]
                token_owners[token_id] = TokenOwner(
                    blockchain=old_token_owner.blockchain,
                    collection_id=old_token_owner.collection_id,
                    token_id=old_token_owner.token_id,
                    account=to_address,
                    quantity=old_token_owner.quantity,
                    data_version=old_token_owner.data_version,
                )
        elif transaction_type == TokenTransactionType.BURN:
            if token_id in token_owners:
                del token_owners[token_id]


class CollectionToEverythingElseErc1155CollectionBasedConsumer(
    CollectionToEverythingElseCollectionBasedConsumerBaseClass
):
    def __init__(
        self,
        data_bus: DataBus,
        data_service: DataService,
        rpc_client: EvmRpcClient,
        block_time_service: BlockTimeService,
        token_transaction_type_oracle: TokenTransactionTypeOracle,
        log_version_oracle: LogVersionOracle,
        max_block_height: HexInt,
    ) -> None:
        super().__init__(
            data_bus=data_bus,
            data_service=data_service,
            block_time_service=block_time_service,
            log_version_oracle=log_version_oracle,
        )
        self.__data_service = data_service
        self.__rpc_client = rpc_client
        self.__token_transaction_type_oracle = token_transaction_type_oracle
        self.__log_version_oracle = log_version_oracle
        self.__max_block_height = max_block_height

    async def receive(self, data_package: DataPackage):
        if (
            not isinstance(data_package, CollectionDataPackage)
            or data_package.collection.specification is not EthereumCollectionType.ERC1155
        ):
            return

        try:

            tokens: Dict[HexInt, Token] = dict()
            token_transfers: List[TokenTransfer] = list()
            token_owners: Dict[HexInt, Dict[Address, TokenOwner]] = dict()
            metadata_uri_updates: Dict[HexInt, TokenMetadataUriUpdatedDataPackage] = dict()

            async for log in self.__rpc_client.get_logs(
                topics=[
                    [
                        Erc1155Events.TRANSFER_SINGLE.event_signature_hash.hex(),
                        Erc1155Events.TRANSFER_BATCH.event_signature_hash.hex(),
                        Erc1155Events.URI.event_signature_hash.hex(),
                    ]
                ],
                from_block=data_package.collection.block_created,
                to_block=self.__max_block_height,
                address=data_package.collection.collection_id,
            ):

                if log.topics[0] == Erc1155Events.TRANSFER_SINGLE.event_signature_hash:

                    from_address = Address(
                        decode(
                            ["address"],
                            log.topics[2],
                        )[0]
                    )
                    to_address = Address(
                        decode(
                            ["address"],
                            log.topics[3],
                        )[0]
                    )
                    token_id, quantity = (
                        HexInt(decoded)
                        for decoded in decode(
                            ["uint256", "uint256"],
                            log.data,
                        )
                    )
                    transaction_type = self.__token_transaction_type_oracle.type_from_log(log)
                    await self._process_token_and_transfers(
                        tokens=tokens,
                        token_transfers=token_transfers,
                        collection=data_package.collection,
                        log=log,
                        transaction_type=transaction_type,
                        from_address=from_address,
                        to_address=to_address,
                        token_id=token_id,
                        quantity=quantity,
                    )
                    await self.__process_token_owner(
                        token_owners=token_owners,
                        collection=data_package.collection,
                        from_address=from_address,
                        to_address=to_address,
                        token_id=token_id,
                        quantity=quantity,
                        transaction_type=transaction_type,
                    )

                elif log.topics[0] == Erc1155Events.TRANSFER_BATCH.event_signature_hash:
                    from_address = Address(
                        decode(
                            ["address"],
                            log.topics[2],
                        )[0]
                    )
                    to_address = Address(
                        decode(
                            ["address"],
                            log.topics[3],
                        )[0]
                    )
                    token_ids, quantities = decode(
                        ["uint256[]", "uint256[]"],
                        log.data,
                    )
                    transaction_type = self.__token_transaction_type_oracle.type_from_log(log)

                    for token_id, quantity in zip(token_ids, quantities):
                        await self._process_token_and_transfers(
                            tokens=tokens,
                            token_transfers=token_transfers,
                            collection=data_package.collection,
                            log=log,
                            transaction_type=transaction_type,
                            from_address=from_address,
                            to_address=to_address,
                            token_id=HexInt(cast(int, token_id)),
                            quantity=HexInt(cast(int, quantity)),
                        )
                        await self.__process_token_owner(
                            token_owners=token_owners,
                            collection=data_package.collection,
                            from_address=from_address,
                            to_address=to_address,
                            token_id=HexInt(cast(int, token_id)),
                            quantity=HexInt(cast(int, quantity)),
                            transaction_type=transaction_type,
                        )

                elif log.topics[0] == Erc1155Events.URI.event_signature_hash:
                    token_id = HexInt(
                        decode(
                            ["uint256"],
                            log.topics[1],
                        )[0]
                    )
                    uri = decode(
                        ["string"],
                        log.data,
                    )[0]

                    attribute_version = self.__log_version_oracle.version_from_log(log)
                    metadata_uri_updates[token_id] = TokenMetadataUriUpdatedDataPackage(
                        blockchain=data_package.collection.blockchain,
                        collection_id=data_package.collection.collection_id,
                        token_id=token_id,
                        metadata_uri=uri,
                        metadata_uri_version=attribute_version,
                        data_version=data_package.collection.data_version,
                    )
                    if token_id in tokens:
                        token = tokens[token_id]
                        tokens[token_id] = Token(
                            blockchain=token.blockchain,
                            collection_id=token.collection_id,
                            token_id=token.token_id,
                            data_version=token.data_version,
                            original_owner=token.original_owner,
                            current_owner=token.current_owner,
                            mint_block=token.mint_block,
                            mint_date=token.mint_date,
                            quantity=token.quantity,
                            attribute_version=token.attribute_version,
                            metadata_url=uri,
                        )

            token_owners_data: List[TokenOwner] = list()
            for oto in token_owners.values():
                token_owners_data.extend(oto.values())

            await self._process_data(
                metadata_uri_updates=[update for update in metadata_uri_updates.values()],
                token_owners=[owner for owner in token_owners_data if owner.quantity > 0],
                token_transfers=token_transfers,
                tokens=[token for token in tokens.values()],
            )
        except Exception as e:
            raise ConsumerError(
                f"Error processing {data_package.collection.specification} "
                f"collection {data_package.collection.collection_id} "
                f"created in block {data_package.collection.block_created} "
                f"-- {e}"
            )

    @staticmethod
    async def __process_token_owner(
        token_owners: Dict[HexInt, Dict[Address, TokenOwner]],
        collection: Collection,
        from_address: Address,
        to_address: Address,
        token_id: HexInt,
        quantity: HexInt,
        transaction_type: TokenTransactionType,
    ):
        def __add_token_quantity_to_owner(
            token_owners_: Dict[HexInt, Dict[Address, TokenOwner]],
            collection_: Collection,
            quantity_: HexInt,
            address: Address,
            token_id_: HexInt,
        ):
            if address in token_owners_[token_id]:
                token_quantity = token_owners[token_id_][address].quantity + quantity_
            else:
                token_quantity = quantity_
            token_owners_[token_id_][address] = TokenOwner(
                blockchain=collection_.blockchain,
                collection_id=collection_.collection_id,
                token_id=token_id_,
                account=address,
                quantity=token_quantity,
                data_version=collection_.data_version,
            )

        if token_id not in token_owners:
            token_owners[token_id] = dict()

        if transaction_type in (TokenTransactionType.MINT, TokenTransactionType.TRANSFER):
            __add_token_quantity_to_owner(
                token_owners_=token_owners,
                collection_=collection,
                quantity_=quantity,
                address=to_address,
                token_id_=token_id,
            )
        if transaction_type in (TokenTransactionType.BURN, TokenTransactionType.TRANSFER):
            __add_token_quantity_to_owner(
                token_owners_=token_owners,
                collection_=collection,
                quantity_=0 - quantity,
                address=from_address,
                token_id_=token_id,
            )
