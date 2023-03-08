import asyncio
from abc import ABC
from asyncio import Task
from typing import List, Dict, cast, Tuple, Awaitable, Callable, Any

from eth_abi import decode

from blockcrawler.core.bus import DataPackage, ConsumerError, Consumer
from blockcrawler.core.rpc import RpcServerError, RpcDecodeError
from blockcrawler.core.types import Address, HexInt
from blockcrawler.evm.rpc import EvmRpcClient, EthCall
from blockcrawler.evm.services import BlockTimeService
from blockcrawler.evm.types import (
    EvmLog,
    Erc721MetadataFunctions,
    Erc721Events,
    Erc1155Events,
    Erc165Functions,
    Erc165InterfaceID,
)
from blockcrawler.nft.data_packages import (
    CollectionDataPackage,
)
from blockcrawler.nft.data_services import DataService
from blockcrawler.nft.entities import (
    EthereumCollectionType,
    TokenTransfer,
    TokenTransactionType,
    Token,
    TokenOwner,
    Collection,
)
from blockcrawler.nft.evm.oracles import TokenTransactionTypeOracle, LogVersionOracle

TokenOwnerRecord = Tuple[TokenOwner, HexInt]


class CollectionToEverythingElseCollectionBasedConsumerBaseClass(Consumer, ABC):
    def __init__(
        self,
        block_time_service: BlockTimeService,
        log_version_oracle: LogVersionOracle,
        write_batch_size: int,
        max_concurrent_batch_writes: int,
    ):
        self._write_batch_size = write_batch_size
        self._max_concurrent_batch_writes = max_concurrent_batch_writes
        self.__block_time_service = block_time_service
        self.__log_version_oracle = log_version_oracle

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

        if token_id in tokens and tokens[token_id].attribute_version > attribute_version:
            current_owner = tokens[token_id].current_owner
            attribute_version = tokens[token_id].attribute_version
        else:
            current_owner = to_address

        if collection.specification is EthereumCollectionType.ERC1155:
            current_owner = None

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
                    attribute_version=attribute_version,
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

    async def _wait_for_ready_to_add_batch_task(self, batch_tasks):
        """
        Function for waiting until the current batch tasks
        limit is not exceeded.
        """
        while len(batch_tasks) >= self._max_concurrent_batch_writes:
            await asyncio.sleep(0)
            for task in batch_tasks:
                if task.done():
                    batch_tasks.remove(task)

    async def _process_batches(self, items: List[Any], processor: Callable):
        tasks: List[Task] = []
        batch_items: List[Any] = []
        batch_items_batches: List[List[Any]] = []
        for token in items:
            batch_items.append(token)
            if len(batch_items) >= self._write_batch_size:
                batch_items_batches.append(batch_items[:])
                batch_items.clear()
        if batch_items:
            batch_items_batches.append(batch_items)
        for batch_items_batch in batch_items_batches:
            await self._wait_for_ready_to_add_batch_task(tasks)
            tasks.append(asyncio.create_task(processor(batch_items_batch)))
        await asyncio.gather(*tasks)


class CollectionToEverythingElseErc721CollectionBasedConsumer(
    CollectionToEverythingElseCollectionBasedConsumerBaseClass
):
    def __init__(
        self,
        data_service: DataService,
        rpc_client: EvmRpcClient,
        block_time_service: BlockTimeService,
        token_transaction_type_oracle: TokenTransactionTypeOracle,
        log_version_oracle: LogVersionOracle,
        max_block_height: HexInt,
        write_batch_size: int = 100,
        max_concurrent_batch_writes: int = 25,
    ) -> None:
        super().__init__(
            block_time_service=block_time_service,
            log_version_oracle=log_version_oracle,
            write_batch_size=write_batch_size,
            max_concurrent_batch_writes=max_concurrent_batch_writes,
        )
        self.__data_service = data_service
        self.__rpc_client = rpc_client
        self.__token_transaction_type_oracle = token_transaction_type_oracle
        self.__log_version_oracle = log_version_oracle
        self.__max_block_height = max_block_height

    async def receive(self, data_package: DataPackage):
        if (
            not isinstance(data_package, CollectionDataPackage)
            or data_package.collection.specification is not EthereumCollectionType.ERC721
        ):
            return

        try:
            log_batch: List[Awaitable] = []
            token_xfr_batch_write_tasks: List[Task] = []
            tokens: Dict[HexInt, Token] = {}
            token_transfers: List[TokenTransfer] = []
            token_owners: Dict[HexInt, TokenOwnerRecord] = {}

            async for log in self.__rpc_client.get_logs(
                topics=[Erc721Events.TRANSFER.event_signature_hash.hex()],
                from_block=data_package.collection.block_created,
                to_block=self.__max_block_height,
                address=data_package.collection.collection_id,
                starting_block_range_size=100_000,
            ):
                if len(log.topics) != 4:
                    continue  # Ignore other same signature events such as ERC-20 Transfer

                log_batch.append(
                    self.__process_log_entry(
                        data_package, log, token_owners, token_transfers, tokens
                    )
                )
                if len(log_batch) >= self._write_batch_size:
                    await asyncio.gather(*log_batch)
                    log_batch.clear()

                    await self._wait_for_ready_to_add_batch_task(token_xfr_batch_write_tasks)
                    token_xfr_batch_write_tasks.append(
                        asyncio.create_task(
                            self.__data_service.write_token_transfer_batch(token_transfers[:])
                        )
                    )
                    await asyncio.sleep(0)  # Start processing token transfer saves to clear memory
                    token_transfers.clear()

            if log_batch:  # Gather any remaining log batch entries
                await asyncio.gather(*log_batch)

            if token_transfers:  # Write any remaining token transfers
                await self._wait_for_ready_to_add_batch_task(token_xfr_batch_write_tasks)
                token_xfr_batch_write_tasks.append(
                    asyncio.create_task(
                        self.__data_service.write_token_transfer_batch(token_transfers)
                    )
                )

            cleaned_token_owners = [
                value for value, _ in token_owners.values() if value.quantity != 0
            ]
            write_token_owners_task = asyncio.create_task(
                self._process_batches(
                    cleaned_token_owners, self.__data_service.write_token_owner_batch
                )
            )

            tokens_list = [token for _, token in tokens.items()]
            (supports_metadata,) = await self.__rpc_client.call(
                EthCall(
                    from_=None,
                    to=data_package.collection.collection_id,
                    function=Erc165Functions.SUPPORTS_INTERFACE,
                    parameters=[Erc165InterfaceID.ERC721_METADATA.bytes],
                )
            )
            if supports_metadata:
                # If the contract supports ERC-721 Metadata, process token URIs
                tokens_batch_processor = self.__process_tokens_batch
            else:
                # If not, just write it
                tokens_batch_processor = self.__data_service.write_token_batch

            write_tokens_task = asyncio.create_task(
                self._process_batches(tokens_list, tokens_batch_processor)
            )

            await asyncio.gather(
                *token_xfr_batch_write_tasks, write_token_owners_task, write_tokens_task
            )

        except Exception as e:
            raise ConsumerError(
                f"Error processing {data_package.collection.specification} "
                f"collection {data_package.collection.collection_id} "
                f"created in block {data_package.collection.block_created} "
                f"-- {e}"
            )

    async def __process_log_entry(
        self,
        data_package: CollectionDataPackage,
        log: EvmLog,
        token_owners: Dict[HexInt, TokenOwnerRecord],
        token_transfers: List[TokenTransfer],
        tokens: Dict[HexInt, Token],
    ):
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
        owner_log_entry_version = self.__log_version_oracle.version_from_log(log)
        await self.__process_token_owners(
            token_owners,
            data_package.collection,
            to_address,
            token_id,
            transaction_type,
            owner_log_entry_version,
        )

    async def __process_tokens_batch(self, tokens: List[Token]):
        calls = []
        for token in tokens:
            calls.append(
                self.__rpc_client.call(
                    EthCall(
                        from_=None,
                        to=token.collection_id,
                        function=Erc721MetadataFunctions.TOKEN_URI,
                        parameters=[token.token_id.int_value],
                        block=token.mint_block,
                    )
                )
            )
        results = await asyncio.gather(*calls, return_exceptions=True)
        completed_tokens: List[Token] = []
        for token, result in zip(tokens, results):
            if (
                isinstance(result, RpcServerError)
                and result.error_code in (-32000, 3)
                # -32000 is generic catch-all for execution reverted
                # 3 is query for non-existent token
                or isinstance(result, RpcDecodeError)
            ):
                metadata_url = None
            elif isinstance(result, Exception):
                raise result
            else:
                metadata_url = cast(Tuple, result)[0]

            completed_tokens.append(
                Token(
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
            )

        await self.__data_service.write_token_batch(completed_tokens)

    @staticmethod
    async def __process_token_owners(
        token_owners: Dict[HexInt, TokenOwnerRecord],
        collection: Collection,
        to_address: Address,
        token_id: HexInt,
        transaction_type: TokenTransactionType,
        owner_log_entry_version: HexInt,
    ):
        if token_id in token_owners and token_owners[token_id][1] > owner_log_entry_version:
            # This log entry occurred before the log entry for the existing record
            # and should be ignored
            return

        if transaction_type == TokenTransactionType.MINT:
            token_owners[token_id] = (
                TokenOwner(
                    blockchain=collection.blockchain,
                    collection_id=collection.collection_id,
                    token_id=token_id,
                    account=to_address,
                    quantity=HexInt(1),
                    data_version=collection.data_version,
                ),
                owner_log_entry_version,
            )
        elif transaction_type == TokenTransactionType.TRANSFER:
            if token_id in token_owners:
                (old_token_owner, _) = token_owners[token_id]
                token_owners[token_id] = (
                    TokenOwner(
                        blockchain=old_token_owner.blockchain,
                        collection_id=old_token_owner.collection_id,
                        token_id=old_token_owner.token_id,
                        account=to_address,
                        quantity=old_token_owner.quantity,
                        data_version=old_token_owner.data_version,
                    ),
                    owner_log_entry_version,
                )
        elif transaction_type == TokenTransactionType.BURN:
            if token_id in token_owners:
                del token_owners[token_id]


class CollectionToEverythingElseErc1155CollectionBasedConsumer(
    CollectionToEverythingElseCollectionBasedConsumerBaseClass
):
    HEX_INT_ZERO = HexInt(0)

    def __init__(
        self,
        data_service: DataService,
        rpc_client: EvmRpcClient,
        block_time_service: BlockTimeService,
        token_transaction_type_oracle: TokenTransactionTypeOracle,
        log_version_oracle: LogVersionOracle,
        max_block_height: HexInt,
        write_batch_size: int = 100,
        max_concurrent_batch_writes: int = 25,
    ) -> None:
        super().__init__(
            block_time_service=block_time_service,
            log_version_oracle=log_version_oracle,
            max_concurrent_batch_writes=max_concurrent_batch_writes,
            write_batch_size=write_batch_size,
        )
        self.__data_service = data_service
        self.__rpc_client = rpc_client
        self.__token_transaction_type_oracle = token_transaction_type_oracle
        self.__max_block_height = max_block_height

    async def receive(self, data_package: DataPackage):
        if (
            not isinstance(data_package, CollectionDataPackage)
            or data_package.collection.specification is not EthereumCollectionType.ERC1155
        ):
            return

        try:
            log_batch: List[Awaitable] = []
            token_xfr_batch_write_tasks: List[Task] = []
            tokens: Dict[HexInt, Token] = {}
            token_transfers: List[TokenTransfer] = []
            token_owners: Dict[HexInt, Dict[Address, TokenOwner]] = {}

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
                starting_block_range_size=100_000,
            ):
                log_batch.append(
                    self.__process_log_entry(
                        data_package, log, token_owners, token_transfers, tokens
                    )
                )
                if len(log_batch) >= self._write_batch_size:
                    await asyncio.gather(*log_batch)
                    log_batch.clear()

                    await self._wait_for_ready_to_add_batch_task(token_xfr_batch_write_tasks)
                    token_xfr_batch_write_tasks.append(
                        asyncio.create_task(
                            self.__data_service.write_token_transfer_batch(token_transfers[:])
                        )
                    )
                    await asyncio.sleep(0)  # Start processing token transfer saves to clear memory
                    token_transfers.clear()

            if log_batch:  # Gather any remaining log batch entries
                await asyncio.gather(*log_batch)

            if token_transfers:  # Write any remaining token transfers
                await self._wait_for_ready_to_add_batch_task(token_xfr_batch_write_tasks)
                token_xfr_batch_write_tasks.append(
                    asyncio.create_task(
                        self.__data_service.write_token_transfer_batch(token_transfers)
                    )
                )

            cleaned_token_owners = []
            for _token_owners in token_owners.values():
                for token_owner in _token_owners.values():
                    if token_owner.quantity != 0:
                        cleaned_token_owners.append(token_owner)

            write_token_owners_task = asyncio.create_task(
                self._process_batches(
                    cleaned_token_owners, self.__data_service.write_token_owner_batch
                )
            )

            tokens_list = [token for _, token in tokens.items()]
            write_tokens_task = asyncio.create_task(
                self._process_batches(tokens_list, self.__data_service.write_token_batch)
            )

            await asyncio.gather(
                *token_xfr_batch_write_tasks, write_token_owners_task, write_tokens_task
            )

        except Exception as e:
            raise ConsumerError(
                f"Error processing {data_package.collection.specification} "
                f"collection {data_package.collection.collection_id} "
                f"created in block {data_package.collection.block_created} "
                f"-- {e}"
            )

    async def __process_log_entry(
        self,
        data_package: CollectionDataPackage,
        log: EvmLog,
        token_owners: Dict[HexInt, Dict[Address, TokenOwner]],
        token_transfers: List[TokenTransfer],
        tokens: Dict[HexInt, Token],
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
            token_owners[token_id] = {}

        if transaction_type in (TokenTransactionType.MINT, TokenTransactionType.TRANSFER):
            __add_token_quantity_to_owner(
                token_owners_=token_owners,
                collection_=collection,
                quantity_=quantity,
                address=to_address,
                token_id_=token_id,
            )
        if transaction_type in (TokenTransactionType.BURN, TokenTransactionType.TRANSFER):
            zero = CollectionToEverythingElseErc1155CollectionBasedConsumer.HEX_INT_ZERO
            __add_token_quantity_to_owner(
                token_owners_=token_owners,
                collection_=collection,
                quantity_=zero - quantity,
                address=from_address,
                token_id_=token_id,
            )
