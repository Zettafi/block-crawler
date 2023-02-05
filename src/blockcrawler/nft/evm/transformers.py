import asyncio
import re
from abc import ABC
from typing import Tuple, Optional

from eth_abi import decode

from blockcrawler.core.bus import (
    Transformer,
    DataPackage,
    DataBus,
    ConsumerError,
)
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.rpc import RpcServerError, RpcDecodeError
from blockcrawler.evm.data_packages import (
    EvmTransactionReceiptDataPackage,
    EvmLogDataPackage,
)
from blockcrawler.evm.rpc import EvmRpcClient, EthCall
from blockcrawler.evm.types import (
    Erc165InterfaceID,
    Erc165Functions,
    Erc721MetadataFunctions,
    Erc721EnumerableFunctions,
    AdditionalFunctions,
    Erc721Events,
    Erc1155Events,
)
from blockcrawler.core.types import Address, HexInt
from blockcrawler.nft.data_packages import (
    CollectionDataPackage,
    TokenTransferDataPackage,
    TokenMetadataUriUpdatedDataPackage,
)
from blockcrawler.nft.entities import (
    EthereumCollectionType,
    Collection,
    TokenTransfer,
    TokenTransactionType,
)
from blockcrawler.nft.evm.oracles import LogVersionOracle, TokenTransactionTypeOracle


class EvmTransactionReceiptToNftCollectionTransformer(Transformer):
    def __init__(
        self, data_bus: DataBus, blockchain: BlockChain, rpc_client: EvmRpcClient, data_version: int
    ) -> None:
        super().__init__(data_bus)
        self.__blockchain = blockchain
        self.__rpc_client = rpc_client
        self.__data_version = data_version

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, EvmTransactionReceiptDataPackage):
            return

        contract_id = data_package.transaction_receipt.contract_address
        if contract_id is None:
            return

        try:
            supports_erc721_interface_coro = self.__rpc_client.call(
                EthCall(
                    None,
                    contract_id,
                    Erc165Functions.SUPPORTS_INTERFACE,
                    [Erc165InterfaceID.ERC721.bytes],
                ),
            )
            supports_erc1155_interface_coro = self.__rpc_client.call(
                EthCall(
                    None,
                    contract_id,
                    Erc165Functions.SUPPORTS_INTERFACE,
                    [Erc165InterfaceID.ERC1155.bytes],
                ),
            )
            supports_erc721_result, supports_erc1155_result = await asyncio.gather(
                supports_erc721_interface_coro,
                supports_erc1155_interface_coro,
                return_exceptions=True,
            )

            if isinstance(supports_erc721_result, (RpcServerError, RpcDecodeError)):
                """
                RpcServerError occurs when the function is not part the contract.
                RpcDecodeError occurs when an unexpected response is returned. This
                can be a result of a hash collision or incorrect implementation. In
                either case, it's not unexpected as we perform discovery on contracts.
                """
                supports_erc721 = False
            elif isinstance(supports_erc721_result, Exception):
                raise supports_erc721_result
            else:
                (supports_erc721,) = supports_erc721_result

            if isinstance(supports_erc1155_result, (RpcServerError, RpcDecodeError)):
                # See erc721 check for details
                supports_erc1155 = False
            elif isinstance(supports_erc1155_result, Exception):
                raise supports_erc1155_result
            else:
                (supports_erc1155,) = supports_erc1155_result

            if supports_erc721:
                specification = EthereumCollectionType.ERC721
            elif supports_erc1155:
                specification = EthereumCollectionType.ERC1155
            else:
                return

            creator = data_package.transaction_receipt.from_

            (symbol, name, total_supply, owner) = await self.__get_contract_metadata(
                contract_id, supports_erc721
            )

            collection = Collection(
                blockchain=self.__blockchain,
                collection_id=contract_id,
                creator=creator,
                owner=owner if owner is None else Address(owner),
                block_created=data_package.block.number,
                name=name,
                symbol=symbol,
                specification=specification,
                date_created=data_package.block.timestamp,
                total_supply=HexInt(hex(total_supply)) if total_supply is not None else None,
                data_version=self.__data_version,
            )
        except Exception as e:
            raise ConsumerError(
                f"Unable to create collection from contract {contract_id} " f"due to error -- {e}"
            )

        await self._get_data_bus().send(CollectionDataPackage(collection))

    async def __get_contract_metadata(
        self, contract_address, is_erc721
    ) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[str]]:
        owner_coro = self.__rpc_client.call(
            EthCall(
                None,
                contract_address,
                AdditionalFunctions.OWNER,
            )
        )

        if is_erc721:
            symbol_coro = self.__rpc_client.call(
                EthCall(
                    None,
                    contract_address,
                    Erc721MetadataFunctions.SYMBOL,
                ),
            )
            name_coro = self.__rpc_client.call(
                EthCall(
                    None,
                    contract_address,
                    Erc721MetadataFunctions.NAME,
                ),
            )
            total_supply_coro = self.__rpc_client.call(
                EthCall(
                    None,
                    contract_address,
                    Erc721EnumerableFunctions.TOTAL_SUPPLY,
                ),
            )
            symbol_result, name_result, total_supply_result, owner_result = await asyncio.gather(
                symbol_coro, name_coro, total_supply_coro, owner_coro, return_exceptions=True
            )
            for result in symbol_result, name_result, total_supply_result, owner_result:
                if isinstance(result, Exception) and not isinstance(
                    result, (RpcServerError, RpcDecodeError)
                ):
                    raise result
        else:
            try:
                owner_result = await owner_coro
            except (RpcServerError, RpcDecodeError):
                owner_result = (None,)
            finally:
                symbol_result, name_result, total_supply_result = (None,), (None,), (None,)

        return (
            None if isinstance(symbol_result, Exception) else symbol_result[0],
            None if isinstance(name_result, Exception) else name_result[0],
            None if isinstance(total_supply_result, Exception) else total_supply_result[0],
            None if isinstance(owner_result, Exception) else owner_result[0],
        )


class EvmLogErcTransferToNftTokenTransferTransformerBase(Transformer, ABC):
    def __init__(
        self,
        data_bus: DataBus,
        data_version: int,
        transaction_type_oracle: TokenTransactionTypeOracle,
        version_oracle: LogVersionOracle,
    ) -> None:
        super().__init__(data_bus)
        self._data_version = data_version
        self._transaction_type_oracle = transaction_type_oracle
        self.__version_oracle = version_oracle

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, EvmLogDataPackage):
            return

        if not self._is_processable(data_package):
            return

        (
            collection_type,
            from_address,
            to_address,
            token_ids,
            quantities,
        ) = self._parse_transfer_data(data_package)

        transaction_type = self._transaction_type_oracle.type_from_log(data_package.log)

        attribute_version = self.__version_oracle.version_from_log(data_package.log)
        for token_id, quantity in zip(token_ids, quantities):
            if data_package.log.address is None:
                raise ValueError("TokenTransfer requires Log->address but was None")
            transfer_data_package = TokenTransferDataPackage(
                token_transfer=TokenTransfer(
                    data_version=self._data_version,
                    blockchain=data_package.blockchain,
                    timestamp=data_package.block.timestamp,
                    collection_id=data_package.log.address,
                    collection_type=collection_type,
                    from_=Address(from_address),
                    to_=Address(to_address),
                    token_id=HexInt(token_id),
                    transaction_type=transaction_type,
                    quantity=HexInt(quantity),
                    block_id=data_package.log.block_number,
                    transaction_hash=data_package.log.transaction_hash,
                    transaction_index=data_package.log.transaction_index,
                    log_index=data_package.log.log_index,
                    attribute_version=attribute_version,
                ),
            )
            await self._get_data_bus().send(transfer_data_package)

    def _is_processable(self, data_package: EvmLogDataPackage):
        raise NotImplementedError

    def _parse_transfer_data(self, data_package):
        raise NotImplementedError


class EvmLogErc721TransferToNftTokenTransferTransformer(
    EvmLogErcTransferToNftTokenTransferTransformerBase
):
    def _is_processable(self, data_package: EvmLogDataPackage):
        return (
            len(data_package.log.topics) == 4
            and data_package.log.topics[0] == Erc721Events.TRANSFER.event_signature_hash
        )

    def _parse_transfer_data(self, data_package):
        from_address = decode(
            ["address"],
            data_package.log.topics[1],
        )[0]
        to_address = decode(
            ["address"],
            data_package.log.topics[2],
        )[0]
        token_id = decode(
            ["uint256"],
            data_package.log.topics[3],
        )[0]
        return EthereumCollectionType.ERC721, from_address, to_address, [token_id], [1]


class EvmLogErc1155TransferSingleToNftTokenTransferTransformer(
    EvmLogErcTransferToNftTokenTransferTransformerBase
):
    def _is_processable(self, data_package: EvmLogDataPackage):
        return (
            data_package.log.topics
            and data_package.log.topics[0] == Erc1155Events.TRANSFER_SINGLE.event_signature_hash
        )

    def _parse_transfer_data(self, data_package):
        from_address = decode(
            ["address"],
            data_package.log.topics[1],
        )[0]
        to_address = decode(
            ["address"],
            data_package.log.topics[2],
        )[0]
        token_id, quantity = decode(
            ["uint256", "uint256"],
            data_package.log.data,
        )

        return EthereumCollectionType.ERC1155, from_address, to_address, [token_id], [quantity]


class EvmLogErc1155TransferToNftTokenTransferTransformer(
    EvmLogErcTransferToNftTokenTransferTransformerBase
):
    def _is_processable(self, data_package: EvmLogDataPackage):
        return (
            data_package.log.topics
            and data_package.log.topics[0] == Erc1155Events.TRANSFER_BATCH.event_signature_hash
        )

    def _parse_transfer_data(self, data_package):
        from_address = decode(
            ["address"],
            data_package.log.topics[1],
        )[0]
        to_address = decode(
            ["address"],
            data_package.log.topics[2],
        )[0]
        token_ids, quantities = decode(
            ["uint256[]", "uint256[]"],
            data_package.log.data,
        )

        return EthereumCollectionType.ERC1155, from_address, to_address, token_ids, quantities


class EvmLogErc1155UriEventToNftTokenMetadataUriUpdatedTransformer(Transformer):
    URI_ID_SUBSTITUTION_REGEX = re.compile(r"\{id}")

    def __init__(
        self, data_bus: DataBus, log_version_oracle: LogVersionOracle, data_version: int
    ) -> None:
        super().__init__(data_bus)
        self._log_version_oracle = log_version_oracle
        self.__data_version = data_version

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, EvmLogDataPackage):
            return

        if not (
            data_package.log.topics
            and data_package.log.topics[0] == Erc1155Events.URI.event_signature_hash
        ):
            return

        if data_package.log.address is None:
            raise ValueError("URI data package requires Log->address but was None")

        token_id = HexInt(decode(["uint256"], data_package.log.topics[1])[0])

        metadata_uri = decode(["string"], data_package.log.data)[0]
        metadata_uri = metadata_uri.replace("{id}", str(token_id.int_value))
        metadata_uri_version = self._log_version_oracle.version_from_log(data_package.log)
        uri_update_data_package = TokenMetadataUriUpdatedDataPackage(
            blockchain=data_package.blockchain,
            collection_id=data_package.log.address,
            token_id=token_id,
            metadata_uri=metadata_uri,
            metadata_uri_version=metadata_uri_version,
            data_version=self.__data_version,
        )

        await self._get_data_bus().send(uri_update_data_package)


class Erc721TokenTransferToNftTokenMetadataUriUpdatedTransformer(Transformer):
    def __init__(
        self,
        data_bus: DataBus,
        rpc_client: EvmRpcClient,
    ) -> None:
        super().__init__(data_bus)
        self.__rpc_client = rpc_client

    async def receive(self, data_package: DataPackage):
        if (
            not isinstance(data_package, TokenTransferDataPackage)
            or data_package.token_transfer.collection_type != EthereumCollectionType.ERC721
            or data_package.token_transfer.transaction_type != TokenTransactionType.MINT
        ):
            return

        try:
            (metadata_uri,) = await self.__rpc_client.call(
                EthCall(
                    from_=None,
                    to=data_package.token_transfer.collection_id,
                    function=Erc721MetadataFunctions.TOKEN_URI,
                    parameters=[data_package.token_transfer.token_id.int_value],
                    block=data_package.token_transfer.block_id,
                )
            )
        except Exception as e:
            if isinstance(e, RpcServerError) and e.error_code == -32000:
                # -32000 is the error when contract does not have function. It's expected sometimes.
                return

            raise ConsumerError(
                f"Unable to retrieve metadata URI for Token ID "
                f"{data_package.token_transfer.token_id.int_value} in "
                f"Collection {data_package.token_transfer.collection_id}"
                f" -- {e}"
            )

        uri_update_data_package = TokenMetadataUriUpdatedDataPackage(
            blockchain=data_package.token_transfer.blockchain,
            collection_id=data_package.token_transfer.collection_id,
            token_id=data_package.token_transfer.token_id,
            metadata_uri=metadata_uri,
            metadata_uri_version=data_package.token_transfer.attribute_version,
            data_version=data_package.token_transfer.data_version,
        )

        await self._get_data_bus().send(uri_update_data_package)
