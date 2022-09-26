import asyncio
import re
from abc import ABC
from typing import List, Tuple, Optional, cast

from eth_abi import decode
from hexbytes import HexBytes

from blockrail.blockcrawler.core.bus import (
    Transformer,
    DataPackage,
    DataBus,
)
from blockrail.blockcrawler.core.entities import BlockChain, HexInt
from blockrail.blockcrawler.core.rpc import RPCServerError
from blockrail.blockcrawler.evm.data_packages import (
    EvmTransactionReceiptDataPackage,
    EvmLogDataPackage,
)
from blockrail.blockcrawler.evm.rpc import EVMRPCClient, EthCall
from blockrail.blockcrawler.evm.types import ERC165InterfaceID, Address, EVMLog
from blockrail.blockcrawler.evm.util import (
    ERC165Functions,
    ERC721MetadataFunctions,
    ERC721EnumerableFunctions,
    AdditionalFunctions,
    ERC721Events,
    ERC1155Events,
)
from blockrail.blockcrawler.nft.data_packages import (
    NftCollectionDataPackage,
    NftTokenTransferDataPackage,
    NftTokenMetadataUriUpdatedDataPackage,
)
from blockrail.blockcrawler.nft.entities import (
    EthereumCollectionType,
    Collection,
    TokenTransactionType,
    TokenTransfer,
)
from blockrail.blockcrawler.nft.evm import LogVersionOracle


class EvmTransactionReceiptToNftCollectionTransformer(Transformer):
    def __init__(
        self, data_bus: DataBus, blockchain: BlockChain, rpc_client: EVMRPCClient, data_version: int
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

        creator = data_package.transaction_receipt.from_
        (
            supported_interfaces,
            name,
            symbol,
            total_supply,
            owner,
        ) = await self.__get_contract_data(contract_id)

        if ERC165InterfaceID.ERC721 in supported_interfaces:
            specification = EthereumCollectionType.ERC721
        elif ERC165InterfaceID.ERC1155 in supported_interfaces:
            specification = EthereumCollectionType.ERC1155
        else:
            return

        collection = Collection(
            blockchain=self.__blockchain,
            collection_id=contract_id,
            creator=creator,
            owner=Address(owner) if owner is not None else None,
            block_created=data_package.block.number,
            name=name,
            symbol=symbol,
            specification=specification,
            date_created=data_package.block.timestamp,
            total_supply=HexInt(hex(total_supply)) if total_supply is not None else None,
            data_version=self.__data_version,
        )

        await self._get_data_bus().send(NftCollectionDataPackage(collection))

    async def __get_contract_data(
        self, contract_address
    ) -> Tuple[List[ERC165InterfaceID], Optional[str], Optional[str], Optional[int], Optional[str]]:
        supports_erc721_interface_coro = self.__rpc_client.call(
            EthCall(
                None,
                contract_address,
                ERC165Functions.SUPPORTS_INTERFACE,
                [ERC165InterfaceID.ERC721.bytes],
            ),
        )
        supports_erc721_metadata_interface_coro = self.__rpc_client.call(
            EthCall(
                None,
                contract_address,
                ERC165Functions.SUPPORTS_INTERFACE,
                [ERC165InterfaceID.ERC721_METADATA.bytes],
            ),
        )
        supports_erc721_enumerable_interface_coro = self.__rpc_client.call(
            EthCall(
                None,
                contract_address,
                ERC165Functions.SUPPORTS_INTERFACE,
                [ERC165InterfaceID.ERC721_ENUMERABLE.bytes],
            ),
        )
        supports_erc1155_interface_coro = self.__rpc_client.call(
            EthCall(
                None,
                contract_address,
                ERC165Functions.SUPPORTS_INTERFACE,
                [ERC165InterfaceID.ERC1155.bytes],
            ),
        )
        supports_erc1155_metadata_uri_interface_coro = self.__rpc_client.call(
            EthCall(
                None,
                contract_address,
                ERC165Functions.SUPPORTS_INTERFACE,
                [ERC165InterfaceID.ERC1155_METADATA_URI.bytes],
            ),
        )
        symbol_coro = self.__rpc_client.call(
            EthCall(
                None,
                contract_address,
                ERC721MetadataFunctions.SYMBOL,
            ),
        )
        name_coro = self.__rpc_client.call(
            EthCall(
                None,
                contract_address,
                ERC721MetadataFunctions.NAME,
            ),
        )
        total_supply_coro = self.__rpc_client.call(
            EthCall(
                None,
                contract_address,
                ERC721EnumerableFunctions.TOTAL_SUPPLY,
            ),
        )
        owner_coro = self.__rpc_client.call(
            EthCall(
                None,
                contract_address,
                AdditionalFunctions.OWNER,
            ),
        )
        results = await asyncio.gather(
            supports_erc721_interface_coro,
            supports_erc721_metadata_interface_coro,
            supports_erc721_enumerable_interface_coro,
            supports_erc1155_interface_coro,
            supports_erc1155_metadata_uri_interface_coro,
            symbol_coro,
            name_coro,
            total_supply_coro,
            owner_coro,
            return_exceptions=True,
        )

        sanitized_results = list()
        for result in results:
            if isinstance(result, RPCServerError):
                sanitized_result = None
            elif isinstance(result, Exception):
                raise result
            else:
                (sanitized_result,) = result
            sanitized_results.append(sanitized_result)

        (
            supports_erc721_interface,
            supports_erc721_metadata_interface,
            supports_erc721_enumerable_interface,
            supports_erc1155_interface,
            supports_erc1155_metadata_uri_interface,
            symbol,
            name,
            total_supply,
            owner,
        ) = sanitized_results
        supports_interfaces = list()
        if supports_erc721_interface:
            supports_interfaces.append(ERC165InterfaceID.ERC721)
        if supports_erc721_metadata_interface:
            supports_interfaces.append(ERC165InterfaceID.ERC721_METADATA)
        if supports_erc721_enumerable_interface:
            supports_interfaces.append(ERC165InterfaceID.ERC721_ENUMERABLE)
        if supports_erc1155_interface:
            supports_interfaces.append(ERC165InterfaceID.ERC1155)
        if supports_erc1155_metadata_uri_interface:
            supports_interfaces.append(ERC165InterfaceID.ERC1155_METADATA_URI)

        return (
            supports_interfaces,
            cast(str, name),
            cast(str, symbol),
            cast(int, total_supply),
            cast(str, owner),
        )


class TokenTransactionTypeOracle:
    ZERO_ADDRESS = HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000")

    def derive_type(self, log: EVMLog) -> TokenTransactionType:
        from_address = log.topics[1]
        to_address = log.topics[2]
        contract_address = HexBytes("0x000000000000000000000000") + HexBytes(str(log.address))
        if to_address == self.ZERO_ADDRESS:
            transaction_type = TokenTransactionType.BURN
        elif from_address in (self.ZERO_ADDRESS, contract_address) and to_address not in (
            self.ZERO_ADDRESS,
            contract_address,
        ):
            transaction_type = TokenTransactionType.MINT
        else:
            transaction_type = TokenTransactionType.TRANSFER
        return transaction_type


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

        from_address, to_address, token_ids, quantities = self._parse_transfer_data(data_package)

        transaction_type = self._transaction_type_oracle.derive_type(data_package.log)

        attribute_version = self.__version_oracle.version_from_log(data_package.log)
        for token_id, quantity in zip(token_ids, quantities):
            if data_package.log.address is None:
                raise ValueError("TokenTransfer requires Log->address but was None")
            transfer_data_package = NftTokenTransferDataPackage(
                token_transfer=TokenTransfer(
                    data_version=self._data_version,
                    blockchain=data_package.blockchain,
                    timestamp=data_package.block.timestamp,
                    collection_id=data_package.log.address,
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
            and data_package.log.topics[0] == ERC721Events.TRANSFER.event_signature_hash
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
        return from_address, to_address, [token_id], [1]


class EvmLogErc1155TransferSingleToNftTokenTransferTransformer(
    EvmLogErcTransferToNftTokenTransferTransformerBase
):
    def _is_processable(self, data_package: EvmLogDataPackage):
        return (
            data_package.log.topics
            and data_package.log.topics[0] == ERC1155Events.TRANSFER_SINGLE.event_signature_hash
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

        return from_address, to_address, [token_id], [quantity]


class EvmLogErc1155TransferToNftTokenTransferTransformer(
    EvmLogErcTransferToNftTokenTransferTransformerBase
):
    def _is_processable(self, data_package: EvmLogDataPackage):
        return (
            data_package.log.topics
            and data_package.log.topics[0] == ERC1155Events.TRANSFER_BATCH.event_signature_hash
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

        return from_address, to_address, token_ids, quantities


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
            and data_package.log.topics[0] == ERC1155Events.URI.event_signature_hash
        ):
            return

        if data_package.log.address is None:
            raise ValueError("URI data package requires Log->address but was None")

        token_id = HexInt(decode(["uint256"], data_package.log.topics[1])[0])

        metadata_uri = decode(["string"], data_package.log.data)[0]
        metadata_uri = metadata_uri.replace("{id}", str(token_id.int_value))
        metadata_uri_version = self._log_version_oracle.version_from_log(data_package.log)
        uri_update_data_package = NftTokenMetadataUriUpdatedDataPackage(
            blockchain=data_package.blockchain,
            collection_id=data_package.log.address,
            token_id=token_id,
            metadata_uri=metadata_uri,
            metadata_uri_version=metadata_uri_version,
            data_version=self.__data_version,
        )

        await self._get_data_bus().send(uri_update_data_package)


class EvmLogErc721TransferToNftTokenMetadataUriUpdatedTransformer(Transformer):
    def __init__(
        self,
        data_bus: DataBus,
        rpc_client: EVMRPCClient,
        log_version_oracle: LogVersionOracle,
        data_version: int,
    ) -> None:
        super().__init__(data_bus)
        self.__rpc_client = rpc_client
        self._log_version_oracle = log_version_oracle
        self.__data_version = data_version

    async def receive(self, data_package: DataPackage):
        if not isinstance(data_package, EvmLogDataPackage):
            return

        if not (
            data_package.log.topics
            and data_package.log.topics[0] == ERC721Events.TRANSFER.event_signature_hash
        ):
            return

        if len(data_package.log.topics) != 4:  # ERC-20 Transfer has the same sig hash
            return

        if data_package.log.address is None:
            raise ValueError("URI data package requires Log->address but was None")

        token_id = HexInt(decode(["uint256"], data_package.log.topics[3])[0])

        try:
            (metadata_uri,) = await self.__rpc_client.call(
                EthCall(
                    from_=None,
                    to=data_package.log.address,
                    function=ERC721MetadataFunctions.TOKEN_URI,
                    parameters=[token_id.int_value],
                )
            )
        except RPCServerError:
            return

        metadata_uri_version = self._log_version_oracle.version_from_log(data_package.log)
        uri_update_data_package = NftTokenMetadataUriUpdatedDataPackage(
            blockchain=data_package.blockchain,
            collection_id=data_package.log.address,
            token_id=token_id,
            metadata_uri=metadata_uri,
            metadata_uri_version=metadata_uri_version,
            data_version=self.__data_version,
        )

        await self._get_data_bus().send(uri_update_data_package)
