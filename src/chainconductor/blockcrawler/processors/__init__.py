import asyncio
import decimal
import json
import re
from logging import Logger
from typing import List, Callable, Tuple, Coroutine, Any, Dict
from typing import Optional

from botocore.exceptions import ClientError
from eth_abi import decode
from eth_bloom import BloomFilter
from eth_hash.auto import keccak
from eth_utils import decode_hex

from chainconductor.blockcrawler.data_clients import (
    IpfsDataClient,
    HttpDataClient,
    ArweaveDataClient,
    ProtocolTimeoutError,
    DataClient,
)
from chainconductor.blockcrawler.data_clients import ProtocolError
from chainconductor.blockcrawler.stats import StatsService
from chainconductor.data.models import Collections, TokenTransfers, Tokens
from chainconductor.web3.rpc import RPCClient, RPCServerError, EthCall, RPCError
from chainconductor.web3.types import (
    Contract,
    ERC165InterfaceID,
    HexInt,
)
from chainconductor.web3.types import (
    TransactionReceipt,
    Block,
)
from chainconductor.web3.util import (
    ERC165Functions,
    ERC721MetadataFunctions,
    ERC721EnumerableFunctions,
    ERC721Events,
    ERC1155MetadataURIFunctions,
)


class ProcessorException(Exception):  # pragma: no cover
    pass


class TransportObject:  # pragma: no cover
    pass


class ContractTransportObject(TransportObject):  # pragma: no cover
    def __init__(
        self,
        *,
        block: Optional[Block] = None,
        transaction_receipt: Optional[TransactionReceipt] = None,
        contract: Optional[Contract] = None,
    ) -> None:
        self.__block: Optional[Block] = block
        self.__transaction_receipt: Optional[TransactionReceipt] = transaction_receipt
        self.__contract: Optional[Contract] = contract

    def __repr__(self) -> str:
        return (
            self.__class__.__name__
            + {
                "block": self.__block,
                "transaction_receipt": self.__transaction_receipt,
                "contract": self.__contract,
            }.__repr__()
        )

    def __eq__(self, o: object) -> bool:
        return (
            isinstance(o, self.__class__)
            and self.block == o.block
            and self.transaction_receipt == o.transaction_receipt
            and self.contract == o.contract
        )

    @property
    def block(self) -> Optional[Block]:
        return self.__block

    @property
    def transaction_receipt(self) -> Optional[TransactionReceipt]:
        return self.__transaction_receipt

    @property
    def contract(self) -> Optional[Contract]:
        return self.__contract


class Token:  # pragma: no cover
    def __init__(
        self,
        *,
        collection_id: str,
        original_owner: str,
        token_id: HexInt,
        timestamp: HexInt,
        metadata_uri: str = None,
        metadata: str = None,
        metadata_content_type: str = None,
    ) -> None:
        self.__collection_id = collection_id
        self.__original_owner = original_owner
        self.__token_id = token_id
        self.__timestamp = timestamp
        self.__metadata_uri = metadata_uri
        self.__metadata = metadata
        self.__metadata_content_type = metadata_content_type

    @property
    def collection_id(self) -> str:
        return self.__collection_id

    @property
    def original_owner(self) -> str:
        return self.__original_owner

    @property
    def token_id(self) -> HexInt:
        return self.__token_id

    @property
    def timestamp(self) -> HexInt:
        return self.__timestamp

    @property
    def metadata_uri(self):
        return self.__metadata_uri

    @property
    def metadata(self):
        return self.__metadata

    @property
    def metadata_content_type(self):
        return self.__metadata_content_type

    def __repr__(self):
        return (
            self.__class__.__name__
            + {
                "collection_id": self.collection_id,
                "token_id": self.token_id,
                "original_owner": self.__original_owner,
                "timestamp": self.timestamp,
                "metadata_uri": self.metadata_uri,
                "metadata": self.__metadata,
                "metadata_content_type": self.__metadata_content_type,
            }.__repr__()
        )

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.collection_id == other.collection_id
            and self.token_id == other.token_id
            and self.original_owner == other.original_owner
            and self.timestamp == other.timestamp
            and self.metadata_uri == other.metadata_uri
            and self.metadata == other.metadata
            and self.metadata_content_type == other.metadata_content_type
        )


class TokenTransportObject(TransportObject):  # pragma: no cover
    def __init__(
        self,
        *,
        block: Optional[Block] = None,
        transaction_receipt: Optional[TransactionReceipt] = None,
        token: Optional[Token] = None,
    ) -> None:
        self.__block: Optional[Block] = block
        self.__transaction_receipt: Optional[TransactionReceipt] = transaction_receipt
        self.__token: Optional[Token] = token

    def __repr__(self):
        return (
            str(self.__class__.__name__)
            + {
                "block": self.__block,
                "transaction_receipt": self.__transaction_receipt,
                "token": self.__token,
            }.__repr__()
        )

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.block == other.block
            and self.transaction_receipt == other.transaction_receipt
            and self.token == other.token
        )

    @property
    def block(self) -> Optional[Block]:
        return self.__block

    @property
    def transaction_receipt(self) -> Optional[TransactionReceipt]:
        return self.__transaction_receipt

    @property
    def token(self) -> Optional[Token]:
        return self.__token


class BlockBatchProcessor:
    PROCESSED_STAT = "blocks_processed"
    RPC_TIMER_GET_BLOCKS = "rpc_get_blocks"
    TRANSFER_EVENT_HASH = decode_hex(ERC721Events.TRANSFER.event_hash)

    def __init__(
        self,
        rpc_client: RPCClient,
        stats_service: StatsService,
    ) -> None:
        self.__rpc_client = rpc_client
        self.__stats_service = stats_service

    async def __call__(self, batch: List[int]) -> List[Tuple[str, Block]]:
        with self.__stats_service.timer(self.RPC_TIMER_GET_BLOCKS):
            blocks = await self.__rpc_client.get_blocks(set(batch))
        results = list()
        for block in blocks:
            if isinstance(block, RPCError):
                raise block
            for transaction in block.transactions:
                results.append((transaction, block))
            self.__stats_service.increment(self.PROCESSED_STAT)
        return results


class TransactionBatchProcessor:
    PROCESSED_STAT = "transactions_processed"
    RPC_TIMER_GET_TRANSACTION_RECEIPTS = "rpc_get_transaction_receipts"
    TRANSFER_EVENT_HASH = decode_hex(ERC721Events.TRANSFER.event_hash)

    def __init__(
        self,
        rpc_client: RPCClient,
        stats_service: StatsService,
    ) -> None:
        self.__rpc_client: RPCClient = rpc_client
        self.__stats_service: StatsService = stats_service

    async def __call__(
        self,
        transport_objects: List[Tuple[str, Block]],
    ) -> List[TransportObject]:
        results: List[TransportObject] = list()
        transaction_hashes: List[str] = list()
        transactions_hash_map: Dict[str, Block] = dict()

        for transaction_hash, block in transport_objects:
            transactions_hash_map[transaction_hash] = block
            transaction_hashes.append(transaction_hash)

        with self.__stats_service.timer(self.RPC_TIMER_GET_TRANSACTION_RECEIPTS):
            receipts = await self.__rpc_client.get_transaction_receipts(transaction_hashes)

        for receipt in receipts:
            if isinstance(receipt, RPCError):
                raise receipt
            block = transactions_hash_map[receipt.transaction_hash]
            out_transport_object: Optional[TransportObject] = None
            if receipt.to_ is None:
                # No to address is indicative of contract creation
                # If there is no to address,
                # create the Contract transport object and send it on to be processed
                # by the transaction processor
                out_transport_object = ContractTransportObject(
                    block=block, transaction_receipt=receipt
                )
            elif self.TRANSFER_EVENT_HASH in BloomFilter(receipt.logs_bloom.int_value):
                # If the event hash is in the block's bloom filter, the block may
                # have a transaction log with the event
                out_transport_object = TokenTransportObject(
                    block=block, transaction_receipt=receipt
                )

            if out_transport_object is not None:
                results.append(out_transport_object)

            self.__stats_service.increment(self.PROCESSED_STAT)
        return results


class ContractBatchProcessor:
    RPC_TIMER_CALL_CONTRACT_INFO = "rpc_timer_call_supports_interfaces"
    PROCESSED_STAT = "contracts_processed"

    def __init__(
        self,
        rpc_client: RPCClient,
        stats_service: StatsService,
    ) -> None:
        self.__rpc_client: RPCClient = rpc_client
        self.__stats_service: StatsService = stats_service

    async def __call__(
        self, transport_objects: List[ContractTransportObject]
    ) -> List[ContractTransportObject]:
        results = list()
        for in_transport_object in transport_objects:
            if in_transport_object.transaction_receipt is None:
                raise ValueError(
                    f"TokenTransportObject provided "
                    f"has no transaction_receipt: {in_transport_object}"
                )

            contract_id = in_transport_object.transaction_receipt.contract_address
            supported_interfaces, name, symbol, total_supply = await self.__get_contract_data(
                contract_id
            )
            if (
                ERC165InterfaceID.ERC721 in supported_interfaces
                and in_transport_object.transaction_receipt.contract_address is not None
            ):
                contract = Contract(
                    address=in_transport_object.transaction_receipt.contract_address,
                    creator=in_transport_object.transaction_receipt.from_,
                    interfaces=supported_interfaces,
                    name=name,
                    symbol=symbol,
                    total_supply=total_supply,
                )

                out_transport_object = ContractTransportObject(
                    block=in_transport_object.block,
                    transaction_receipt=in_transport_object.transaction_receipt,
                    contract=contract,
                )
                results.append(out_transport_object)
            self.__stats_service.increment(self.PROCESSED_STAT)
        return results

    async def __get_contract_data(
        self, contract_address
    ) -> Tuple[List[ERC165InterfaceID], str, str, int]:
        with self.__stats_service.timer(self.RPC_TIMER_CALL_CONTRACT_INFO):
            responses = await self.__rpc_client.calls(
                [
                    EthCall(
                        ERC165InterfaceID.ERC721.value,
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC721.bytes],
                    ),
                    EthCall(
                        ERC165InterfaceID.ERC721_METADATA.value,
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC721_METADATA.bytes],
                    ),
                    EthCall(
                        ERC165InterfaceID.ERC721_ENUMERABLE.value,
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC721_ENUMERABLE.bytes],
                    ),
                    EthCall(
                        ERC165InterfaceID.ERC1155.value,
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC1155.bytes],
                    ),
                    EthCall(
                        ERC165InterfaceID.ERC1155_METADATA_URI.value,
                        None,
                        contract_address,
                        ERC165Functions.SUPPORTS_INTERFACE,
                        [ERC165InterfaceID.ERC1155_METADATA_URI.bytes],
                    ),
                    EthCall(
                        "symbol",
                        None,
                        contract_address,
                        ERC721MetadataFunctions.SYMBOL,
                    ),
                    EthCall(
                        "name",
                        None,
                        contract_address,
                        ERC721MetadataFunctions.NAME,
                    ),
                    EthCall(
                        "total_supply",
                        None,
                        contract_address,
                        ERC721EnumerableFunctions.TOTAL_SUPPLY,
                    ),
                ]
            )
        supports_interfaces = [
            ERC165InterfaceID.from_value(key)
            for key, value in responses.items()
            if value is not None and not isinstance(value, RPCServerError) and value[0] is True
        ]

        if "name" in responses:
            name = (
                responses["name"][0]
                if responses["name"] is not None
                and not isinstance(responses["name"], RPCServerError)
                else None
            )
        else:
            name = None

        if "symbol" in responses:
            symbol = (
                responses["symbol"][0]
                if responses["symbol"] is not None
                and not isinstance(responses["symbol"], RPCServerError)
                else None
            )
        else:
            symbol = None

        if "total_supply" in responses:
            total_supply = (
                responses["total_supply"][0]
                if responses["total_supply"] is not None
                and not isinstance(responses["total_supply"], RPCServerError)
                else None
            )
        else:
            total_supply = None

        return supports_interfaces, name, symbol, total_supply


class CollectionPersistenceBatchProcessor:
    PROCESSED_STAT = "collection_persisted"
    DYNAMODB_TIMER_WRITE_CONTRACTS = "dynamodb_write_collections"

    def __init__(
        self, dynamodb, stats_service: StatsService, logger: Logger, blockchain: str
    ) -> None:
        self.__dynamodb = dynamodb
        self.__stats_service: StatsService = stats_service
        self.__logger: Logger = logger
        self.__blockchain = blockchain

    async def __call__(self, transport_objects: List[ContractTransportObject]):
        try:
            collections = await self.__dynamodb.Table(Collections.table_name)
            with self.__stats_service.timer(self.DYNAMODB_TIMER_WRITE_CONTRACTS):
                items = list()
                async with collections.batch_writer() as batch:
                    for transport_object in transport_objects:
                        if transport_object.contract is None:
                            raise ValueError(
                                f"TokenTransportObject provided has no contract: {transport_object}"
                            )
                        if transport_object.block is None:
                            raise ValueError(
                                f"TokenTransportObject provided has no block: {transport_object}"
                            )
                        if transport_object.transaction_receipt is None:
                            raise ValueError(
                                f"TokenTransportObject provided "
                                f"has no transaction_receipt: {transport_object}"
                            )

                        collection_id = transport_object.contract.address
                        block_number = transport_object.transaction_receipt.block_number.int_value
                        transaction_index = (
                            transport_object.transaction_receipt.transaction_index.int_value
                        )
                        transaction_hash = transport_object.transaction_receipt.transaction_hash
                        creator = transport_object.contract.creator
                        timestamp = transport_object.block.timestamp.int_value
                        name = transport_object.contract.name
                        symbol = transport_object.contract.symbol
                        total_supply = (
                            hex(transport_object.contract.total_supply)
                            if transport_object.contract.total_supply
                            else None
                        )
                        interfaces = [i.value for i in transport_object.contract.interfaces]
                        item = {
                            "blockchain": self.__blockchain,
                            "collection_id": collection_id,
                            "block_number": block_number,
                            "transaction_index": transaction_index,
                            "transaction_hash": transaction_hash,
                            "creator": creator,
                            "timestamp": timestamp,
                            "name": name,
                            "symbol": symbol,
                            "total_supply": total_supply,
                            "interfaces": interfaces,
                        }
                        items.append(item)
                        await batch.put_item(Item=item)
                        self.__stats_service.increment(self.PROCESSED_STAT)
        except (ClientError, decimal.Inexact):
            self.__logger.exception(f"Error writing items to Collections: {items}")


class TokenTransferPersistenceBatchProcessor:
    PROCESSED_STAT = "token_transfers_persisted"
    DYNAMODB_TIMER_WRITE_TOKEN_TRANSFERS = "dynamodb_write_token_transfers"
    TOPICS_DECODE_ERRORS = "token_transfers_log_topic_decode_error"

    def __init__(
        self, dynamodb, stats_service: StatsService, logger: Logger, blockchain: str
    ) -> None:
        self.__dynamodb = dynamodb
        self.__stats_service: StatsService = stats_service
        self.__logger = logger
        self.__blockchain = blockchain

    async def __call__(self, transport_objects: List[TokenTransportObject]):
        results = list()
        items = list()
        transfers = await self.__dynamodb.Table(TokenTransfers.table_name)
        try:
            with self.__stats_service.timer(self.DYNAMODB_TIMER_WRITE_TOKEN_TRANSFERS):
                async with transfers.batch_writer() as batch:
                    for transport_object in transport_objects:
                        if transport_object.transaction_receipt is None:
                            raise ValueError(
                                f"TokenTransportObject provided "
                                f"has no transaction_receipt: {transport_object}"
                            )

                        for log in transport_object.transaction_receipt.logs:
                            if (
                                len(log.topics) == 4
                                and log.topics[0] == ERC721Events.TRANSFER.event_hash
                                and log.address is not None
                            ):
                                # ERC-20 transfer events only have 3 topics. ERC-721 transfer events
                                # have 4 topics. Process only logs with 4 topics.
                                try:
                                    from_address: str = decode(
                                        ["address"], decode_hex(log.topics[1])
                                    )[0]
                                    to_address: str = decode(
                                        ["address"], decode_hex(log.topics[2])
                                    )[0]
                                    token_id: int = decode(["uint256"], decode_hex(log.topics[3]))[
                                        0
                                    ]
                                    collection_id: str = log.address
                                except Exception:
                                    # There are occasional bad transactions in the chain,
                                    # it's usually token IDs that are larger than 256 bits
                                    self.__stats_service.increment(self.TOPICS_DECODE_ERRORS)
                                    continue

                                if transport_object.block is None:
                                    raise ValueError(
                                        f"TokenTransportObject provided "
                                        f"has no Block: {transport_object}"
                                    )

                                timestamp = transport_object.block.timestamp
                                transaction_log_index_hash = keccak(
                                    (
                                        log.block_number.hex_value
                                        + log.transaction_index.hex_value
                                        + log.log_index.hex_value
                                    ).encode("utf8")
                                ).hex()
                                item = {
                                    "blockchain": self.__blockchain,
                                    "transaction_log_index_hash": transaction_log_index_hash,
                                    "collection_id": collection_id,
                                    "token_id": str(token_id),
                                    "from": from_address,
                                    "to": to_address,
                                    "block": log.block_number.int_value,
                                    "transaction_index": log.transaction_index.int_value,
                                    "log_index": log.log_index.int_value,
                                    "timestamp": timestamp.int_value,
                                }
                                items.append(item)
                                await batch.put_item(Item=item)
                                if (
                                    # Transferring from the 0 or collection address
                                    (from_address == collection_id or int(from_address, 16) == 0)
                                    # and not to the collection address
                                    and to_address != collection_id
                                    # and not to the 0 address
                                    and int(to_address, 16) != 0
                                    # this is a minting transfer
                                ):
                                    token_mint = Token(
                                        collection_id=collection_id,
                                        original_owner=to_address,
                                        token_id=HexInt(hex(token_id)),
                                        timestamp=timestamp,
                                    )
                                    out_transport_object = TokenTransportObject(
                                        block=transport_object.block,
                                        transaction_receipt=transport_object.transaction_receipt,
                                        token=token_mint,
                                    )
                                    results.append(out_transport_object)

                                self.__stats_service.increment(self.PROCESSED_STAT)
        except (ClientError, decimal.Inexact):
            self.__logger.exception(f"Error writing items to TokenTransfers: {items}")
        return results


class TokenMetadataUriBatchProcessor:
    PROCESSED_STAT = "token_metadata_uris_processed"
    RPC_GET_TOKEN_METADATA_URI = "rpc_call_token_metadata_uri"

    def __init__(
        self,
        rpc_client: RPCClient,
        stats_service: StatsService,
    ) -> None:
        self.__rpc_client = rpc_client
        self.__stats_service = stats_service

    async def __call__(self, transport_objects: List[TokenTransportObject]):
        def _get_721_id_for_token(token):
            return f"{token.collection_id}-{token.token_id}-721"

        def _get_1155_id_for_token(token):
            return f"{token.collection_id}-{token.token_id}-1155"

        results = list()
        requests = list()
        for transport_object in transport_objects:
            if transport_object.token is None:
                raise ValueError(f"TokenTransportObject provided has no Token: {transport_object}")
            token_id = transport_object.token.token_id.int_value
            requests.append(
                EthCall(
                    _get_721_id_for_token(transport_object.token),
                    None,
                    transport_object.token.collection_id,
                    ERC721MetadataFunctions.TOKEN_URI,
                    [token_id],
                )
            )
            requests.append(
                EthCall(
                    _get_1155_id_for_token(transport_object.token),
                    None,
                    transport_object.token.collection_id,
                    ERC1155MetadataURIFunctions.URI,
                    [token_id],
                )
            )

        with self.__stats_service.timer(self.RPC_GET_TOKEN_METADATA_URI):
            responses = await self.__rpc_client.calls(requests)

        for transport_object in transport_objects:
            metadata_uri = None
            for get_id in [_get_1155_id_for_token, _get_721_id_for_token]:
                response = responses[get_id(transport_object.token)]
                if response is not None and not isinstance(response, RPCServerError):
                    metadata_uri = response[0]
                    break

            if transport_object.token is None:
                raise ValueError(f"TokenTransportObject provided has no Token: {transport_object}")

            out_transport_object = TokenTransportObject(
                block=transport_object.block,
                transaction_receipt=transport_object.transaction_receipt,
                token=Token(
                    collection_id=transport_object.token.collection_id,
                    original_owner=transport_object.token.original_owner,
                    token_id=transport_object.token.token_id,
                    timestamp=transport_object.token.timestamp,
                    metadata_uri=metadata_uri,
                ),
            )
            results.append(out_transport_object)
            self.__stats_service.increment(self.PROCESSED_STAT)

        return results


class TokenMetadataRetrievalBatchProcessor:
    PROCESSED_STAT = "token_metadata_retrieves"
    PROTOCOL_MATCH_REGEX = re.compile("^(?:([^:]+)://|(data):).+$")
    METADATA_RETRIEVAL_HTTP_TIMER = "metadata_retrieval_http"
    METADATA_RETRIEVAL_HTTP_ERROR_STAT = "metadata_retrieval_http_error"
    METADATA_RETRIEVAL_HTTP_TIMEOUT_STAT = "metadata_retrieval_http_timeout"
    METADATA_RETRIEVAL_IPFS_TIMER = "metadata_retrieval_ipfs"
    METADATA_RETRIEVAL_IPFS_ERROR_STAT = "metadata_retrieval_ipfs_error"
    METADATA_RETRIEVAL_IPFS_TIMEOUT_STAT = "metadata_retrieval_ipfs_timeout"
    METADATA_RETRIEVAL_ARWEAVE_TIMER = "metadata_retrieval_arweave"
    METADATA_RETRIEVAL_ARWEAVE_ERROR_STAT = "metadata_retrieval_arweave_error"
    METADATA_RETRIEVAL_ARWEAVE_TIMEOUT_STAT = "metadata_retrieval_arweave_timeout"
    METADATA_RETRIEVAL_DATA_URI_TIMER = "metadata_retrieval_data_uri"
    METADATA_RETRIEVAL_DATA_URI_ERROR_STAT = "metadata_retrieval_data_uri_error"
    METADATA_RETRIEVAL_ENCODING_ERROR_STAT = "metadata_retrieval_encoding_error"
    METADATA_RETRIEVAL_UNSUPPORTED_PROTOCOL_STAT = "metadata_retrieval_unsupported_protocol"

    def __init__(
        self,
        stats_service: StatsService,
        http_data_client: HttpDataClient,
        ipfs_data_client: IpfsDataClient,
        arweave_data_client: ArweaveDataClient,
        data_uri_data_client,
    ) -> None:
        self.__stats_service = stats_service
        self.__http_data_client = http_data_client
        self.__ipfs_data_client = ipfs_data_client
        self.__arweave_data_client = arweave_data_client
        self.__data_uri_data_client = data_uri_data_client

    async def __call__(self, transport_objects: List[TokenTransportObject]):
        results: List[TokenTransportObject] = list()
        coroutines: List[Coroutine] = list()
        uri_tto_map: Dict[str, List[TokenTransportObject]] = dict()
        for transport_object in transport_objects:
            if transport_object.token is None:
                raise ValueError(f"TokenTransportObject provided has no Token: {transport_object}")
            uri = transport_object.token.metadata_uri
            if uri:  # TODO: Filter our non metadata URI tokens before the batch
                stat, data_client = self.__get_stat_and_data_client_for_uri(uri)
            else:
                stat, data_client = None, None
            if stat and data_client:
                if uri not in uri_tto_map:  # Don't get the same URI multiple times
                    uri_tto_map[uri] = list()
                    coroutines.append(
                        self.__uri_indexed_coroutine(
                            uri, self.__timed_coroutine(stat, data_client.get(uri))
                        )
                    )
                uri_tto_map[uri].append(transport_object)
            else:
                self.__stats_service.increment(self.METADATA_RETRIEVAL_UNSUPPORTED_PROTOCOL_STAT)

        if coroutines:
            responses = await asyncio.gather(*coroutines)
            for uri, response_content_type, response in responses:
                for transport_object in uri_tto_map[uri]:
                    if isinstance(response, (ProtocolError, UnicodeDecodeError)):
                        await self.__process_error(response, uri)
                        continue
                    else:
                        if transport_object.token is None:
                            raise ValueError(
                                f"TokenTransportObject provided has no Token: {transport_object}"
                            )
                        if self.__get_protocol_from_uri(uri) == "data":
                            # Data URIs are too large to store
                            metadata_uri = "data"
                        else:
                            metadata_uri = transport_object.token.metadata_uri
                        out_transport_object = TokenTransportObject(
                            block=transport_object.block,
                            transaction_receipt=transport_object.transaction_receipt,
                            token=Token(
                                token_id=transport_object.token.token_id,
                                collection_id=transport_object.token.collection_id,
                                original_owner=transport_object.token.original_owner,
                                timestamp=transport_object.token.timestamp,
                                metadata_uri=metadata_uri,
                                metadata=response,
                                metadata_content_type=response_content_type,
                            ),
                        )
                    results.append(out_transport_object)
                    self.__stats_service.increment(self.PROCESSED_STAT)
        return results

    async def __process_error(self, error: Exception, uri: str) -> None:
        proto = self.__get_protocol_from_uri(uri)
        if isinstance(error, UnicodeDecodeError):
            self.__stats_service.increment(self.METADATA_RETRIEVAL_ENCODING_ERROR_STAT)
        elif proto in ("http", "https"):
            if isinstance(error, ProtocolTimeoutError):
                self.__stats_service.increment(self.METADATA_RETRIEVAL_HTTP_TIMEOUT_STAT)
            else:
                self.__stats_service.increment(self.METADATA_RETRIEVAL_HTTP_ERROR_STAT)
        elif proto == "ipfs":
            if isinstance(error, ProtocolTimeoutError):
                self.__stats_service.increment(self.METADATA_RETRIEVAL_IPFS_TIMEOUT_STAT)
            else:
                self.__stats_service.increment(self.METADATA_RETRIEVAL_IPFS_ERROR_STAT)
        elif proto == "ar":
            if isinstance(error, ProtocolTimeoutError):
                self.__stats_service.increment(self.METADATA_RETRIEVAL_ARWEAVE_TIMEOUT_STAT)
            else:
                self.__stats_service.increment(self.METADATA_RETRIEVAL_ARWEAVE_ERROR_STAT)
        elif proto == "data":
            self.__stats_service.increment(self.METADATA_RETRIEVAL_DATA_URI_ERROR_STAT)

    def __get_protocol_from_uri(self, uri: str) -> Optional[str]:
        match = self.PROTOCOL_MATCH_REGEX.fullmatch(uri)
        if match and match.group(1):
            protocol = match.group(1)
        elif match and match.group(2):
            protocol = match.group(2)
        else:
            protocol = None
        return protocol

    async def __timed_coroutine(self, timer: str, coroutine: Coroutine) -> str:
        with self.__stats_service.timer(timer):
            return await coroutine

    @staticmethod
    async def __uri_indexed_coroutine(uri, coroutine) -> Tuple[str, str, str]:
        try:
            result_content_type, result = await coroutine
        except (ProtocolError, UnicodeDecodeError) as e:
            result_content_type, result = None, e
        return uri, result_content_type, result

    def __get_stat_and_data_client_for_uri(
        self, uri: str
    ) -> Tuple[Optional[str], Optional[DataClient]]:
        proto = self.__get_protocol_from_uri(uri)
        if proto in ("https", "http"):
            stat = self.METADATA_RETRIEVAL_HTTP_TIMER
            data_client = self.__http_data_client
        elif proto == "ipfs":
            stat = self.METADATA_RETRIEVAL_IPFS_TIMER
            data_client = self.__ipfs_data_client
        elif proto == "ar":
            stat = self.METADATA_RETRIEVAL_ARWEAVE_TIMER
            data_client = self.__arweave_data_client
        elif proto == "data":
            stat = self.METADATA_RETRIEVAL_DATA_URI_TIMER
            data_client = self.__data_uri_data_client
        else:
            stat = None
            data_client = None
        return stat, data_client


class TokenPersistenceBatchProcessor:
    PROCESSED_STAT = "tokens_persisted"
    DYNAMODB_TIMER_WRITE_TOKENS = "dynamodb_write_tokens"

    def __init__(
        self,
        dynamodb,
        stats_service: StatsService,
        logger: Logger,
        blockchain: str,
    ) -> None:
        self.__stats_service = stats_service
        self.__dynamodb = dynamodb
        self.__logger = logger
        self.__blockchain = blockchain

    async def __call__(
        self, transport_objects: List[TokenTransportObject]
    ) -> List[TokenTransportObject]:
        result = transport_objects[:]
        table = await self.__dynamodb.Table(Tokens.table_name)
        items = list()
        try:
            with self.__stats_service.timer(self.DYNAMODB_TIMER_WRITE_TOKENS):
                async with table.batch_writer() as batch_writer:
                    for transport_object in transport_objects:
                        if transport_object.token is None:
                            raise ValueError(
                                f"TokenTransportObject provided has no Token: {transport_object}"
                            )
                        collection_id = transport_object.token.collection_id
                        blockchain_collection_id = keccak(
                            (self.__blockchain + collection_id).encode("utf8")
                        ).hex()
                        timestamp = transport_object.token.timestamp.int_value
                        original_owner = transport_object.token.original_owner
                        token_id = transport_object.token.token_id.hex_value
                        metadata_uri = transport_object.token.metadata_uri
                        if metadata_uri and metadata_uri.startswith("data:"):
                            metadata_uri = "data"

                        item = {
                            "blockchain_collection_id": blockchain_collection_id,
                            "token_id": token_id,
                            "blockchain": self.__blockchain,
                            "collection_id": collection_id,
                            "original_owner": original_owner,
                            "mint_timestamp": timestamp,
                            "metadata_uri": metadata_uri,
                        }
                        items.append(item)
                        await batch_writer.put_item(Item=item)
                        self.__stats_service.increment(self.PROCESSED_STAT)
        except (ClientError, decimal.Inexact):
            self.__logger.exception(f"Error writing items to Tokens: {items}")
        return result


class TokenMetadataPersistenceBatchProcessor:
    PROCESSED_STAT = "token_metadata_files_persisted"
    S3_TIMER_WRITE_METADATA_FILES = "s3_write_metadata_files"

    def __init__(
        self,
        s3_bucket,
        s3_batch_size: int,
        stats_service: StatsService,
        logger: Logger,
        blockchain: str,
    ) -> None:
        self.__stats_service = stats_service
        self.__s3_bucket = s3_bucket
        self.__s3_batch_size = s3_batch_size
        self.__logger = logger
        self.__blockchain = blockchain

    async def __call__(self, transport_objects: List[TokenTransportObject]):
        coroutines: List[Coroutine] = list()
        for transport_object in transport_objects:
            if transport_object.token is None:
                raise ValueError(f"TokenTransportObject provided has no Token: {transport_object}")
            token: Token = transport_object.token
            coroutines.append(self.store_metadata(token))
        await asyncio.gather(*coroutines)

    async def store_metadata(self, token: Token):
        with self.__stats_service.timer(self.S3_TIMER_WRITE_METADATA_FILES):
            await self.__s3_bucket.put_object(
                Key=f"{self.__blockchain}/{token.collection_id}/{token.token_id}",
                Body=token.metadata.encode("utf8"),
                ContentType=token.metadata_content_type,
            )
        self.__stats_service.increment(self.PROCESSED_STAT)


class RPCErrorRetryDecoratingBatchProcessor:
    def __init__(
        self, processor: Callable[[List], Coroutine[Any, Any, List]], max_retries=0
    ) -> None:
        self.__processor = processor
        self.__max_retries = max_retries

    async def __call__(self, batch_items: List):
        retries = 0
        while True:
            # noinspection PyBroadException
            try:
                return await self.__processor(batch_items)
            except (RPCError, asyncio.TimeoutError, json.decoder.JSONDecodeError):
                if retries > self.__max_retries:
                    raise
                else:
                    retries += 1
