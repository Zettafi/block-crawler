import asyncio
import dataclasses
import time
from asyncio import CancelledError, Task
from typing import Dict, Union, List, Awaitable, Optional, Tuple

import aioboto3
import boto3
import click
from boto3.dynamodb.conditions import Key, ConditionBase
from boto3.dynamodb.table import TableResource
from botocore.config import Config as BotoConfig
from eth_abi import decode, encode
from hexbytes import HexBytes

from blockcrawler.core.click import HexIntParamType, AddressParamType
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.rpc import RpcServerError, RpcClient
from blockcrawler.core.stats import StatsService
from blockcrawler.core.types import Address, HexInt
from blockcrawler.evm.rpc import EvmRpcClient, EthCall
from blockcrawler.evm.services import BlockTimeService, MemoryBlockTimeCache
from blockcrawler.evm.types import (
    Erc165InterfaceID,
    EvmBlock,
    EvmLog,
    Erc165Functions,
    Erc721Functions,
    Erc721MetadataFunctions,
    Erc721EnumerableFunctions,
    Erc1155MetadataUriFunctions,
    AdditionalFunctions,
    Erc721Events,
    Erc1155Events,
)
from blockcrawler.nft.bin.shared import Config


try:
    from dotenv import load_dotenv
except ImportError:
    pass


ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

PROCESSING_COLOR = "yellow"
STAT_OWNER_VERIFIED = "owner_verified"
STAT_TOKEN_VERIFIED = "token_verified"
STAT_TRANSFER_VERIFIED = "transfer_verified"
STAT_COLLECTION_VERIFIED = "collection_verified"


@dataclasses.dataclass(frozen=True)
class VerifyResult:
    name: str
    passed: bool
    errors: list
    warnings: list


class RpcService:
    def __init__(self, rpc_client: EvmRpcClient, block_height: HexInt, collection_id: str) -> None:
        self.__rpc_client = rpc_client
        self.__block_height = block_height
        self.__collection_id = collection_id

        self.__supports: Dict[Erc165InterfaceID, bool] = dict()
        self.__token_owners: Dict[int, str] = dict()
        self.__transfer_logs: List[EvmLog] = []
        self.__transfer_logs_loading = False
        self.__uri_logs: List[EvmLog] = []
        self.__uri_logs_loading = False
        self.__total_supply: Optional[int] = None

    async def get_total_supply(self):
        if self.__total_supply is None:
            (self.__total_supply,) = await self.__rpc_client.call(
                EthCall(
                    from_=None,
                    to=self.__collection_id,
                    function=Erc721EnumerableFunctions.TOTAL_SUPPLY,
                    block=self.__block_height,
                )
            )
        return self.__total_supply

    async def get_token_owner(self, token_id: int):
        if token_id not in self.__token_owners:
            (self.__token_owners[token_id],) = await self.__rpc_client.call(
                EthCall(
                    from_=None,
                    to=self.__collection_id,
                    function=Erc721Functions.OWNER_OF_TOKEN,
                    parameters=[token_id],
                    block=self.__block_height,
                )
            )
        return self.__token_owners[token_id]

    async def get_token_id_by_index(self, index: int):
        (token_id,) = await self.__rpc_client.call(
            EthCall(
                from_=None,
                to=self.__collection_id,
                function=Erc721EnumerableFunctions.TOKEN_BY_INDEX,
                parameters=[index],
                block=self.__block_height,
            )
        )
        return token_id

    async def __contract_supports(self, interface: Erc165InterfaceID):
        if interface not in self.__supports:
            (self.__supports[interface],) = await self.__rpc_client.call(
                EthCall(
                    from_=None,
                    to=self.__collection_id,
                    function=Erc165Functions.SUPPORTS_INTERFACE,
                    parameters=[interface.bytes],
                )
            )
        return self.__supports[interface]

    async def contract_supports_erc721(self):
        return await self.__contract_supports(Erc165InterfaceID.ERC721)

    async def contract_supports_erc721_enumerable(self):
        return await self.__contract_supports(Erc165InterfaceID.ERC721_ENUMERABLE)

    async def contract_supports_erc721_metadata(self):
        return await self.__contract_supports(Erc165InterfaceID.ERC721_METADATA)

    async def contract_supports_erc1155(self):
        return await self.__contract_supports(Erc165InterfaceID.ERC1155)

    async def contract_supports_erc1155_metadata_uri(self):
        return await self.__contract_supports(Erc165InterfaceID.ERC1155_METADATA_URI)

    async def get_contract_name(self):
        (name,) = await self.__rpc_client.call(
            EthCall(
                from_=None,
                to=self.__collection_id,
                function=Erc721MetadataFunctions.NAME,
            )
        )
        return name

    async def get_contract_symbol(self):
        (name,) = await self.__rpc_client.call(
            EthCall(
                from_=None,
                to=self.__collection_id,
                function=Erc721MetadataFunctions.SYMBOL,
            )
        )
        return name

    async def get_contract_owner(self):
        try:
            (name,) = await self.__rpc_client.call(
                EthCall(
                    from_=None,
                    to=self.__collection_id,
                    function=AdditionalFunctions.OWNER,
                )
            )
        except RpcServerError:
            name = None

        return name

    async def get_block(self, block_number: HexInt):
        return await self.__rpc_client.get_block(block_number, full_transactions=True)

    async def get_transaction_receipt(self, transaction_hash):
        return await self.__rpc_client.get_transaction_receipt(transaction_hash)

    async def get_erc721_token_uri(self, token_id: HexInt):
        (uri,) = await self.__rpc_client.call(
            EthCall(
                from_=None,
                to=self.__collection_id,
                function=Erc721MetadataFunctions.TOKEN_URI,
                parameters=[token_id.int_value],
                block=self.__block_height,
            )
        )
        return uri

    async def get_erc1155_metadata_uri(self, token_id: HexInt):
        (uri,) = await self.__rpc_client.call(
            EthCall(
                from_=None,
                to=self.__collection_id,
                function=Erc1155MetadataUriFunctions.URI,
                parameters=[token_id.int_value],
                block=self.__block_height,
            )
        )
        return uri

    async def get_token_transfer_logs(self, token_id: HexInt) -> List[EvmLog]:
        token_id_bytes = HexBytes(encode(["uint256"], [token_id.int_value]))
        logs = []
        for log in await self.get_transfer_logs():
            if (
                log.topics[0] == Erc721Events.TRANSFER.event_signature_hash
                and log.topics[3] == token_id_bytes
            ):
                logs.append(log)
            elif log.topics[0] == Erc1155Events.TRANSFER_SINGLE.event_signature_hash:
                log_token_id, _ = decode(["uint256", "uint256"], log.data)
                if token_id.int_value == log_token_id:
                    logs.append(log)
            elif log.topics[0] == Erc1155Events.TRANSFER_BATCH.event_signature_hash:
                token_ids, _ = decode(["uint256[]", "uint256[]"], log.data)
                if token_id.int_value in token_ids:
                    logs.append(log)
        return logs

    async def get_transfer_logs(self):
        while self.__transfer_logs_loading:
            await asyncio.sleep(0)
        if not self.__transfer_logs:
            self.__transfer_logs_loading = True
            logs = []
            # Get all transfer events in the collection from block 1 to block height
            async for log in self.__rpc_client.get_logs(
                [
                    [
                        Erc721Events.TRANSFER.event_signature_hash.hex(),
                        Erc1155Events.TRANSFER_SINGLE.event_signature_hash.hex(),
                        Erc1155Events.TRANSFER_BATCH.event_signature_hash.hex(),
                    ],
                ],
                HexInt(0x0),
                self.__block_height,
                Address(self.__collection_id),
            ):
                logs.append(log)
            self.__transfer_logs = logs
            self.__transfer_logs_loading = False
        return self.__transfer_logs[:]

    async def get_token_uri_logs(self, token_id: HexInt) -> List[EvmLog]:
        token_id_bytes = HexBytes(encode(["uint256"], [token_id.int_value]))
        logs = []
        for log in await self.get_uri_logs():
            if log.topics[1] == token_id_bytes:
                logs.append(log)
        return logs

    async def get_uri_logs(self) -> List[EvmLog]:
        while self.__uri_logs_loading:
            await asyncio.sleep(0)
        if not self.__uri_logs:
            self.__uri_logs_loading = True
            logs = []
            # Get all URI events in the collection from block 1 to block height
            async for log in self.__rpc_client.get_logs(
                [
                    [
                        Erc1155Events.URI.event_signature_hash.hex(),
                    ],
                ],
                HexInt(0x0),
                self.__block_height,
                Address(self.__collection_id),
            ):
                logs.append(log)
            self.__uri_logs = logs
            self.__uri_logs_loading = False
        return self.__transfer_logs[:]


class StatsWriter:
    def __init__(self, stats_service: StatsService):
        self.__stats_service = stats_service

    async def loop(self, increment: int):
        self.write_line()
        try:
            while True:
                await asyncio.sleep(increment)
                self.write_line()
        except CancelledError:
            pass
        finally:
            self.write_line()
            print()

    def write_line(self):
        print(
            "\r",
            f"R:{self.__stats_service.get_count(RpcClient.STAT_RESPONSE_RECEIVED):,}",
            f"C:{self.__stats_service.get_count(STAT_COLLECTION_VERIFIED):,}",
            f"O:{self.__stats_service.get_count(STAT_OWNER_VERIFIED):,}",
            f"T:{self.__stats_service.get_count(STAT_TOKEN_VERIFIED):,}",
            f"X:{self.__stats_service.get_count(STAT_TRANSFER_VERIFIED):,}",
            end="",
        )


@click.command
@click.argument(
    "COLLECTION_ID",
    type=AddressParamType(),
)
@click.argument(
    "BLOCK_HEIGHT",
    type=HexIntParamType(),
)
@click.pass_obj
def verify(
    config: Config,
    collection_id: Address,
    block_height: HexInt,
):
    """
    Verify a collection in databases against an EVM archive node by
    COLLECTION_ID at a BLOCK_HEIGHT.

    COLLECTION_ID: The collection ID of the NFT collection you wish to verify
    BLOCK_HEIGHT: Block height at which you wush tio verify the data.
    """
    stats_writer = StatsWriter(config.stats_service)
    loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(loop)
    stats_task = loop.create_task(stats_writer.loop(1))
    try:
        loop.run_until_complete(
            run(
                blockchain=config.blockchain,
                collection_id=collection_id,
                block_height=block_height,
                boto3_session=aioboto3.Session(),
                dynamodb_endpoint_url=config.dynamodb_endpoint_url,
                dynamodb_timeout=config.dynamodb_timeout,
                table_prefix=config.table_prefix,
                evm_rpc_client=config.evm_rpc_client,
                stats_service=config.stats_service,
            ),
        )
    except KeyboardInterrupt:
        pass
    finally:
        stats_task.cancel()
        while loop.is_running():
            time.sleep(0.001)


async def run(
    blockchain: BlockChain,
    collection_id: Address,
    block_height: HexInt,
    boto3_session: boto3.Session,
    dynamodb_endpoint_url: str,
    dynamodb_timeout: float,
    table_prefix: str,
    evm_rpc_client: EvmRpcClient,
    stats_service: StatsService,
):
    config = BotoConfig(connect_timeout=dynamodb_timeout, read_timeout=dynamodb_timeout)
    dynamodb_resource_kwargs: Dict[str, Union[str, BotoConfig]] = dict(config=config)
    if dynamodb_endpoint_url is not None:  # This would only be in non-deployed environments
        dynamodb_resource_kwargs["endpoint_url"] = dynamodb_endpoint_url
    async with evm_rpc_client:
        rpc_service = RpcService(evm_rpc_client, block_height, collection_id)
        block_time_service = BlockTimeService(MemoryBlockTimeCache(), evm_rpc_client)
        async with boto3_session.resource(
            "dynamodb", **dynamodb_resource_kwargs
        ) as dynamodb:  # type: ignore
            # noinspection PyTypeChecker
            results: Tuple[VerifyResult] = await asyncio.gather(
                verify_collection(
                    dynamodb,
                    rpc_service,
                    stats_service,
                    blockchain,
                    table_prefix,
                    collection_id,
                ),
                verify_tokens(
                    dynamodb,
                    rpc_service,
                    stats_service,
                    blockchain,
                    table_prefix,
                    collection_id,
                ),
                verify_owners(
                    dynamodb,
                    rpc_service,
                    stats_service,
                    blockchain,
                    table_prefix,
                    collection_id,
                ),
                verify_transfers(
                    dynamodb,
                    rpc_service,
                    stats_service,
                    block_time_service,
                    blockchain,
                    table_prefix,
                    collection_id,
                ),
            )
            click.secho()
            for result in results:
                click.secho(
                    f"{result.name}: {'passed' if result.passed else 'failed'}",
                    fg="green" if result.passed else "red",
                )

                click.secho("  Errors:", fg="red" if len(result.errors) else "green", nl=False)
                if result.errors:
                    click.echo()
                    for error in result.errors:
                        click.secho(f"    {error}", fg="red")
                else:
                    click.secho(" None", fg="green")

                click.secho(
                    "  Warnings:", fg="yellow" if len(result.warnings) else "green", nl=False
                )
                if result.warnings:
                    click.echo()
                    for warning in result.warnings:
                        click.secho(f"    {warning}", fg="yellow")
                else:
                    click.secho(" None", fg="green")


async def verify_collection(
    dynamodb,
    rpc_service: RpcService,
    stats_service: StatsService,
    blockchain,
    table_prefix,
    collection_id,
) -> VerifyResult:
    errors: List[str] = []
    warnings: List[str] = []

    collection_table = await dynamodb.Table(f"{table_prefix}collection")
    items = await get_table_items(
        collection_table,
        Key("blockchain").eq(blockchain.value) & Key("collection_id").eq(collection_id),
    )

    if items:
        record = items[0]
        if await rpc_service.contract_supports_erc721_metadata():
            contract_name = await rpc_service.get_contract_name()
            contract_name_lower = contract_name.lower()[:1024]
            contract_symbol = await rpc_service.get_contract_symbol()
        else:
            contract_name = None
            contract_name_lower = None
            contract_symbol = None

        if await rpc_service.contract_supports_erc721():
            specification = "ERC-721"
        elif await rpc_service.contract_supports_erc1155():
            specification = "ERC-1155"
        else:
            specification = "UNKNOWN"

        db_specification = record.get("specification")
        if db_specification != specification:
            errors.append(
                f"Database has specification set to {db_specification} but contract "
                f"claims to support {specification}"
            )
        db_name = record.get("collection_name")
        if db_name != contract_name:
            errors.append(
                f"Database has collection_name set to {db_name} but contract name() "
                f"returns {contract_name}"
            )
        db_name_lower = record.get("name_lower")
        if db_name_lower != contract_name_lower:
            errors.append(
                f'Database has name_lower set to "{db_name_lower}" but first 1024 chars of '
                f'lowercase contract name() would be "{contract_name_lower}"'
            )
        db_symbol = record.get("symbol")
        if db_symbol != contract_symbol:
            errors.append(
                f"Database has symbol set to {db_symbol} but contract symbol() returns"
                f" {contract_symbol}"
            )

        contract_owner = await rpc_service.get_contract_owner()
        db_owner = record.get("owner_account")
        if db_owner != contract_owner:
            errors.append(
                f"Database has owner_account set to {db_owner} but contract owner() returns"
                f" {contract_owner}"
            )

        if await rpc_service.contract_supports_erc721_enumerable():
            contract_total_supply = await rpc_service.get_total_supply()
        else:
            contract_total_supply = None

        db_total_supply = (
            HexInt(record.get("total_supply")).int_value if record.get("total_supply") else None
        )
        if db_total_supply != contract_total_supply:
            errors.append(
                f"Database has total_supply set to {db_total_supply} but contract "
                f"total_supply() returns {contract_total_supply}"
            )

        # Verify block_created, created time, and creator
        block: EvmBlock = await rpc_service.get_block(HexInt(record["block_created"]))
        if block.timestamp.int_value != record["date_created"]:
            errors.append(
                f"Database has date_created set to {record['date_created']} but block "
                f"{record['block_created']} has {block.timestamp.int_value}"
            )

        if not block.transactions:
            raise ValueError("No transactions in block")
        potential_create_txs = [
            await rpc_service.get_transaction_receipt(tx.hash)
            for tx in block.transactions
            if tx.to_ is None
        ]

        transaction_match = False
        for tx in potential_create_txs:
            if tx.contract_address == collection_id:
                transaction_match = True
                if record["creator_account"] != tx.from_:
                    errors.append(
                        f"Database has creator_account set to {record['creator_account']}"
                        f" but transaction {tx.transaction_hash} has {tx.from_}"
                    )
        if not transaction_match:
            errors.append(
                f"Cannot find transaction for creation of collection in block "
                f"{record['block_created']} as identified in block_created in database."
            )

    else:
        errors.append("No record for Collection in the database")
    stats_service.increment(STAT_COLLECTION_VERIFIED)
    return VerifyResult(
        name="Collections", passed=len(errors) == 0, errors=errors, warnings=warnings
    )


async def verify_tokens(
    dynamodb,
    rpc_service: RpcService,
    stats_service: StatsService,
    blockchain,
    table_prefix,
    collection_id,
) -> VerifyResult:
    table = await dynamodb.Table(f"{table_prefix}token")
    warnings = []
    errors = []
    token_count = 0
    tasks: List[Task] = []
    key_condition_expression = Key("blockchain_collection_id").eq(
        f"{blockchain.value}::{collection_id}"
    )
    for token in await get_table_items(table, key_condition_expression):
        token_errors, token_warnings = await verify_token(
            rpc_service, stats_service, collection_id, token
        )
        errors.extend(token_errors)
        warnings.extend(token_warnings)
        token_count += 1

    # Verify token quantity
    if await rpc_service.contract_supports_erc721_enumerable():
        expected_tokens = await rpc_service.get_total_supply()
        if token_count != expected_tokens:
            errors.append(
                f"{token_count} token records in database does not match "
                f"total_supply() result of {expected_tokens}"
            )
    else:
        warnings.append(
            "Collection does not support enumerable which is required to verify "
            "the number of token records"
        )
    await asyncio.gather(*tasks)
    return VerifyResult(name="Tokens", passed=len(errors) == 0, errors=errors, warnings=warnings)


async def get_table_items(table: TableResource, key_condition_expression: ConditionBase):
    key = None
    items = []
    while True:
        query_kwargs = {"KeyConditionExpression": key_condition_expression}
        if key:
            query_kwargs["ExclusiveStartKey"] = key

        response = await table.query(**query_kwargs)

        if "Items" in response:
            items.extend(response["Items"])

        if "LastEvaluatedKey" in response:
            key = response["LastEvaluatedKey"]
        else:
            break
    return items


async def get_token_original_token_owner(
    rpc_service: RpcService, collection_id: str, token_id: HexInt
):
    zero_addr = HexBytes(encode(["address"], ["0x0000000000000000000000000000000000000000"]))
    contract_addr = HexBytes(encode(["address"], [collection_id]))

    for log in await rpc_service.get_token_transfer_logs(token_id):
        _, from_, to_, _ = log.topics
        if from_ in [zero_addr, contract_addr] and to_ not in [zero_addr, contract_addr]:
            (original_owner,) = decode(["address"], to_)
            return original_owner


async def get_current_owner(rpc_service: RpcService, token_id: HexInt):
    transfers: List[EvmLog] = await rpc_service.get_token_transfer_logs(token_id)
    if transfers:
        (current_owner,) = decode(["address"], transfers.pop().topics[2])
    else:
        current_owner = None
    return current_owner


async def verify_token(
    rpc_service: RpcService,
    stats_service: StatsService,
    collection_id: str,
    token: dict,
):
    errors: List[str] = []
    warnings: List[str] = []
    token_id = HexInt(token["token_id"])

    if await rpc_service.contract_supports_erc721():
        original_owner = await get_token_original_token_owner(rpc_service, collection_id, token_id)
        db_original_owner = token.get("original_owner_account")
        if original_owner != db_original_owner:
            errors.append(
                f"Database original owner value {db_original_owner} does not "
                f"log event value of {original_owner} for token {token_id.int_value}"
            )

        current_owner = await get_current_owner(rpc_service, token_id)
        db_current_owner = token.get("current_owner_account")
        if current_owner != db_current_owner:
            errors.append(
                f"Database current owner value {db_current_owner} does not match "
                f"log event value of {current_owner} for token {token_id.int_value}"
            )

        if await rpc_service.contract_supports_erc721_metadata():
            contract_metadata_uri = await rpc_service.get_erc721_token_uri(token_id)
        else:
            contract_metadata_uri = None
        db_metadata_url = token.get("metadata_url")
        if contract_metadata_uri != db_metadata_url:
            errors.append(
                f"Database metadata URL value {db_metadata_url} does not match "
                f"tokenURI() value of {contract_metadata_uri} for token {token_id.int_value}"
            )
        token_quantity = token.get("quantity")
        if token_quantity != 1:
            errors.append(
                f"Database quantity value value {token_quantity} does not match "
                f"expected value of 1 for token {token_id.int_value}"
            )
    elif await rpc_service.contract_supports_erc1155():
        if token.get("original_owner_account") is not None:
            errors.append("ERC-1155 token item should not have original_owner_account set")
        if token.get("current_owner_account") is not None:
            errors.append("ERC-1155 token item should not have current_owner_account set")

        logs = await rpc_service.get_token_uri_logs(token_id)
        (log_uri,) = decode(["string"], logs.pop().topics[1]) if logs else (None,)
        if await rpc_service.contract_supports_erc1155_metadata_uri():
            contract_uri = await rpc_service.get_erc1155_metadata_uri(token_id)
        else:
            contract_uri = None
        if log_uri and contract_uri and log_uri != contract_uri:
            warnings.append(
                f"Contract discrepancy for metadata URI on token{token_id} as contract "
                f"uri() returns {contract_uri} and last URI event log has {log_uri}"
            )
        db_metadata_url = token.get("metadata_url")
        if log_uri:
            if log_uri != db_metadata_url:
                errors.append(
                    f"Metadata URL in token item for token {token_id} has {db_metadata_url} and "
                    f"URI event logs have {log_uri}"
                )
        elif contract_uri:
            if contract_uri != db_metadata_url:
                errors.append(
                    f"Metadata URL in token item for token {token_id} has {db_metadata_url} "
                    f"and contrat uri() returns {contract_uri}"
                )

        expected_quantity = 0
        for log in await rpc_service.get_token_transfer_logs(token_id):
            (from_,) = decode(["address"], log.topics[2])
            (to_,) = decode(["address"], log.topics[3])
            if log.topics[0] == Erc1155Events.TRANSFER_SINGLE.event_signature_hash:
                _, quantity = decode(["uint256", "uint256"], log.data)
            elif log.topics[0] == Erc1155Events.TRANSFER_BATCH.event_signature_hash:
                tokens, values = decode(["uint256[]", "uint256[]"], log.data)
                quantities = [
                    value
                    for log_token_id, value in zip(tokens, values)
                    if log_token_id == token_id.int_value
                ]
                quantity = sum(quantities)
            if from_ in (ZERO_ADDRESS, collection_id) and to_ not in (ZERO_ADDRESS, collection_id):
                expected_quantity += quantity
            elif to_ == ZERO_ADDRESS and from_ not in (ZERO_ADDRESS, collection_id):
                expected_quantity -= quantity
        if token["quantity"] != expected_quantity:
            errors.append(
                f"Database quantity value value {token['quantity']} does not match "
                f"expected value of {expected_quantity} for token {token_id.int_value}"
            )
    else:
        warnings.append(
            f"Unable to verify metadata URI for token {token_id} as Contract supports "
            f"neither ERC-721 nor ERC-1155"
        )
    stats_service.increment(STAT_TOKEN_VERIFIED)
    return errors, warnings


async def verify_erc721_transfer(
    block_time_service: BlockTimeService,
    stats_service: StatsService,
    log: EvmLog,
    item: dict,
    collection_id: Address,
):
    (log_from,) = decode(["address"], log.topics[1])
    (log_to,) = decode(["address"], log.topics[2])
    (token_id,) = decode(["uint256"], log.topics[3])
    errors = await verify_transfer(
        block_time_service=block_time_service,
        collection_id=collection_id,
        item=item,
        block_number=log.block_number,
        transaction_index=log.transaction_index,
        log_index=log.log_index,
        transaction_hash=log.transaction_hash,
        quantity=HexInt("0x1"),
        token_id=HexInt(token_id),
        log_from=Address(log_from),
        log_to=Address(log_to),
    )
    stats_service.increment(STAT_TRANSFER_VERIFIED)
    return errors


async def verify_erc1155_transfer(
    block_time_service: BlockTimeService,
    stats_service: StatsService,
    log: EvmLog,
    items: List[dict],
    collection_id: Address,
):
    errors = []
    if log.topics[0] == Erc1155Events.TRANSFER_SINGLE.event_signature_hash:
        token_id, quantity = decode(["uint256", "uint256"], log.data)
        token_ids = [token_id]
        quantities = [quantity]
    elif log.topics[0] == Erc1155Events.TRANSFER_BATCH.event_signature_hash:
        token_ids, quantities = decode(["uint256[]", "uint256[]"], log.data)
    else:
        raise ValueError(f"Log is neither TransferSingle nor TransferBatch: {log}")
    (log_from,) = decode(["address"], log.topics[2])
    (log_to,) = decode(["address"], log.topics[3])
    check_items: List[Tuple[int, int]] = [
        (token_id, quantity) for token_id, quantity in zip(token_ids, quantities)
    ]

    for token_id, quantity in check_items:
        try:
            index, item = [
                (index, item)
                for index, item in enumerate(items)
                if HexInt(item["token_id"]) == token_id
            ][0]
            del items[index]

            errors = await verify_transfer(
                block_time_service=block_time_service,
                collection_id=collection_id,
                item=item,
                block_number=log.block_number,
                transaction_index=log.transaction_index,
                log_index=log.log_index,
                transaction_hash=log.transaction_hash,
                quantity=HexInt(quantity),
                token_id=HexInt(token_id),
                log_from=Address(log_from),
                log_to=Address(log_to),
            )
        except IndexError:
            errors.append(
                f"Transfer Log from RPC with block_number {log.block_number} and "
                f"transaction_index {log.transaction_index} and log_index "
                f"{log.log_index} and token_id {token_id} could not be found in "
                f"tokentransfers items"
            )

    for item in items:
        errors.append(
            f"tokentransfers item with block_id {item['block_id']} and "
            f"transaction_index {item['transaction_index']} and log_index "
            f"{item['log_index']} and token_id {item['token_id']} is not in "
            f"Transfer Logs from RPC"
        )

    stats_service.increment(STAT_TRANSFER_VERIFIED)
    return errors


async def verify_transfer(
    block_time_service: BlockTimeService,
    collection_id: Address,
    block_number: HexInt,
    transaction_index,
    log_index: HexInt,
    transaction_hash,
    item: dict,
    quantity: HexInt,
    token_id: HexInt,
    log_from: Address,
    log_to: Address,
):
    errors = []
    if item["collection_id"] != collection_id:
        errors.append(
            f"Database tokentransfers item with block_id {block_number} and "
            f"transaction_index {transaction_index} and log_index "
            f"{log_index} has incorrect collection_id {collection_id}"
        )
    transaction_hash = transaction_hash.hex()
    if item["transaction_hash"] != transaction_hash:
        errors.append(
            f"Transfer Log from RPC with block_number {block_number} and "
            f"transaction_index {transaction_index} and log_index "
            f"{log_index} has transaction_hash of {transaction_hash} but "
            f"tokentransfers item has {item['transaction_hash']}"
        )
    block_time = await block_time_service.get_block_timestamp(block_number)
    if item["transaction_timestamp"] != block_time.int_value:
        errors.append(
            f"Transfer Log from RPC with block_number {block_number} and "
            f"transaction_index {transaction_index} and log_index "
            f"{log_index} has a block time of {block_time} but "
            f"tokentransfers item has {item['transaction_timestamp']}"
        )
    if item["from_account"] != log_from:
        errors.append(
            f"Transfer Log from RPC with block_number {block_number} and "
            f"transaction_index {transaction_index} and log_index "
            f"{log_index} has from address of {log_from} but tokentransfers "
            f"item has {item['from_account']}"
        )
    if item["to_account"] != log_to:
        errors.append(
            f"Transfer Log from RPC with block_number {block_number} and "
            f"transaction_index {transaction_index} and log_index "
            f"{log_index} has to address of {log_to} but tokentransfers "
            f"item has {item['to_account']}"
        )
    if log_from in (ZERO_ADDRESS, collection_id) and log_to not in (
        ZERO_ADDRESS,
        collection_id,
    ):
        expected_type = "mint"
    elif log_from != ZERO_ADDRESS and log_to == ZERO_ADDRESS:
        expected_type = "burn"
    else:
        expected_type = "transfer"
    if item["transaction_type"] != expected_type:
        errors.append(
            f"Transfer Log from RPC with block_number {block_number} and "
            f"transaction_index {transaction_index} and log_index "
            f"{log_index} has from address of {log_from} and to address of "
            f"{log_to} but tokentransfers item has transaction_type "
            f"{item['transaction_type']}"
        )
    if item["token_id"] != token_id.hex_value:
        errors.append(
            f"Transfer Log from RPC with block_number {block_number} and "
            f"transaction_index {transaction_index} and log_index "
            f"{log_index} has to address of {log_to} but tokentransfers "
            f"item has {item['to_account']}"
        )
    if HexInt(item["quantity"]) != quantity:
        errors.append(
            f"Transfer Log from RPC with block_number {block_number} and "
            f"transaction_index {transaction_index} and log_index "
            f"{log_index} has quantity of {quantity} but tokentransfers "
            f"item has {item['quantity']}"
        )

    return errors


async def verify_transfers(
    dynamodb,
    rpc_service: RpcService,
    stats_service: StatsService,
    block_time_service: BlockTimeService,
    blockchain,
    table_prefix,
    collection_id,
) -> VerifyResult:
    errors: List[str] = []
    warnings: List[str] = []
    table = await dynamodb.Table(f"{table_prefix}tokentransfers")
    transfer_items: List[dict] = await get_table_items(
        table, Key("blockchain_collection_id").eq(f"{blockchain.value}::{collection_id}")
    )
    transfer_logs: List[EvmLog] = await rpc_service.get_transfer_logs()

    tasks = []
    if await rpc_service.contract_supports_erc721():
        while transfer_logs:
            log = transfer_logs.pop(0)
            try:
                index, item = [
                    (index, item)
                    for index, item in enumerate(transfer_items)
                    if HexInt(item["block_id"]) == log.block_number
                    and HexInt(item["transaction_index"]) == log.transaction_index
                    and HexInt(item["log_index"]) == log.log_index
                ][0]
                del transfer_items[index]
                tasks.append(
                    asyncio.create_task(
                        verify_erc721_transfer(
                            block_time_service, stats_service, log, item, collection_id
                        )
                    )
                )
                await asyncio.sleep(0)  # Allow task to start
            except IndexError:
                errors.append(
                    f"Transfer Log from RPC with block_number {log.block_number} and "
                    f"transaction_index {log.transaction_index} and log_index "
                    f"{log.log_index} could not be found in tokentransfers items"
                )
                stats_service.increment(STAT_TRANSFER_VERIFIED)
        for task_errors in await asyncio.gather(*tasks):
            errors.extend(task_errors)

    elif await rpc_service.contract_supports_erc1155():
        while transfer_logs:
            log = transfer_logs.pop(0)
            try:
                items: List[Tuple[int, dict]] = [
                    (index, item)
                    for index, item in enumerate(transfer_items)
                    if HexInt(item["block_id"]) == log.block_number
                    and HexInt(item["transaction_index"]) == log.transaction_index
                    and HexInt(item["log_index"]) == log.log_index
                ]
                review_items = []
                for index, item in items:
                    review_items.append(item)
                    del transfer_items[index]
                tasks.append(
                    asyncio.create_task(
                        verify_erc1155_transfer(
                            block_time_service, stats_service, log, review_items, collection_id
                        )
                    )
                )
                await asyncio.sleep(0)  # Allow task to start
            except IndexError:
                errors.append(
                    f"Transfer Log from RPC with block_number {log.block_number} and "
                    f"transaction_index {log.transaction_index} and log_index "
                    f"{log.log_index} could not be found in tokentransfers items"
                )
                stats_service.increment(STAT_TRANSFER_VERIFIED)
        for task_errors in await asyncio.gather(*tasks):
            errors.extend(task_errors)

    for transfer_item in transfer_items:
        errors.append(
            f"tokentransfers item with block_id {transfer_item['block_id']} and "
            f"transaction_index {transfer_item['transaction_index']} and log_index "
            f"{transfer_item['log_index']} is not in Transfer Logs from RPC"
        )

    return VerifyResult(name="Transfers", passed=len(errors) == 0, errors=errors, warnings=warnings)


async def verify_owners(
    dynamodb,
    rpc_service: RpcService,
    stats_service: StatsService,
    blockchain: BlockChain,
    table_prefix: str,
    collection_id: str,
):
    table = await dynamodb.Table(f"{table_prefix}owner")
    errors: List[str] = []
    warnings: List[str] = []

    if await rpc_service.contract_supports_erc721_enumerable():
        total_supply = await rpc_service.get_total_supply()

        checks: List[Awaitable] = []
        for index in range(total_supply):
            checks.append(
                check_for_erc721_owner_discrepancy(
                    table=table,
                    rpc_service=rpc_service,
                    stats_service=stats_service,
                    blockchain=blockchain,
                    collection_id=collection_id,
                    token_index=index,
                )
            )
        check_results: Tuple = await asyncio.gather(*checks)
        errors, warnings = zip(*check_results)
    else:
        warnings.append(
            "Collection does not support EC721 Enumerable interface which is required "
            "for fully verifying owners"
        )

    errors = [error for error in errors if error]
    warnings = [warning for warning in warnings if warning]
    return VerifyResult(
        name="Owner",
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


async def check_for_erc721_owner_discrepancy(
    table: TableResource,
    rpc_service: RpcService,
    stats_service: StatsService,
    blockchain: BlockChain,
    collection_id: str,
    token_index: int,
):
    discrepancy, warning = None, None

    try:
        token_id = await rpc_service.get_token_id_by_index(token_index)

        contract_owner = await rpc_service.get_token_owner(token_id)

        response = await get_table_items(
            table,
            Key("blockchain_account").eq(f"{blockchain.value}::{contract_owner}")
            & Key("collection_id_token_id").eq(f"{collection_id}::{HexInt(token_id).hex_value}"),
        )
        if await rpc_service.contract_supports_erc721():
            response_len = len(response)
            if response_len == 1 and response[0]["quantity"] == 1:
                discrepancy = None
            elif response_len > 0:
                discrepancy = (
                    f"{response_len} database owner records for token {token_id} "
                    f"and owner address {contract_owner}"
                )
            else:
                discrepancy = (
                    f"No owner record found in database for token {token_id} and "
                    f"owner address {contract_owner}"
                )
        elif await rpc_service.contract_supports_erc1155():
            warning = f"No check for token {token_id}"
    except RpcServerError as e:
        warning = f"Unable to verify owner for token at index {token_index} due to RPC error: {e}"

    stats_service.increment(STAT_OWNER_VERIFIED)
    return discrepancy, warning


if __name__ == "__main__":
    load_dotenv()
    verify()
