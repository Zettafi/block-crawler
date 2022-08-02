import asyncio
from datetime import datetime
from math import floor
from statistics import median, mean

import click

from chainconductor.blockcrawler.commands import process_contracts_async
from chainconductor.blockcrawler.processors import (
    BlockProcessor,
    TransactionProcessor,
    CollectionPersistenceProcessor,
    ContractProcessor,
    TokenTransferProcessor,
    TokenMetadataProcessor,
    TokenPersistenceProcessor,
    TokenMetadataGatheringProcessor,
)
from chainconductor.blockcrawler.stats import StatsService

try:  # If dotenv in installed, use it load env vars
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


async def _stats_writer(
    stats_service: StatsService,
    start_time: datetime,
    run_forever=True,
    show_header=True,
):
    if show_header:
        print(
            "Total Time  | Blocks      | Transactions | Contracts   | "
            "Collection Writes | Token Transfer Writes | Token Metadata | "
            "Metadata Gather | Token Writes"
        )

    while True:
        await asyncio.sleep(1)
        end = datetime.utcnow()
        total_time = end - start_time
        seconds = total_time.seconds % 60 + total_time.microseconds / 1_000_000
        minutes = total_time.seconds // 60
        hours = minutes // 60
        print(
            "\r{:02d}:{:02d}:{:05.2f}".format(hours, minutes, seconds),
            "|",
            "{:<11,}".format(stats_service.get_count(BlockProcessor.PROCESSED_STAT)),
            "|",
            "{:<12,}".format(stats_service.get_count(TransactionProcessor.PROCESSED_STAT)),
            "|",
            "{:<11,}".format(stats_service.get_count(ContractProcessor.PROCESSED_STAT)),
            "|",
            "{:<17,}".format(
                stats_service.get_count(CollectionPersistenceProcessor.PROCESSED_STAT)
            ),
            "|",
            "{:<21,}".format(stats_service.get_count(TokenTransferProcessor.PROCESSED_STAT)),
            "|",
            "{:<14,}".format(stats_service.get_count(TokenMetadataProcessor.PROCESSED_STAT)),
            "|",
            "{:<15,}".format(
                stats_service.get_count(TokenMetadataGatheringProcessor.PROCESSED_STAT)
            ),
            "|",
            "{:<12,}".format(stats_service.get_count(TokenPersistenceProcessor.PROCESSED_STAT)),
            end="",
        )
        if not run_forever:
            break


@click.command()
@click.argument("STARTING_BLOCK", type=int)
@click.argument("ENDING_BLOCK", type=int)
@click.option(
    "--rpc-batch-size",
    default=100,
    show_default=True,
    help="Batch size for JSON-RPC calls",
)
@click.option(
    "--dynamodb-batch-size",
    default=25,
    show_default=True,
    help="Batch size DynamoDB calls",
)
@click.option(
    "--http-batch-size",
    default=100,
    show_default=True,
    help="Maximum concurrent HTTP calls",
)
@click.option(
    "--max-batch-wait-time",
    default=10,
    show_default=True,
    help="Maximum time imn seconds to wait for batch size to be reached before " "processing batch",
)
@click.option(
    "--archive_node_uri",
    envvar="ARCHIVE_NODE_URI",
    help="URI to access the archive node RPC HTTP server",
)
@click.option(
    "--dynamodb_uri",
    envvar="DYNAMODB_URI",
    help="Override URI for connecting to DynamoDB",
)
@click.option(
    "--ipfs_node_uri",
    envvar="IPFS_NODE_URI",
    help="URI for IPFS requests to obtain token metadata",
)
@click.option(
    "--arweave_node_uri",
    envvar="ARWEAVE_NODE_URI",
    help="URI for Arweave requests to obtain token metadata",
)
@click.option(
    "--block-processors",
    default=10,
    show_default=True,
    help="Number of parallel block processors to run",
)
@click.option(
    "--transaction-processors",
    default=100,
    show_default=True,
    help="Number of parallel transaction processors to run",
)
@click.option(
    "--contract-processors",
    default=1,
    show_default=True,
    help="Number of parallel contract processors to run",
)
@click.option(
    "--contract-persistence-processors",
    default=1,
    show_default=True,
    help="Number of parallel contract persistence processors to run",
)
@click.option(
    "--token-processors",
    default=80,
    show_default=True,
    help="Number of parallel token processors to run",
)
@click.option(
    "--token-metadata-processors",
    default=1,
    show_default=True,
    help="Number of parallel token processors to run",
)
@click.option(
    "--token-metadata_gathering-processors",
    default=1,
    show_default=True,
    help="Number of parallel token processors to run",
)
@click.option(
    "--token-persistence-processors",
    default=1,
    show_default=True,
    help="Number of parallel token processors to run",
)
def process_contracts(
    starting_block: int,
    ending_block: int,
    archive_node_uri: str,
    dynamodb_uri: str,
    ipfs_node_uri: str,
    arweave_node_uri: str,
    rpc_batch_size: int,
    dynamodb_batch_size: int,
    http_batch_size: int,
    max_batch_wait_time: int,
    block_processors: int,
    transaction_processors: int,
    contract_processors: int,
    contract_persistence_processors: int,
    token_processors: int,
    token_metadata_processors: int,
    token_metadata_gathering_processors: int,
    token_persistence_processors: int,
):
    """
    Crawl the blocks from the STARTING_BLOCK to the
    ENDING_BLOCK from an archive node, parse the data we want to collect
    and put that data in the database
    """
    start = datetime.utcnow()
    stats_service = StatsService()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stats_writer = loop.create_task(_stats_writer(stats_service, start, True, True))
    try:
        loop.run_until_complete(
            process_contracts_async(
                stats_service=stats_service,
                archive_node_uri=archive_node_uri,
                dynamodb_uri=dynamodb_uri,
                ipfs_node_uri=ipfs_node_uri,
                arweave_node_uri=arweave_node_uri,
                starting_block=starting_block,
                ending_block=ending_block,
                rpc_batch_size=rpc_batch_size,
                dynamodb_batch_size=dynamodb_batch_size,
                http_batch_size=http_batch_size,
                max_batch_wait_time=max_batch_wait_time,
                block_processor_instances=block_processors,
                transaction_processor_instances=transaction_processors,
                contract_processor_instances=contract_processors,
                contract_persistence_processor_instances=contract_persistence_processors,
                token_processor_instances=token_processors,
                token_metadata_processor_instances=token_metadata_processors,
                token_metadata_gathering_processor_instances=token_metadata_gathering_processors,
                token_persistence_processor_instances=token_persistence_processors,
            )
        )
        asyncio.gather(stats_writer)
    except KeyboardInterrupt:
        pass

    loop.run_until_complete(_stats_writer(stats_service, start, False, False))
    loop.stop()
    print()

    def _get_time_from_secs(nanoseconds):
        seconds = nanoseconds / 1_000_000_000
        minutes = floor(seconds / 60)
        return "{:02d}:{:05.2f}".format(minutes, seconds % 60)

    click.echo("Blocks:")
    blocks_processed = stats_service.get_count(BlockProcessor.PROCESSED_STAT)
    click.echo("  Total Processed: {:,}".format(blocks_processed))
    block_batches = stats_service.get_timings(BlockProcessor.RPC_TIMER_GET_BLOCKS)
    block_batches_count = len(block_batches)
    click.echo("  Get Block RPC Calls: {:,}".format(block_batches_count))
    block_batches_average_size = (
        blocks_processed / block_batches_count if block_batches_count else 0.0
    )
    click.echo("  Get Block RPC Avg Batch Size: {:0.2f}".format(block_batches_average_size))
    click.echo(
        "  Get Block RPC Total Processing Time: {}".format(_get_time_from_secs(sum(block_batches)))
    )
    click.echo(
        "  Get Block RPC Median Processing Time: {}".format(
            _get_time_from_secs(median(block_batches)) if block_batches else "n/a"
        )
    )
    click.echo(
        "  Get Block RPC Mean Processing Time: {}".format(_get_time_from_secs(mean(block_batches)))
        if block_batches
        else "n/a"
    )

    click.echo()
    click.echo("Transactions Receipts:")
    transactions_processed = stats_service.get_count(TransactionProcessor.PROCESSED_STAT)
    click.echo("  Total Processed: {:,}".format(transactions_processed))
    transaction_batches = stats_service.get_timings(
        TransactionProcessor.RPC_TIMER_GET_TRANSACTION_RECEIPTS
    )
    transaction_batches_count = len(transaction_batches)
    transaction_batches_average_size = (
        transactions_processed / transaction_batches_count if transaction_batches_count > 0 else 0.0
    )
    click.echo("  Get Transaction Receipt RPC Calls: {:,}".format(len(transaction_batches)))
    click.echo(
        "  Get Transaction Receipt RPC Avg Batch Size: {:0.2f}".format(
            transaction_batches_average_size
        )
    )
    click.echo(
        "  Get Transaction Receipt RPC Total Processing Time: {}".format(
            _get_time_from_secs(sum(transaction_batches)) if transaction_batches else "n/a"
        )
    )
    click.echo(
        "  Get Transaction Receipt RPC Median Processing Time: {}".format(
            _get_time_from_secs(median(transaction_batches)) if transaction_batches else "n/a"
        )
    )
    click.echo(
        "  Get Transaction Receipt RPC Mean Processing Time: {}".format(
            _get_time_from_secs(mean(transaction_batches)) if transaction_batches else "n/a"
        )
    )

    click.echo()
    click.echo("Smart Contracts:")
    contracts_processed = stats_service.get_count(ContractProcessor.PROCESSED_STAT)
    click.echo("  Total Processed: {:,}".format(contracts_processed))
    click.echo("  Supports Interfaces:")
    supports_interface_batches = stats_service.get_timings(
        ContractProcessor.RPC_TIMER_CALL_SUPPORTS_INTERFACES
    )
    click.echo(
        "    Get Call Supports Interface RPC Calls: {:,}".format(len(supports_interface_batches))
    )
    click.echo(
        "    Get Call Supports Interface RPC Total Processing Time: {}".format(
            _get_time_from_secs(sum(supports_interface_batches))
            if supports_interface_batches
            else "n/a"
        )
    )
    click.echo(
        "    Get Call Supports Interface RPC Median Processing Time: {}".format(
            _get_time_from_secs(median(supports_interface_batches))
            if supports_interface_batches
            else "n/a"
        )
    )
    click.echo(
        "    Get Call Supports Interface RPC Mean Processing Time: {}".format(
            _get_time_from_secs(mean(supports_interface_batches))
            if supports_interface_batches
            else "n/a"
        )
    )
    click.echo("  Contract Metadata:")
    get_metadata_batches = stats_service.get_timings(
        ContractProcessor.RPC_TIMER_CALL_CONTRACT_METADATA
    )
    click.echo("    Get Call Get Metadata RPC Calls: {:,}".format(len(get_metadata_batches)))
    click.echo(
        "    Get Call Get Metadata RPC Total Processing Time: {}".format(
            _get_time_from_secs(sum(get_metadata_batches)) if get_metadata_batches else "n/a"
        )
    )
    click.echo(
        "    Get Call Get Metadata RPC Median Processing Time: {}".format(
            _get_time_from_secs(median(get_metadata_batches)) if get_metadata_batches else "n/a"
        )
    )
    click.echo(
        "    Get Call Get Metadata RPC Mean Processing Time: {}".format(
            _get_time_from_secs(mean(get_metadata_batches)) if get_metadata_batches else "n/a"
        )
    )

    click.echo()
    click.echo("Stored Collections:")
    collections_stored = stats_service.get_count(CollectionPersistenceProcessor.PROCESSED_STAT)
    click.echo("  Total Collections Stored: {:,}".format(collections_stored))
    dynamodb_collections_write_batches = stats_service.get_timings(
        CollectionPersistenceProcessor.DYNAMODB_TIMER_WRITE_CONTRACTS
    )
    dynamodb_collections_write_batches_count = len(dynamodb_collections_write_batches)
    click.echo(
        "  DynamoDB Collections Batch Put Calls: {:,}".format(
            len(dynamodb_collections_write_batches)
        )
    )
    dynamodb_write_batches_average_size = (
        collections_stored / dynamodb_collections_write_batches_count
        if dynamodb_collections_write_batches_count
        else 0.0
    )
    click.echo(
        "  DynamoDB Collections Batch Put Avg Batch Size: {:0.2f}".format(
            dynamodb_write_batches_average_size
        )
    )
    click.echo(
        "  DynamoDB Collections Batch Put Total Processing Time: {}".format(
            _get_time_from_secs(sum(dynamodb_collections_write_batches))
            if len(dynamodb_collections_write_batches)
            else "n/a"
        )
    )
    click.echo(
        "  DynamoDB Collections Batch Put Median Processing Time: {}".format(
            _get_time_from_secs(median(dynamodb_collections_write_batches))
            if len(dynamodb_collections_write_batches)
            else "n/a"
        )
    )
    click.echo(
        "  DynamoDB Collections Batch Put Mean Processing Time: {}".format(
            _get_time_from_secs(mean(dynamodb_collections_write_batches))
            if len(dynamodb_collections_write_batches)
            else "n/a"
        )
    )

    click.echo()
    click.echo("Stored Token Transfers:")
    token_transfers_stored = stats_service.get_count(TokenTransferProcessor.PROCESSED_STAT)
    click.echo("  Total Token Transfers Stored: {:,}".format(token_transfers_stored))
    token_transfer_decode_errors = stats_service.get_count(
        TokenTransferProcessor.TOPICS_DECODE_ERRORS
    )
    click.echo("  Total Token Transfer Decode Errors: {:,}".format(token_transfer_decode_errors))
    token_transfer_decode_error_rate = (
        token_transfers_stored / token_transfer_decode_errors if token_transfer_decode_errors else 0
    )
    click.echo(
        "  Total Token Transfer Error Rate: {:0.2f}".format(token_transfer_decode_error_rate)
    )
    dynamodb_write_token_transfer_batches = stats_service.get_timings(
        TokenTransferProcessor.DYNAMODB_TIMER_WRITE_TOKEN_TRANSFERS
    )
    dynamodb_write_token_transfer_batches_count = len(dynamodb_write_token_transfer_batches)
    click.echo(
        "  Get DynamoDB TokenTransfers Batch Put Calls: {:,}".format(
            len(dynamodb_write_token_transfer_batches)
        )
    )
    dynamodb_write_batches_average_size = (
        token_transfers_stored / dynamodb_write_token_transfer_batches_count
        if dynamodb_write_token_transfer_batches_count
        else 0.0
    )
    click.echo(
        "  DynamoDB TokenTransfers Batch Put Avg Batch Size: {:0.2f}".format(
            dynamodb_write_batches_average_size
        )
    )
    click.echo(
        "  DynamoDB TokenTransfers Batch Put Total Processing Time: {}".format(
            _get_time_from_secs(sum(dynamodb_write_token_transfer_batches))
            if len(dynamodb_write_token_transfer_batches)
            else "n/a"
        )
    )
    click.echo(
        "  DynamoDB TokenTransfers Batch Put Median Processing Time: {}".format(
            _get_time_from_secs(median(dynamodb_write_token_transfer_batches))
            if len(dynamodb_write_token_transfer_batches)
            else "n/a"
        )
    )
    click.echo(
        "  DynamoDB TokenTransfers Batch Put Mean Processing Time: {}".format(
            _get_time_from_secs(mean(dynamodb_write_token_transfer_batches))
            if len(dynamodb_write_token_transfer_batches)
            else "n/a"
        )
    )

    click.echo()
    click.echo("Token Metadata:")
    token_metadata_processed = stats_service.get_count(TokenMetadataProcessor.PROCESSED_STAT)
    click.echo("  Total Tokens Metadata Processed: {:,}".format(token_metadata_processed))
    token_metadata_invalid_token_id = stats_service.get_count(
        TokenMetadataProcessor.TOKEN_METADATA_INVALID_TOKEN_ID
    )
    click.echo(
        "  Tokens Metadata Invalid Token ID Errors: {:,}".format(token_metadata_invalid_token_id)
    )
    token_metadata_invalid_token_id_error_rate = (
        token_metadata_processed / token_metadata_invalid_token_id
        if token_metadata_invalid_token_id
        else 0
    )
    click.echo(
        "  Tokens Metadata Invalid Token ID Error Rate: {:0.2f}".format(
            token_metadata_invalid_token_id_error_rate
        )
    )
    rpc_get_token_metadata_uri_batches = stats_service.get_timings(
        TokenMetadataProcessor.RPC_GET_TOKEN_METADATA_URI
    )
    rpc_get_token_metadata_uri_batches_count = len(rpc_get_token_metadata_uri_batches)
    click.echo(
        "  RPC Calls to Get Metadata URI: {:,}".format(rpc_get_token_metadata_uri_batches_count)
    )
    dynamodb_write_tokens_batches_average_size = (
        token_metadata_processed / rpc_get_token_metadata_uri_batches_count
        if rpc_get_token_metadata_uri_batches_count
        else 0.0
    )
    click.echo(
        "  RPC Calls to Get Metadata URI Avg Batch Size: {:0.2f}".format(
            dynamodb_write_tokens_batches_average_size
        )
    )
    click.echo(
        "  RPC Calls to Get Metadata URI Total Processing Time: {}".format(
            _get_time_from_secs(sum(rpc_get_token_metadata_uri_batches))
            if len(rpc_get_token_metadata_uri_batches)
            else "n/a"
        )
    )
    click.echo(
        "  RPC Calls to Get Metadata URI Mean Processing Time: {}".format(
            _get_time_from_secs(mean(rpc_get_token_metadata_uri_batches))
            if len(rpc_get_token_metadata_uri_batches)
            else "n/a"
        )
    )
    click.echo(
        "  RPC Calls to Get Metadata URI Median Processing Time: {}".format(
            _get_time_from_secs(median(rpc_get_token_metadata_uri_batches))
            if len(rpc_get_token_metadata_uri_batches)
            else "n/a"
        )
    )

    click.echo()
    click.echo("Token Metadata Gather:")
    token_metadata_gather_processed = stats_service.get_count(
        TokenMetadataGatheringProcessor.PROCESSED_STAT
    )
    click.echo(
        "  Total Tokens Metadata Gather Processed: {:,}".format(token_metadata_gather_processed)
    )
    metadata_retrieval_http_timers = stats_service.get_timings(
        TokenMetadataGatheringProcessor.METADATA_RETRIEVAL_HTTP_TIMER
    )
    metadata_retrieval_http_timers_count = len(metadata_retrieval_http_timers)
    metadata_retrieval_http_errors = stats_service.get_count(
        TokenMetadataGatheringProcessor.METADATA_RETRIEVAL_HTTP_ERROR_STAT
    )
    metadata_retrieval_http_error_rate = (
        metadata_retrieval_http_errors / metadata_retrieval_http_timers_count
        if metadata_retrieval_http_timers_count
        else 0
    )
    metadata_retrieval_http_batches_average_size = (
        token_metadata_gather_processed / metadata_retrieval_http_timers_count
        if metadata_retrieval_http_timers_count
        else 0.0
    )
    click.echo("  HTTP Gather Calls:")
    click.echo("    Total Calls: {:,}".format(len(metadata_retrieval_http_timers)))
    click.echo("    Total Errors: {:,}".format(metadata_retrieval_http_errors))
    click.echo("    Error Rate: {:0.2f}".format(metadata_retrieval_http_error_rate))
    click.echo("    Avg Batch Size: {:0.2f}".format(metadata_retrieval_http_batches_average_size))
    click.echo(
        "    Total Processing Time: {}".format(
            _get_time_from_secs(sum(metadata_retrieval_http_timers))
            if len(metadata_retrieval_http_timers)
            else "n/a"
        )
    )
    click.echo(
        "    Median Batch Processing Time: {}".format(
            _get_time_from_secs(median(metadata_retrieval_http_timers))
            if len(metadata_retrieval_http_timers)
            else "n/a"
        )
    )
    click.echo(
        "    Mean Batch Processing Time: {}".format(
            _get_time_from_secs(mean(metadata_retrieval_http_timers))
            if len(metadata_retrieval_http_timers)
            else "n/a"
        )
    )
    metadata_retrieval_ipfs_timers = stats_service.get_timings(
        TokenMetadataGatheringProcessor.METADATA_RETRIEVAL_IPFS_TIMER
    )
    metadata_retrieval_ipfs_timers_count = len(metadata_retrieval_ipfs_timers)
    metadata_retrieval_ipfs_errors = stats_service.get_count(
        TokenMetadataGatheringProcessor.METADATA_RETRIEVAL_HTTP_ERROR_STAT
    )
    metadata_retrieval_ipfs_error_rate = (
        metadata_retrieval_ipfs_errors / metadata_retrieval_ipfs_timers_count
        if metadata_retrieval_ipfs_timers_count
        else 0
    )
    metadata_retrieval_ipfs_batches_average_size = (
        token_metadata_gather_processed / metadata_retrieval_ipfs_timers_count
        if metadata_retrieval_ipfs_timers_count
        else 0.0
    )
    click.echo("  IPFS Gather Calls:")
    click.echo("    Total Calls: {:,}".format(len(metadata_retrieval_ipfs_timers)))
    click.echo("    Total Errors: {:,}".format(metadata_retrieval_ipfs_errors))
    click.echo("    Error Rate: {:0.2f}".format(metadata_retrieval_ipfs_error_rate))
    click.echo("    Avg Batch Size: {:0.2f}".format(metadata_retrieval_ipfs_batches_average_size))
    click.echo(
        "    Total Processing Time: {}".format(
            _get_time_from_secs(sum(metadata_retrieval_ipfs_timers))
            if len(metadata_retrieval_ipfs_timers)
            else "n/a"
        )
    )
    click.echo(
        "    Median Batch Processing Time: {}".format(
            _get_time_from_secs(median(metadata_retrieval_ipfs_timers))
            if len(metadata_retrieval_ipfs_timers)
            else "n/a"
        )
    )
    click.echo(
        "    Mean Batch Processing Time: {}".format(
            _get_time_from_secs(mean(metadata_retrieval_ipfs_timers))
            if len(metadata_retrieval_ipfs_timers)
            else "n/a"
        )
    )
    metadata_retrieval_arweave_timers = stats_service.get_timings(
        TokenMetadataGatheringProcessor.METADATA_RETRIEVAL_ARWEAVE_TIMER
    )
    metadata_retrieval_arweave_timers_count = len(metadata_retrieval_arweave_timers)
    metadata_retrieval_arweave_errors = stats_service.get_count(
        TokenMetadataGatheringProcessor.METADATA_RETRIEVAL_HTTP_ERROR_STAT
    )
    metadata_retrieval_arweave_error_rate = (
        metadata_retrieval_arweave_errors / metadata_retrieval_arweave_timers_count
        if metadata_retrieval_arweave_timers_count
        else 0
    )
    metadata_retrieval_arweave_batches_average_size = (
        token_metadata_gather_processed / metadata_retrieval_arweave_timers_count
        if metadata_retrieval_arweave_timers_count
        else 0.0
    )
    click.echo("  Arweave Gather Calls:")
    click.echo("    Total Calls: {:,}".format(len(metadata_retrieval_arweave_timers)))
    click.echo("    Total Errors: {:,}".format(metadata_retrieval_arweave_errors))
    click.echo("    Error Rate: {:0.2f}".format(metadata_retrieval_arweave_error_rate))
    click.echo(
        "    Avg Batch Size: {:0.2f}".format(metadata_retrieval_arweave_batches_average_size)
    )
    click.echo(
        "    Total Processing Time: {}".format(
            _get_time_from_secs(sum(metadata_retrieval_arweave_timers))
            if len(metadata_retrieval_arweave_timers)
            else "n/a"
        )
    )
    click.echo(
        "    Median Batch Processing Time: {}".format(
            _get_time_from_secs(median(metadata_retrieval_arweave_timers))
            if len(metadata_retrieval_arweave_timers)
            else "n/a"
        )
    )
    click.echo(
        "    Mean Batch Processing Time: {}".format(
            _get_time_from_secs(mean(metadata_retrieval_arweave_timers))
            if len(metadata_retrieval_arweave_timers)
            else "n/a"
        )
    )
    metadata_retrieval_unsupported = stats_service.get_count(
        TokenMetadataGatheringProcessor.METADATA_RETRIEVAL_UNSUPPORTED_PROTOCOL_STAT
    )
    click.echo("  Total Unsupported URIs: {:,}".format(metadata_retrieval_unsupported))

    click.echo()
    click.echo("Stored Tokens:")
    tokens_stored = stats_service.get_count(TokenPersistenceProcessor.PROCESSED_STAT)
    click.echo("  Total Tokens Stored: {:,}".format(tokens_stored))
    dynamodb_write_tokens_batches = stats_service.get_timings(
        TokenPersistenceProcessor.DYNAMODB_TIMER_WRITE_TOKENS
    )
    dynamodb_write_tokens_batches_count = len(dynamodb_write_tokens_batches)
    click.echo(
        "  Get DynamoDB Tokens Batch Put Calls: {:,}".format(dynamodb_write_tokens_batches_count)
    )
    dynamodb_write_tokens_batches_average_size = (
        tokens_stored / dynamodb_write_tokens_batches_count
        if dynamodb_write_tokens_batches_count
        else 0.0
    )
    click.echo(
        "  DynamoDB Tokens Batch Put Avg Batch Size: {:0.2f}".format(
            dynamodb_write_tokens_batches_average_size
        )
    )
    click.echo(
        "  DynamoDB Tokens Batch Put Total Processing Time: {}".format(
            _get_time_from_secs(sum(dynamodb_write_tokens_batches))
            if len(dynamodb_write_tokens_batches)
            else "n/a"
        )
    )
    click.echo(
        "  DynamoDB Tokens Batch Put Median Processing Time: {}".format(
            _get_time_from_secs(median(dynamodb_write_tokens_batches))
            if len(dynamodb_write_tokens_batches)
            else "n/a"
        )
    )
    click.echo(
        "  DynamoDB Tokens Batch Put Mean Processing Time: {}".format(
            _get_time_from_secs(mean(dynamodb_write_tokens_batches))
            if len(dynamodb_write_tokens_batches)
            else "n/a"
        )
    )


if __name__ == "__main__":
    process_contracts()
