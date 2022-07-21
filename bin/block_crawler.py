import asyncio
from datetime import datetime
from math import floor
from statistics import median, mean

import click

from chainconductor.blockcrawler.commands import process_contracts_async
from chainconductor.blockcrawler.processors import (
    BlockProcessor,
    TransactionProcessor,
    ContractPersistenceProcessor,
    ContractProcessor,
    TokenProcessor,
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
            "Total Time  | Blocks      | Transactions | Contracts   | Contract Writes | Token Transfer Writes"
        )

    while True:
        await asyncio.sleep(1)
        end = datetime.utcnow()
        total_time = end - start_time
        get_blocks_timings = stats_service.get_timings(
            BlockProcessor.RPC_TIMER_GET_BLOCKS
        )
        get_transaction_timings = stats_service.get_timings(
            TransactionProcessor.RPC_TIMER_GET_TRANSACTION_RECEIPTS
        )
        get_contract_call_interfaces_timings = stats_service.get_timings(
            ContractProcessor.RPC_TIMER_CALL_SUPPORTS_INTERFACES
        )
        get_contract_call_contract_metadata_timings = stats_service.get_timings(
            ContractProcessor.RPC_TIMER_CALL_CONTRACT_METADATA
        )
        get_dynamodb_timings = stats_service.get_timings(
            ContractPersistenceProcessor.DYNAMODB_TIMER_WRITE_CONTRACT
        )
        seconds = total_time.seconds % 60 + total_time.microseconds / 1_000_000
        minutes = total_time.seconds // 60
        hours = minutes // 60
        print(
            "\r{:02d}:{:02d}:{:05.2f}".format(hours, minutes, seconds),
            "|",
            "{:<11,}".format(stats_service.get_count(BlockProcessor.PROCESSED_STAT)),
            "|",
            "{:<12,}".format(
                stats_service.get_count(TransactionProcessor.PROCESSED_STAT)
            ),
            "|",
            "{:<11,}".format(stats_service.get_count(ContractProcessor.PROCESSED_STAT)),
            "|",
            "{:<15,}".format(
                stats_service.get_count(ContractPersistenceProcessor.PROCESSED_STAT)
            ),
            "|",
            "{:<21,}".format(stats_service.get_count(TokenProcessor.PROCESSED_STAT)),
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
    "--max-batch-wait-time",
    default=10,
    show_default=True,
    help="Maximum time imn seconds to wait for batch size to be reached before processing batch",
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
    "--block-processors",
    default=10,
    show_default=True,
    help="Number of parallel block processors to run",
)
@click.option(
    "--transaction-processors",
    default=10,
    show_default=True,
    help="Number of parallel transaction processors to run",
)
@click.option(
    "--contract-processors",
    default=20,
    show_default=True,
    help="Number of parallel contract processors to run",
)
@click.option(
    "--contract-persistence-processors",
    default=10,
    show_default=True,
    help="Number of parallel contract persistence processors to run",
)
@click.option(
    "--token-processors",
    default=10,
    show_default=True,
    help="Number of parallel token processors to run",
)
def process_contracts(
    starting_block: int,
    ending_block: int,
    archive_node_uri: str,
    dynamodb_uri: str,
    rpc_batch_size: int,
    dynamodb_batch_size: int,
    max_batch_wait_time: int,
    block_processors: int,
    transaction_processors: int,
    contract_processors: int,
    contract_persistence_processors: int,
    token_processors: int,
):
    """
    Crawl the blocks from the STARTING_BLOCK to the ENDING_BLOCK from an archive node, parse the data we want to collect
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
                starting_block=starting_block,
                ending_block=ending_block,
                rpc_batch_size=rpc_batch_size,
                dynamodb_batch_size=dynamodb_batch_size,
                max_batch_wait_time=max_batch_wait_time,
                block_processor_instances=block_processors,
                transaction_processor_instances=transaction_processors,
                contract_processor_instances=contract_processors,
                contract_persistence_processor_instances=contract_persistence_processors,
                token_processor_instances=token_processors,
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
    click.echo(
        "  Get Block RPC Avg Batch Size: {:0.2f}".format(block_batches_average_size)
    )
    click.echo(
        "  Get Block RPC Total Processing Time: {}".format(
            _get_time_from_secs(sum(block_batches))
        )
    )
    click.echo(
        "  Get Block RPC Median Processing Time: {}".format(
            _get_time_from_secs(median(block_batches))
        )
    )
    click.echo(
        "  Get Block RPC Mean Processing Time: {}".format(
            _get_time_from_secs(mean(block_batches))
        )
    )
    click.echo()
    click.echo("Transactions Receipts:")
    transactions_processed = stats_service.get_count(
        TransactionProcessor.PROCESSED_STAT
    )
    click.echo("  Total Processed: {:,}".format(transactions_processed))
    transaction_batches = stats_service.get_timings(
        TransactionProcessor.RPC_TIMER_GET_TRANSACTION_RECEIPTS
    )
    transaction_batches_count = len(transaction_batches)
    transaction_batches_average_size = (
        transactions_processed / transaction_batches_count
        if transaction_batches_count > 0
        else 0.0
    )
    click.echo(
        "  Get Transaction Receipt RPC Calls: {:,}".format(len(transaction_batches))
    )
    click.echo(
        "  Get Transaction Receipt RPC Avg Batch Size: {:0.2f}".format(
            transaction_batches_average_size
        )
    )
    click.echo(
        "  Get Transaction Receipt RPC Total Processing Time: {}".format(
            _get_time_from_secs(sum(transaction_batches))
        )
    )
    click.echo(
        "  Get Transaction Receipt RPC Median Processing Time: {}".format(
            _get_time_from_secs(median(transaction_batches))
            if len(transaction_batches)
            else 0
        )
    )
    click.echo(
        "  Get Transaction Receipt RPC Mean Processing Time: {}".format(
            _get_time_from_secs(mean(transaction_batches))
            if len(transaction_batches)
            else 0
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
        "    Get Call Supports Interface RPC Calls: {:,}".format(
            len(supports_interface_batches)
        )
    )
    click.echo(
        "    Get Call Supports Interface RPC Total Processing Time: {}".format(
            _get_time_from_secs(sum(supports_interface_batches))
        )
    )
    click.echo(
        "    Get Call Supports Interface RPC Median Processing Time: {}".format(
            _get_time_from_secs(median(supports_interface_batches))
            if len(supports_interface_batches)
            else 0
        )
    )
    click.echo(
        "    Get Call Supports Interface RPC Mean Processing Time: {}".format(
            _get_time_from_secs(mean(supports_interface_batches))
            if len(supports_interface_batches)
            else 0
        )
    )
    click.echo("  Contract Metadata:")
    get_metadata_batches = stats_service.get_timings(
        ContractProcessor.RPC_TIMER_CALL_CONTRACT_METADATA
    )
    click.echo(
        "    Get Call Get Metadata RPC Calls: {:,}".format(len(get_metadata_batches))
    )
    click.echo(
        "    Get Call Get Metadata RPC Total Processing Time: {}".format(
            _get_time_from_secs(sum(get_metadata_batches))
        )
    )
    click.echo(
        "    Get Call Get Metadata RPC Median Processing Time: {}".format(
            _get_time_from_secs(median(get_metadata_batches))
            if len(get_metadata_batches)
            else 0
        )
    )
    click.echo(
        "    Get Call Get Metadata RPC Mean Processing Time: {}".format(
            _get_time_from_secs(mean(get_metadata_batches))
            if len(get_metadata_batches)
            else 0
        )
    )
    click.echo()
    click.echo("Stored Token Transfers:")
    token_transfers_stored = stats_service.get_count(TokenProcessor.PROCESSED_STAT)
    click.echo("  Total Token Transfers Stored: {:,}".format(token_transfers_stored))
    dynamodb_write_token_transfer_batches = stats_service.get_timings(
        TokenProcessor.DYNAMODB_TIMER_WRITE_TOKEN_TRANSFER
    )
    dynamodb_write_token_transfer_batches_count = len(
        dynamodb_write_token_transfer_batches
    )
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
        )
    )
    click.echo(
        "  DynamoDB TokenTransfers Batch Put Median Processing Time: {}".format(
            _get_time_from_secs(median(dynamodb_write_token_transfer_batches))
            if len(dynamodb_write_token_transfer_batches)
            else 0
        )
    )
    click.echo(
        "  DynamoDB TokenTransfers Batch Put Mean Processing Time: {}".format(
            _get_time_from_secs(mean(dynamodb_write_token_transfer_batches))
            if len(dynamodb_write_token_transfer_batches)
            else 0
        )
    )
    click.echo()
    click.echo("Stored Contracts:")
    contracts_stored = stats_service.get_count(
        ContractPersistenceProcessor.PROCESSED_STAT
    )
    click.echo("  Total Contracts Stored: {:,}".format(contracts_stored))
    dynamodb_contracts_write_batches = stats_service.get_timings(
        ContractPersistenceProcessor.DYNAMODB_TIMER_WRITE_CONTRACT
    )
    dynamodb_contracts_write_batches_count = len(dynamodb_contracts_write_batches)
    click.echo(
        "  DynamoDB Contracts Batch Put Calls: {:,}".format(
            len(dynamodb_contracts_write_batches)
        )
    )
    dynamodb_write_batches_average_size = (
        contracts_stored / dynamodb_contracts_write_batches_count
        if dynamodb_contracts_write_batches_count
        else 0.0
    )
    click.echo(
        "  DynamoDB Contracts Batch Put Avg Batch Size: {:0.2f}".format(
            dynamodb_write_batches_average_size
        )
    )
    click.echo(
        "  DynamoDB Contracts Batch Put Total Processing Time: {}".format(
            _get_time_from_secs(sum(dynamodb_contracts_write_batches))
        )
    )
    click.echo(
        "  DynamoDB Contracts Batch Put Median Processing Time: {}".format(
            _get_time_from_secs(median(dynamodb_contracts_write_batches))
            if len(dynamodb_contracts_write_batches)
            else 0
        )
    )
    click.echo(
        "  DynamoDB Contracts Batch Put Mean Processing Time: {}".format(
            _get_time_from_secs(mean(dynamodb_contracts_write_batches))
            if len(dynamodb_contracts_write_batches)
            else 0
        )
    )


if __name__ == "__main__":
    process_contracts()
