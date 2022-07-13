import asyncio
from asyncio import FIRST_COMPLETED
from datetime import datetime

import click

from chainconductor.contractpuller.commands import process_contracts_async
from chainconductor.contractpuller.stats import StatsService

try:  # If dotenv in installed, use it load env vars
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


async def _stats_writer(stats_service: StatsService, start_time: datetime, run_forever=False):
    while True:
        await asyncio.sleep(1)
        end = datetime.utcnow()
        total_time = end - start_time
        get_blocks_timings = stats_service.get_timings("rpc_get_blocks")
        get_transaction_timings = stats_service.get_timings("rpc_get_transaction_receipts")
        get_dynamodb_timings = stats_service.get_timings("dynamodb_batch_write")
        print(
            "\r{:02d}:{:05.2f}".format(total_time.seconds // 60, total_time.seconds + total_time.microseconds / 1_000_000), ":",
            "B", "{:,}".format(stats_service.get_count(StatsService.BLOCKS)),
            "[", len(get_blocks_timings),
            "|", "{:0.2f}".format(sum(get_blocks_timings) / 1_000_000_000),
            "|", "{:0.2f}".format(sum(get_blocks_timings) / 1_000_000_000 / len(get_blocks_timings) if len(get_blocks_timings) > 0 else 0),
            "]",

            "R", "{:,}".format(stats_service.get_count(StatsService.TRANSACTION_RECEIPTS)),
            "[", len(get_transaction_timings),
            "|", "{:0.2f}".format(sum(get_transaction_timings) / 1_000_000_000),
            "|", "{:0.2f}".format(sum(get_transaction_timings) / len(get_transaction_timings) / 1_000_000_000 if len(get_transaction_timings) > 0 else 0),
            "]",

            "C", "{:,}".format(stats_service.get_count(StatsService.CONTRACTS)),
            "[", len(get_dynamodb_timings),
            "|", "{:0.2f}".format(sum(get_dynamodb_timings) / 1_000_000_000),
            "|", "{:0.2f}".format(sum(get_dynamodb_timings) / len(get_dynamodb_timings) / 1_000_000_000 if len(get_dynamodb_timings) > 0 else 0),
            "]",
            end=""
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
    default=10,
    show_default=True,
    help="Number of parallel contract processors to run",
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
):
    """
    Pull all contracts from the STARTING_BLOCK to the ENDING_BLOCK from an archive node and put them in the database
    """
    start = datetime.utcnow()
    stats_service = StatsService()

    loop = asyncio.get_event_loop()
    loop.create_task(_stats_writer(stats_service, start, True))
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
        )
    )
    loop.run_until_complete(_stats_writer(stats_service, start))


if __name__ == "__main__":
    process_contracts()
