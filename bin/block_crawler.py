import asyncio
import sys
from datetime import datetime
from logging import Logger
from logging import StreamHandler

import click

from chainconductor.blockcrawler.commands import (
    crawl_evm_blocks,
    listen_for_and_process_new_evm_blocks,
    set_last_block_id_for_block_chain,
)
from chainconductor.blockcrawler.stats import StatsService
from chainconductor.blockcrawler.stats_writer import StatsWriter

try:  # If dotenv in installed, use it load env vars
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


@click.group
def block_crawler():
    pass


@block_crawler.command()
@click.argument("BLOCKCHAIN")
@click.argument("STARTING_BLOCK", type=int)
@click.argument("ENDING_BLOCK", type=int)
@click.option(
    "--rpc-batch-size",
    envvar="RPC_BATCH_SIZE",
    default=100,
    show_default=True,
    help="Batch size for JSON-RPC calls",
)
@click.option(
    "--dynamodb-batch-size",
    envvar="DYNAMODB_BATCH_SIZE",
    default=25,
    show_default=True,
    help="Batch size DynamoDB calls",
)
@click.option(
    "--http-batch-size",
    envvar="HTTP_BATCH_SIZE",
    default=10,
    show_default=True,
    help="Maximum concurrent HTTP calls per batch processor",
)
@click.option(
    "--max-batch-wait-time",
    envvar="MAX_BATCH_WAIT_TIME",
    default=30,
    show_default=True,
    help="Maximum time in seconds to wait for batch size to be reached before processing batch",
)
@click.option(
    "--evm-archive-node-uri",
    envvar="EVM_ARCHIVE_NODE_URI",
    help="URI to access the archive node Ethereum RPC HTTP server",
)
@click.option(
    "--dynamodb-uri",
    envvar="DYNAMODB_URI",
    help="Override URI for connecting to DynamoDB",
)
@click.option(
    "--dynamodb-timeout",
    envvar="DYNAMODB_TIMEOUT",
    default=5.0,
    help="Maximum time in seconds to wait for connect or response from DynamoDB",
)
@click.option(
    "--http-metadata-timeout",
    envvar="HTTP_METADATA_TIMEOUT",
    default=10.0,
    show_default=True,
    help="Maximum time in seconds to wait for response from HTTP server when collecting metadata",
)
@click.option(
    "--ipfs-node-uri",
    envvar="IPFS_NODE_URI",
    help="URI for IPFS requests to obtain token metadata",
)
@click.option(
    "--ipfs-metadata-timeout",
    envvar="IPFS_METADATA_TIMEOUT",
    default=60.0,
    show_default=True,
    help="Maximum time in seconds to wait for response from IPFS node when collecting metadata",
)
@click.option(
    "--arweave-node-uri",
    envvar="ARWEAVE_NODE_URI",
    help="URI for Arweave requests to obtain token metadata",
)
@click.option(
    "--arweave-metadata-timeout",
    envvar="ARWEAVE_METADATA_TIMEOUT",
    default=10.0,
    show_default=True,
    help="Maximum time in seconds to wait for response from Arweave node when collecting metadata",
)
@click.option(
    "--block-processors",
    envvar="BLOCK_PROCESSORS",
    default=5,
    show_default=True,
    help="Maximum number of parallel block processors to run",
)
@click.option(
    "--transaction-processors",
    envvar="TRANSACTION_PROCESSORS",
    default=20,
    show_default=True,
    help="Maximum number of parallel transaction processors to run",
)
@click.option(
    "--contract-processors",
    envvar="TOKEN_PERSISTENCE_PROCESSORS",
    default=2,
    show_default=True,
    help="Maximum number of parallel contract processors to run",
)
@click.option(
    "--collection-persistence-processors",
    envvar="COLLECTION_PERSISTENCE_PROCESSORS",
    default=2,
    show_default=True,
    help="Maximum number of parallel collection persistence processors to run",
)
@click.option(
    "--token-transfer-persistence-processors",
    envvar="TOKEN_TRANSFER_PERSISTENCE_PROCESSORS",
    default=8,
    show_default=True,
    help="Maximum number of parallel token transfer persistence processors to run",
)
@click.option(
    "--token-metadata-uri-processors",
    envvar="TOKEN_METADATA_URI_PROCESSORS",
    default=5,
    show_default=True,
    help="Maximum number of parallel token metadata URI processors to run",
)
@click.option(
    "--token-metadata-retrieving-processors",
    envvar="TOKEN_METADATA_RETRIEVING_PROCESSORS",
    default=20,
    show_default=True,
    help="Maximum number of parallel token metadata retrieving processors to run",
)
@click.option(
    "--token-persistence-processors",
    envvar="TOKEN_PERSISTENCE_PROCESSORS",
    default=2,
    show_default=True,
    help="Maximum number of parallel token processors to run",
)
def crawl(
    starting_block: int,
    ending_block: int,
    blockchain: str,
    evm_archive_node_uri: str,
    dynamodb_uri: str,
    dynamodb_timeout: float,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    rpc_batch_size: int,
    dynamodb_batch_size: int,
    http_batch_size: int,
    max_batch_wait_time: int,
    block_processors: int,
    transaction_processors: int,
    contract_processors: int,
    collection_persistence_processors: int,
    token_transfer_persistence_processors: int,
    token_metadata_uri_processors: int,
    token_metadata_retrieving_processors: int,
    token_persistence_processors: int,
):
    """
    Crawl blocks and store the data.

    Crawl the block in the BLOCKCHAIN from the STARTING_BLOCK to the
    ENDING_BLOCK , parse the data we want to collect and put that data in the database
    """
    start = datetime.utcnow()
    stats_service = StatsService()
    stats_writer = StatsWriter(stats_service)
    log_handler = StreamHandler(sys.stdout)
    logger = Logger("block_crawler")
    logger.addHandler(log_handler)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stats_writer_task = loop.create_task(stats_writer.status_writer(start, True, True))
    try:
        loop.run_until_complete(
            crawl_evm_blocks(
                stats_service=stats_service,
                logger=logger,
                archive_node_uri=evm_archive_node_uri,
                blockchain=blockchain,
                dynamodb_uri=dynamodb_uri,
                dynamodb_timeout=dynamodb_timeout,
                http_metadata_timeout=http_metadata_timeout,
                ipfs_node_uri=ipfs_node_uri,
                ipfs_metadata_timeout=ipfs_metadata_timeout,
                arweave_node_uri=arweave_node_uri,
                arweave_metadata_timeout=arweave_metadata_timeout,
                starting_block=starting_block,
                ending_block=ending_block,
                rpc_batch_size=rpc_batch_size,
                dynamodb_batch_size=dynamodb_batch_size,
                http_batch_size=http_batch_size,
                max_batch_wait_time=max_batch_wait_time,
                block_processors=block_processors,
                transaction_processors=transaction_processors,
                contract_processors=contract_processors,
                collection_persistence_processors=collection_persistence_processors,
                token_transfer_persistence_processors=token_transfer_persistence_processors,
                token_metadata_uri_processors=token_metadata_uri_processors,
                token_metadata_retrieval_processors=token_metadata_retrieving_processors,
                token_persistence_processors=token_persistence_processors,
            )
        )
        asyncio.gather(stats_writer_task)
    except KeyboardInterrupt:
        pass

    loop.run_until_complete(stats_writer.status_writer(start, False, False))
    loop.run_until_complete(stats_writer.print_statistics())
    loop.stop()


@block_crawler.command()
@click.argument("BLOCKCHAIN")
@click.option(
    "--rpc-batch-size",
    envvar="RPC_BATCH_SIZE",
    default=100,
    show_default=True,
    help="Batch size for JSON-RPC calls",
)
@click.option(
    "--dynamodb-batch-size",
    envvar="DYNAMODB_BATCH_SIZE",
    default=25,
    show_default=True,
    help="Batch size DynamoDB calls",
)
@click.option(
    "--http-batch-size",
    envvar="HTTP_BATCH_SIZE",
    default=10,
    show_default=True,
    help="Maximum concurrent HTTP calls per batch processor",
)
@click.option(
    "--max-batch-wait-time",
    envvar="MAX_BATCH_WAIT_TIME",
    default=30,
    show_default=True,
    help="Maximum time in seconds to wait for batch size to be reached before processing batch",
)
@click.option(
    "--evm-archive-node-uri",
    envvar="EVM_ARCHIVE_NODE_URI",
    help="URI to access the archive node RPC HTTP server",
)
@click.option(
    "--dynamodb-uri",
    envvar="DYNAMODB_URI",
    help="Override URI for connecting to DynamoDB",
)
@click.option(
    "--dynamodb-timeout",
    envvar="DYNAMODB_TIMEOUT",
    default=5.0,
    help="Maximum time in seconds to wait for connect or response from DynamoDB",
)
@click.option(
    "--http-metadata-timeout",
    envvar="HTTP_METADATA_TIMEOUT",
    default=10.0,
    show_default=True,
    help="Maximum time in seconds to wait for response from HTTP server when collecting metadata",
)
@click.option(
    "--ipfs-node-uri",
    envvar="IPFS_NODE_URI",
    help="URI for IPFS requests to obtain token metadata",
)
@click.option(
    "--ipfs-metadata-timeout",
    envvar="IPFS_METADATA_TIMEOUT",
    default=60.0,
    show_default=True,
    help="Maximum time in seconds to wait for response from IPFS node when collecting metadata",
)
@click.option(
    "--arweave-node-uri",
    envvar="ARWEAVE_NODE_URI",
    help="URI for Arweave requests to obtain token metadata",
)
@click.option(
    "--arweave-metadata-timeout",
    envvar="ARWEAVE_METADATA_TIMEOUT",
    default=10.0,
    show_default=True,
    help="Maximum time in seconds to wait for response from Arweave node when collecting metadata",
)
@click.option(
    "--block-processors",
    envvar="BLOCK_PROCESSORS",
    default=5,
    show_default=True,
    help="Maximum number of parallel block processors to run",
)
@click.option(
    "--transaction-processors",
    envvar="TRANSACTION_PROCESSORS",
    default=20,
    show_default=True,
    help="Maximum number of parallel transaction processors to run",
)
@click.option(
    "--contract-processors",
    envvar="TOKEN_PERSISTENCE_PROCESSORS",
    default=2,
    show_default=True,
    help="Maximum number of parallel contract processors to run",
)
@click.option(
    "--collection-persistence-processors",
    envvar="COLLECTION_PERSISTENCE_PROCESSORS",
    default=2,
    show_default=True,
    help="Maximum number of parallel collection persistence processors to run",
)
@click.option(
    "--token-transfer-persistence-processors",
    envvar="TOKEN_TRANSFER_PERSISTENCE_PROCESSORS",
    default=8,
    show_default=True,
    help="Maximum number of parallel token transfer persistence processors to run",
)
@click.option(
    "--token-metadata-uri-processors",
    envvar="TOKEN_METADATA_URI_PROCESSORS",
    default=5,
    show_default=True,
    help="Maximum number of parallel token metadata URI processors to run",
)
@click.option(
    "--token-metadata-retrieving-processors",
    envvar="TOKEN_METADATA_RETRIEVING_PROCESSORS",
    default=20,
    show_default=True,
    help="Maximum number of parallel token metadata retrieving processors to run",
)
@click.option(
    "--token-persistence-processors",
    envvar="TOKEN_PERSISTENCE_PROCESSORS",
    default=2,
    show_default=True,
    help="Maximum number of parallel token processors to run",
)
@click.option(
    "--trail-blocks",
    envvar="TRAIL_BOCKS",
    default=10,
    show_default=True,
    help="Trail the last block by this many blocks.",
)
@click.option(
    "--process-interval",
    envvar="PROCESS_INTERVAL",
    default=10.0,
    show_default=True,
    help="Minimum interval in seconds between block processing actions.",
)
def tail(
    blockchain: str,
    evm_archive_node_uri: str,
    dynamodb_uri: str,
    dynamodb_timeout: float,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    rpc_batch_size: int,
    dynamodb_batch_size: int,
    http_batch_size: int,
    max_batch_wait_time: int,
    block_processors: int,
    transaction_processors: int,
    contract_processors: int,
    collection_persistence_processors: int,
    token_transfer_persistence_processors: int,
    token_metadata_uri_processors: int,
    token_metadata_retrieving_processors: int,
    token_persistence_processors: int,
    trail_blocks: int,
    process_interval: int,
):
    """
    Process new blocks in BLOCKCHAIN
    Listen for incoming blocks in the BLOCKCHAIN, parse the data we want to collect
    and store that data in the database
    """
    stats_service = StatsService()
    log_handler = StreamHandler(sys.stdout)
    logger = Logger("block_crawler")
    logger.addHandler(log_handler)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            listen_for_and_process_new_evm_blocks(
                stats_service=stats_service,
                logger=logger,
                archive_node_uri=evm_archive_node_uri,
                blockchain=blockchain,
                dynamodb_uri=dynamodb_uri,
                dynamodb_timeout=dynamodb_timeout,
                http_metadata_timeout=http_metadata_timeout,
                ipfs_node_uri=ipfs_node_uri,
                ipfs_metadata_timeout=ipfs_metadata_timeout,
                arweave_node_uri=arweave_node_uri,
                arweave_metadata_timeout=arweave_metadata_timeout,
                rpc_batch_size=rpc_batch_size,
                dynamodb_batch_size=dynamodb_batch_size,
                http_batch_size=http_batch_size,
                block_processors=block_processors,
                transaction_processors=transaction_processors,
                contract_processors=contract_processors,
                collection_persistence_processors=collection_persistence_processors,
                token_transfer_persistence_processors=token_transfer_persistence_processors,
                token_metadata_uri_processors=token_metadata_uri_processors,
                token_metadata_retrieval_processors=token_metadata_retrieving_processors,
                token_persistence_processors=token_persistence_processors,
                trail_blocks=trail_blocks,
                process_interval=process_interval,
            )
        )
    except KeyboardInterrupt:
        pass


@block_crawler.command()
@click.argument("BLOCKCHAIN")
@click.argument("LAST_BLOCK_ID", type=int)
@click.option(
    "--dynamodb-uri",
    envvar="DYNAMODB_URI",
    help="Override URI for connecting to DynamoDB",
)
def seed(blockchain, last_block_id, dynamodb_uri):
    """
    Set the LAST_BLOCK_ID processed for the BLOCKCHAIN in the database. The
    listen command will use ths value to process blocks after this bock.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        set_last_block_id_for_block_chain(blockchain, last_block_id, dynamodb_uri)
    )


if __name__ == "__main__":
    block_crawler()
