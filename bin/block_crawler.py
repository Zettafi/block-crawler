import asyncio
import logging
import sys
from logging import Logger
from logging import StreamHandler
from typing import Optional

import click

from blockrail.blockcrawler.core.click import BlockChainParamType
from blockrail.blockcrawler.core.entities import BlockChain
from blockrail.blockcrawler.nft.commands import (
    crawl_evm_blocks,
    listen_for_and_process_new_evm_blocks,
    set_last_block_id_for_block_chain,
    get_block,
)

try:  # If dotenv in installed, use it load env vars
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


@click.group
def block_crawler():
    pass


@block_crawler.command()
@click.argument("STARTING_BLOCK", type=int)
@click.argument("ENDING_BLOCK", type=int)
@click.option(
    "--blockchain",
    envvar="BLOCKCHAIN",
    help="Blockchain that will be processed",
    required=True,
    type=BlockChainParamType(),
)
@click.option(
    "--evm-archive-node-uri",
    envvar="EVM_ARCHIVE_NODE_URI",
    help="URI to access the archive node EVM RPC HTTP server",
)
@click.option(
    "--rpc-requests-per-second",
    envvar="RPC_REQUESTS_PER_SECOND",
    help="The maximum number of requests to process per second",
    default=None,
    type=int,
)
@click.option(
    "--dynamodb-endpoint-url",
    envvar="AWS_DYNAMODB_ENDPOINT_URL",
    help="Override URL for connecting to Amazon DynamoDB",
)
@click.option(
    "--s3-endpoint-url",
    envvar="AWS_S3_ENDPOINT_URL",
    help="Override URL for connecting to Amazon S3",
)
@click.option(
    "--dynamodb-timeout",
    envvar="DYNAMODB_TIMEOUT",
    default=5.0,
    help="Maximum time in seconds to wait for connect or response from DynamoDB",
)
@click.option(
    "--dynamodb-region",
    envvar="AWS_DYNAMODB_REGION",
    help="AWS region for DynamoDB",
)
@click.option("--table-prefix", envvar="TABLE_PREFIX", help="Prefix for table names", default="")
@click.option(
    "--s3-region",
    envvar="AWS_S3_REGION",
    help="AWS region for S3",
)
@click.option(
    "--s3-metadata-bucket",
    envvar="AWS_S3_METADATA_BUCKET",
    default="chain-conductor-metadata",
    help="S3 bucket to store metadata files",
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
    "--increment-data-version",
    envvar="INCREMENT_DATA_VERSION",
    default=True,
    show_default=True,
    help="Increment the data version being processed. This ensures data integrity when "
    "reprocessing the blockchain. If you are running multiple crawlers, set the first "
    'to "True" to increment the version and the rest to "False" to use the version '
    "set by the first crawler.",
)
@click.option(
    "--debug/--no-debug",
    envvar="DEBUG",
    default=False,
    show_default=True,
    help="Show debug messages in the console.",
)
def crawl(
    starting_block: int,
    ending_block: int,
    blockchain: BlockChain,
    evm_archive_node_uri: str,
    rpc_requests_per_second: Optional[int],
    dynamodb_timeout: float,
    dynamodb_endpoint_url: str,
    dynamodb_region: str,
    table_prefix: str,
    s3_endpoint_url: str,
    s3_region: str,
    s3_metadata_bucket: str,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    increment_data_version: bool,
    debug: bool,
):
    """
    Crawl blocks and store the data.

    Crawl the block in the BLOCKCHAIN from the STARTING_BLOCK to the
    ENDING_BLOCK , parse the data we want to collect and put that data in the database
    """
    log_handler = StreamHandler(sys.stdout)
    logger = Logger("block_crawler")
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            crawl_evm_blocks(
                logger=logger,
                archive_node_uri=evm_archive_node_uri,
                rpc_requests_per_second=rpc_requests_per_second,
                blockchain=blockchain,
                dynamodb_endpoint_url=dynamodb_endpoint_url,
                dynamodb_region=dynamodb_region,
                s3_endpoint_url=s3_endpoint_url,
                s3_region=s3_region,
                dynamodb_timeout=dynamodb_timeout,
                table_prefix=table_prefix,
                s3_metadata_bucket=s3_metadata_bucket,
                http_metadata_timeout=http_metadata_timeout,
                ipfs_node_uri=ipfs_node_uri,
                ipfs_metadata_timeout=ipfs_metadata_timeout,
                arweave_node_uri=arweave_node_uri,
                arweave_metadata_timeout=arweave_metadata_timeout,
                starting_block=starting_block,
                ending_block=ending_block,
                increment_data_version=increment_data_version,
            )
        )
    except KeyboardInterrupt:
        pass


@block_crawler.command()
@click.option(
    "--blockchain",
    envvar="BLOCKCHAIN",
    help="Blockchain that will be processed",
    required=True,
    type=BlockChainParamType(),
)
@click.option(
    "--evm-archive-node-uri",
    envvar="EVM_ARCHIVE_NODE_URI",
    help="URI to access the archive node EVM RPC HTTP server",
)
@click.option(
    "--rpc-requests-per-second",
    envvar="RPC_REQUESTS_PER_SECOND",
    help="The maximum number of requests to process per second",
    default=None,
    type=int,
)
@click.option(
    "--dynamodb-endpoint-url",
    envvar="AWS_DYNAMODB_ENDPOINT_URL",
    help="Override URL for connecting to Amazon DYnamoDB",
)
@click.option(
    "--s3-endpoint-url",
    envvar="AWS_S3_ENDPOINT_URL",
    help="Override URL for connecting to Amazon S3",
)
@click.option(
    "--dynamodb-timeout",
    envvar="DYNAMODB_TIMEOUT",
    default=5.0,
    help="Maximum time in seconds to wait for connect or response from DynamoDB",
)
@click.option(
    "--dynamodb-region",
    envvar="AWS_DYNAMODB_REGION",
    help="AWS region for DynamoDB",
)
@click.option("--table-prefix", envvar="TABLE_PREFIX", help="Prefix for table names", default="")
@click.option(
    "--s3-region",
    envvar="AWS_S3_REGION",
    help="AWS region for S3",
)
@click.option(
    "--s3-metadata-bucket",
    envvar="AWS_S3_METADATA_BUCKET",
    default="chain-conductor-metadata",
    help="S3 bucket to store metadata files",
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
    "--trail-blocks",
    envvar="TRAIL_BOCKS",
    default=1,
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
@click.option(
    "--debug/--no-debug",
    envvar="DEBUG",
    default=False,
    show_default=True,
    help="Show debug messages in the console.",
)
def tail(
    evm_archive_node_uri: str,
    rpc_requests_per_second: Optional[int],
    blockchain: BlockChain,
    dynamodb_endpoint_url: str,
    dynamodb_region: str,
    s3_endpoint_url: str,
    s3_region: str,
    dynamodb_timeout: float,
    table_prefix: str,
    s3_metadata_bucket: str,
    http_metadata_timeout: float,
    ipfs_node_uri: str,
    ipfs_metadata_timeout: float,
    arweave_node_uri: str,
    arweave_metadata_timeout: float,
    trail_blocks: int,
    process_interval: int,
    debug: bool,
):
    """
    Process new blocks in the blockchain
    Listen for incoming blocks in the blockchain, parse the data we want to collect
    and store that data in the database
    """
    log_handler = StreamHandler(sys.stdout)
    logger = Logger("block_crawler")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.addHandler(log_handler)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            listen_for_and_process_new_evm_blocks(
                logger=logger,
                archive_node_uri=evm_archive_node_uri,
                rpc_requests_per_second=rpc_requests_per_second,
                blockchain=blockchain,
                dynamodb_endpoint_url=dynamodb_endpoint_url,
                dynamodb_region=dynamodb_region,
                dynamodb_timeout=dynamodb_timeout,
                table_prefix=table_prefix,
                s3_endpoint_url=s3_endpoint_url,
                s3_region=s3_region,
                s3_metadata_bucket=s3_metadata_bucket,
                http_metadata_timeout=http_metadata_timeout,
                ipfs_node_uri=ipfs_node_uri,
                ipfs_metadata_timeout=ipfs_metadata_timeout,
                arweave_node_uri=arweave_node_uri,
                arweave_metadata_timeout=arweave_metadata_timeout,
                trail_blocks=trail_blocks,
                process_interval=process_interval,
            )
        )
    except KeyboardInterrupt:
        pass


@block_crawler.command()
@click.argument("LAST_BLOCK_ID", type=int)
@click.option(
    "--dynamodb-endpoint-url",
    envvar="AWS_DYNAMODB_ENDPOINT_URL",
    help="Override URL for connecting to Amazon Web Services",
)
@click.option(
    "--blockchain",
    envvar="BLOCKCHAIN",
    help="Blockchain that will be processed",
    required=True,
    type=BlockChainParamType(),
)
@click.option("--table-prefix", envvar="TABLE_PREFIX", help="Prefix for table names", default="")
def seed(blockchain, last_block_id, dynamodb_endpoint_url, table_prefix):
    """
    Set the LAST_BLOCK_ID processed for the blockchain in the database. The
    listen command will use ths value to process blocks after this bock.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        set_last_block_id_for_block_chain(
            blockchain.value, last_block_id, dynamodb_endpoint_url, table_prefix
        )
    )


@block_crawler.command()
@click.option(
    "--evm-archive-node-uri",
    envvar="EVM_ARCHIVE_NODE_URI",
    help="URI to access the archive node EVM RPC HTTP server",
)
def block_number(evm_archive_node_uri):
    """
    Get the current block for a blockchain.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    block_number = loop.run_until_complete(get_block(evm_archive_node_uri))
    click.echo(block_number)


if __name__ == "__main__":
    block_crawler()
