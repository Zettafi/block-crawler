import logging
import pathlib
from logging import StreamHandler
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, List, Tuple

import click
from click import Context

from blockcrawler import LOGGER_NAME
from blockcrawler.core.click import BlockChainParamType
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.stats import StatsService
from blockcrawler.evm.rpc import EvmRpcClient, ConnectionPoolingEvmRpcClient
from blockcrawler.nft.bin.shared import Config
from blockcrawler.nft.bin.crawl import crawl
from blockcrawler.nft.bin.force import force_load
from blockcrawler.nft.bin.load import load
from blockcrawler.nft.bin.seed import seed
from blockcrawler.nft.bin.tail import tail
from blockcrawler.nft.bin.verify import verify

try:  # If dotenv in installed, use it load env vars
    # noinspection PyPackageRequirements
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


@click.group
@click.option(
    "--blockchain",
    envvar="BLOCKCHAIN",
    help="Blockchain that will be processed",
    required=True,
    type=BlockChainParamType(),
)
@click.option(
    "--evm-rpc-nodes",
    envvar="EVM_RPC_NODES",
    help="RPC Node URI and the number of connections",
    multiple=True,
    required=True,
    type=click.Tuple([str, int]),
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
    "--dynamodb-timeout",
    envvar="DYNAMODB_TIMEOUT",
    default=5.0,
    show_default=True,
    help="Maximum time in seconds to wait for connect or response from DynamoDB",
)
@click.option(
    "--dynamodb-table-prefix",
    envvar="AWS_DYNAMO_DB_TABLE_PREFIX",
    help="Prefix for table names",
    show_default=True,
    default="",
)
@click.option(
    "--log-file",
    envvar="LOG_FILE",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False, path_type=pathlib.Path),
    multiple=True,
    help="Location and filename for a log.",
)
@click.option(
    "--debug/--no-debug",
    envvar="DEBUG",
    default=False,
    show_default=True,
    help="Show debug messages in the console.",
)
@click.pass_context
def nft(
    ctx: Context,
    blockchain: BlockChain,
    evm_rpc_nodes: List[Tuple[str, int]],
    rpc_requests_per_second: Optional[int],
    dynamodb_timeout: float,
    dynamodb_endpoint_url: str,
    dynamodb_table_prefix: str,
    log_file: List[pathlib.Path],
    debug: bool,
):
    """
    Loading and verifying NFTs
    """
    logger = logging.getLogger(LOGGER_NAME)
    handlers: List[logging.Handler] = [StreamHandler()]
    for filename in log_file:
        handlers.append(TimedRotatingFileHandler(filename, when="D"))
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logging.basicConfig(format="%(asctime)s %(message)s", handlers=handlers)

    stats_service = StatsService()

    rpc_clients: List[EvmRpcClient] = []
    for uri, instances in evm_rpc_nodes:
        for _ in range(instances):
            rpc_clients.append(EvmRpcClient(uri, stats_service, rpc_requests_per_second))
    evm_rpc_client = ConnectionPoolingEvmRpcClient(rpc_clients)

    ctx.obj = Config(
        evm_rpc_client=evm_rpc_client,
        stats_service=stats_service,
        blockchain=blockchain,
        dynamodb_timeout=dynamodb_timeout,
        dynamodb_endpoint_url=dynamodb_endpoint_url,
        table_prefix=dynamodb_table_prefix,
        logger=logger,
    )


nft.add_command(verify)
nft.add_command(load)
nft.add_command(crawl)
nft.add_command(tail)
nft.add_command(seed)
nft.add_command(force_load)

if __name__ == "__main__":
    nft()
