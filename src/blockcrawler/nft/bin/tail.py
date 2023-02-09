import asyncio

import aioboto3
import click

from blockcrawler.nft.bin import Config
from blockcrawler.nft.bin.commands import listen_for_and_process_new_evm_blocks


@click.command()
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
@click.pass_obj
def tail(
    config: Config,
    trail_blocks: int,
    process_interval: int,
):
    """
    Process new blocks in the blockchain
    Listen for incoming blocks in the blockchain, parse the data we want to collect
    and store that data in the database
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            listen_for_and_process_new_evm_blocks(
                logger=config.logger,
                stats_service=config.stats_service,
                evm_rpc_client=config.evm_rpc_client,
                boto3_session=aioboto3.Session(),
                blockchain=config.blockchain,
                dynamodb_endpoint_url=config.dynamodb_endpoint_url,
                dynamodb_timeout=config.dynamodb_timeout,
                table_prefix=config.table_prefix,
                trail_blocks=trail_blocks,
                process_interval=process_interval,
            )
        )
    except KeyboardInterrupt:
        pass
