import asyncio

import aioboto3
import click

from blockcrawler.core.click import HexIntParamType
from blockcrawler.core.types import HexInt
from blockcrawler.nft.bin.shared import (
    Config,
    _update_latest_block,
)


@click.command()
@click.argument("LAST_BLOCK_ID", type=HexIntParamType())
@click.pass_obj
def seed(config: Config, last_block_id: HexInt):
    """
    Set the LAST_BLOCK_ID processed for the blockchain in the database. The
    listen command will use ths value to process blocks after this bock.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        _update_latest_block(
            aioboto3.Session(),
            config.blockchain.value,
            last_block_id,
            config.dynamodb_endpoint_url,
            config.table_prefix,
        )
    )
