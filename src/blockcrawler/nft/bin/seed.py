import asyncio

import aioboto3
import click

from blockcrawler.core.types import HexInt
from blockcrawler.nft.bin import Config
from blockcrawler.nft.bin.commands import set_last_block_id_for_block_chain


@click.command()
@click.argument("LAST_BLOCK_ID", type=HexInt)
@click.pass_obj
def seed(config: Config, last_block_id: HexInt):
    """
    Set the LAST_BLOCK_ID processed for the blockchain in the database. The
    listen command will use ths value to process blocks after this bock.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        set_last_block_id_for_block_chain(
            aioboto3.Session(),
            config.blockchain.value,
            last_block_id,
            config.dynamodb_endpoint_url,
            config.table_prefix,
        )
    )
