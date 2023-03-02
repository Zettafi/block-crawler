import asyncio

import click

from blockcrawler.core.stats import StatsService
from blockcrawler.core.types import HexInt
from blockcrawler.evm.rpc import EvmRpcClient
from blockcrawler.evm.services import MemoryBlockTimeCache, BlockTimeService


async def run(rpc_uri):
    block_time_cache = MemoryBlockTimeCache()
    await block_time_cache.set(1, 12345)
    block_1_time = await block_time_cache.get(1)
    print("Block 1 Time:", block_1_time)
    block_2_time = await block_time_cache.get(2)
    print("Block 2 Time:", block_2_time)

    stats_service = StatsService()
    async with EvmRpcClient(rpc_uri, stats_service) as rpc_client:
        block_time_service = BlockTimeService(block_time_cache, rpc_client)
        block_1_time = await block_time_service.get_block_timestamp(HexInt(1))
        print("Block 1 Time:", block_1_time.int_value)
        block_2_time = await block_time_service.get_block_timestamp(HexInt(2))
        print("Block 2 Time:", block_2_time.int_value)
        print("Get Block Calls:", stats_service.get_count(EvmRpcClient.STAT_REQUEST_SENT))
        print("Block 2 Time:", block_2_time.int_value)
        print("Get Block Calls:", stats_service.get_count(EvmRpcClient.STAT_REQUEST_SENT))

    print("\nCache Data:")
    for block_id, time in block_time_cache:
        print(f"  Block/Time: {block_id}/{time}")


@click.command()
@click.argument("RPC_URI")
def command(rpc_uri: str):
    """
    Provide the RPC URI for Websockets to process the example
    """
    asyncio.run(run(rpc_uri))


if __name__ == "__main__":
    command()
