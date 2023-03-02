import asyncio
import pathlib

import click

from blockcrawler.evm.services import MemoryBlockTimeCache
from blockcrawler.evm.util import BlockTimeCacheManager


async def run(file: pathlib.Path):
    block_time_cache = MemoryBlockTimeCache()
    await block_time_cache.set(1, 12345)
    await block_time_cache.set(2, 23456)
    await block_time_cache.set(3, 34567)

    cache_manager = BlockTimeCacheManager(file)
    cache_manager.write_block_times_to_cache(
        [(block, time) for block, time in block_time_cache],
    )

    print("File Contents:")
    with open(file) as f:
        print(f.read())

    cache_manager = BlockTimeCacheManager(file)
    block_time_cache = MemoryBlockTimeCache()
    for block, time in cache_manager.get_block_times_from_cache():
        block_time_cache.set_sync(block, time)
    print("Block 1 Time:", await block_time_cache.get(1))


@click.command()
@click.argument(
    "FILE",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False, path_type=pathlib.Path),
)
def command(file: pathlib.Path):
    """
    Provide the file patch for the CSV file
    """
    asyncio.run(run(file))


if __name__ == "__main__":
    command()
