import asyncio

from blockcrawler.core.bus import ParallelDataBus, DebugConsumer
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.types import HexInt
from blockcrawler.evm.producers import BlockIDProducer


async def run():
    async with ParallelDataBus() as data_bus:
        await data_bus.register(DebugConsumer())

        block_id_producer = BlockIDProducer(
            BlockChain.ETHEREUM_MAINNET,
            HexInt(0x0),
            HexInt(0x9),
            1,
        )
        await block_id_producer(data_bus)


if __name__ == "__main__":
    asyncio.run(run())
