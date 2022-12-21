import asyncio

from blockrail.blockcrawler.core.entities import BlockChain, HexInt
from blockrail.blockcrawler.core.bus import Producer, DataBus
from blockrail.blockcrawler.evm.data_packages import EvmBlockIDDataPackage


class BlockIDProducer(Producer):
    """
    Producer for placing Block ID Packages on the data bus
    """

    def __init__(
        self, blockchain: BlockChain, starting_block: int, ending_block: int, step: int = 1
    ) -> None:
        self.__block_chain = blockchain
        self.__block_range = range(starting_block, ending_block + step, step)

    async def __call__(self, data_bus: DataBus):
        for block_id in self.__block_range:
            await data_bus.send(
                EvmBlockIDDataPackage(
                    self.__block_chain,
                    HexInt(block_id),
                ),
            )
            await asyncio.sleep(0)
