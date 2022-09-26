from dataclasses import dataclass

from blockrail.blockcrawler.core.entities import BlockChain, HexInt
from blockrail.blockcrawler.core.bus import Producer, DataBus, DataPackage


@dataclass
class EVMBlockIDDataPackage(DataPackage):
    """
    Data package for placing Block IDs on the Data Bus
    """

    blockchain: BlockChain
    block_id: HexInt


class BlockIDProducer(Producer):
    """
    Producer for placing Block ID Packages on the data bus
    """

    def __init__(self, blockchain: BlockChain, starting_block: int, ending_block: int) -> None:
        self.__block_chain = blockchain
        self.__block_range = range(starting_block, ending_block + 1)

    async def __call__(self, data_bus: DataBus):
        for block_id in self.__block_range:
            await data_bus.send(
                EVMBlockIDDataPackage(
                    self.__block_chain,
                    HexInt(block_id),
                ),
            )
