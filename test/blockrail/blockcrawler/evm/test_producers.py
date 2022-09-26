from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, call, Mock

from blockrail.blockcrawler.core.entities import BlockChain, HexInt
from blockrail.blockcrawler.core.bus import DataBus
from blockrail.blockcrawler.evm.producers import BlockIDProducer, EVMBlockIDDataPackage


class BlockIDProducerTestCase(IsolatedAsyncioTestCase):
    @staticmethod
    async def test_produces_range_of_block_ids_and_places_each_on_bus_in_order():
        data_bus = AsyncMock(DataBus)
        blockchain = Mock(BlockChain)
        producer = BlockIDProducer(blockchain, 1, 5)
        await producer(data_bus)
        data_bus.send.assert_has_awaits(
            (
                call(EVMBlockIDDataPackage(blockchain, HexInt(1))),
                call(EVMBlockIDDataPackage(blockchain, HexInt(2))),
                call(EVMBlockIDDataPackage(blockchain, HexInt(3))),
                call(EVMBlockIDDataPackage(blockchain, HexInt(4))),
                call(EVMBlockIDDataPackage(blockchain, HexInt(5))),
            )
        )
