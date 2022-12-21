from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, call, Mock

from blockrail.blockcrawler.core.entities import BlockChain, HexInt
from blockrail.blockcrawler.core.bus import DataBus
from blockrail.blockcrawler.evm.producers import BlockIDProducer
from blockrail.blockcrawler.evm.data_packages import EvmBlockIDDataPackage


class BlockIDProducerTestCase(IsolatedAsyncioTestCase):
    @staticmethod
    async def test_produces_range_of_block_ids_and_places_each_on_bus_in_order():
        data_bus = AsyncMock(DataBus)
        blockchain = Mock(BlockChain)
        producer = BlockIDProducer(blockchain, 1, 5)
        await producer(data_bus)
        data_bus.send.assert_has_awaits(
            (
                call(EvmBlockIDDataPackage(blockchain, HexInt(1))),
                call(EvmBlockIDDataPackage(blockchain, HexInt(2))),
                call(EvmBlockIDDataPackage(blockchain, HexInt(3))),
                call(EvmBlockIDDataPackage(blockchain, HexInt(4))),
                call(EvmBlockIDDataPackage(blockchain, HexInt(5))),
            )
        )
