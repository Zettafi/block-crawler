from typing import Dict

from blockrail.blockcrawler.core.entities import HexInt
from blockrail.blockcrawler.evm.rpc import EvmRpcClient


class BlockTimeService:
    """
    Service for caching block times
    """

    def __init__(self, rpc_client: EvmRpcClient) -> None:
        self.__rpc_client = rpc_client
        self.__block_timestamps: Dict[int, int] = dict()

    async def set_block_timestamp(self, block_id: HexInt, timestamp: HexInt):
        self.__block_timestamps[block_id.int_value] = timestamp.int_value

    async def get_block_timestamp(self, block_id: HexInt) -> HexInt:
        """
        Gets a previously set timestamp for a block.
        """
        try:
            timestamp = HexInt(self.__block_timestamps[block_id.int_value])
        except KeyError:
            block = await self.__rpc_client.get_block(block_id)
            timestamp = block.timestamp
            self.__block_timestamps[block_id.int_value] = timestamp.int_value

        return timestamp
