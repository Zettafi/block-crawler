from typing import Dict, Union

from blockrail.blockcrawler.core.entities import HexInt
from blockrail.blockcrawler.evm.rpc import EvmRpcClient


class BlockTimeService:
    """
    Service for caching block times
    """

    def __init__(self, rpc_client: EvmRpcClient) -> None:
        self.__rpc_client = rpc_client
        self.__block_timestamps: Dict[int, int] = dict()

    async def set_block_timestamp(
        self, block_id: Union[HexInt, int], timestamp: Union[HexInt, int]
    ):
        block_id_int = block_id.int_value if isinstance(block_id, HexInt) else block_id
        timestamp_int = timestamp.int_value if isinstance(timestamp, HexInt) else timestamp
        self.__block_timestamps[block_id_int] = timestamp_int

    async def get_block_timestamp(self, block_id: HexInt) -> HexInt:
        """
        Gets a previously set timestamp for a block.
        """
        try:
            block_id_int = block_id.int_value
            timestamp = HexInt(self.__block_timestamps[block_id_int])
        except KeyError:
            block = await self.__rpc_client.get_block(block_id)
            timestamp = block.timestamp
            await self.set_block_timestamp(block_id_int, block.timestamp)

        return timestamp

    def __iter__(self):
        for block_id, timestamp in self.__block_timestamps.items():
            yield block_id, timestamp
