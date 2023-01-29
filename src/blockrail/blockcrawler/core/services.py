import abc
from typing import Dict, Union, Optional

from blockrail.blockcrawler.core.entities import HexInt
from blockrail.blockcrawler.evm.rpc import EvmRpcClient


class BlockTimeCache(abc.ABC):
    @abc.abstractmethod
    async def set(self, block_id: int, timestamp: int):
        raise NotImplementedError

    @abc.abstractmethod
    async def get(self, block_id: int) -> Optional[int]:
        raise NotImplementedError


class MemoryBlockTimeCache(BlockTimeCache):
    def __init__(self) -> None:
        self.__block_timestamps: Dict[int, int] = {}

    async def set(self, block_id: int, timestamp: int):
        self.set_sync(block_id, timestamp)

    async def get(self, block_id: int) -> Optional[int]:
        """
        Gets a previously set timestamp for a block.
        """
        try:
            timestamp = self.__block_timestamps[block_id]
        except KeyError:
            timestamp = None

        return timestamp

    def set_sync(self, block_id: int, timestamp: int):
        self.__block_timestamps[block_id] = timestamp

    def __iter__(self):
        for block_id, timestamp in self.__block_timestamps.items():
            yield block_id, timestamp


class BlockTimeService:
    """
    Service for caching block times
    """

    def __init__(self, cache: BlockTimeCache, rpc_client: EvmRpcClient) -> None:
        self.__cache = cache
        self.__rpc_client = rpc_client

    async def set_block_timestamp(
        self, block_id: Union[HexInt, int], timestamp: Union[HexInt, int]
    ):
        block_id_int = block_id.int_value if isinstance(block_id, HexInt) else block_id
        timestamp_int = timestamp.int_value if isinstance(timestamp, HexInt) else timestamp
        await self.__cache.set(block_id_int, timestamp_int)

    async def get_block_timestamp(self, block_id: HexInt) -> HexInt:
        """
        Gets a previously set timestamp for a block.
        """
        block_id_int = block_id.int_value
        timestamp_int = await self.__cache.get(block_id_int)
        if timestamp_int is None:
            block = await self.__rpc_client.get_block(block_id)
            timestamp = block.timestamp
            await self.__cache.set(block_id_int, block.timestamp.int_value)
        else:
            timestamp = HexInt(timestamp_int)

        return timestamp
