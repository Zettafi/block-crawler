import asyncio

import click

from blockcrawler.core.bus import ParallelDataBus, DebugConsumer
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.stats import StatsService
from blockcrawler.core.types import HexInt
from blockcrawler.evm.data_packages import EvmBlockIDDataPackage
from blockcrawler.evm.rpc import EvmRpcClient
from blockcrawler.evm.services import BlockTimeService, MemoryBlockTimeCache
from blockcrawler.evm.transformers import (
    BlockIdToEvmBlockTransformer,
    EvmBlockToEvmTransactionHashTransformer,
    EvmBlockIdToEvmBlockAndEvmTransactionAndEvmTransactionHashTransformer,
    EvmTransactionHashToEvmTransactionReceiptTransformer,
    EvmTransactionReceiptToEvmLogTransformer,
    EvmTransactionToContractEvmTransactionReceiptTransformer,
)


async def run(rpc_uri):
    async with EvmRpcClient(rpc_uri, StatsService()) as rpc_client:
        block_time_service = BlockTimeService(
            MemoryBlockTimeCache(),
            rpc_client,
        )
        async with ParallelDataBus() as data_bus:
            # Path 1 is path used by load and crawl
            await data_bus.register(
                BlockIdToEvmBlockTransformer(
                    data_bus=data_bus,
                    blockchain=BlockChain.ETHEREUM_MAINNET,
                    rpc_client=rpc_client,
                ),
            )
            await data_bus.register(
                EvmBlockToEvmTransactionHashTransformer(data_bus),
            )
            await data_bus.register(
                EvmTransactionHashToEvmTransactionReceiptTransformer(
                    data_bus=data_bus,
                    blockchain=BlockChain.ETHEREUM_MAINNET,
                    rpc_client=rpc_client,
                ),
            )

            # Path 2 is used by load and force
            await data_bus.register(
                EvmBlockIdToEvmBlockAndEvmTransactionAndEvmTransactionHashTransformer(
                    data_bus=data_bus,
                    blockchain=BlockChain.POLYGON_MAINNET,
                    block_time_service=block_time_service,
                    rpc_client=rpc_client,
                ),
            )
            await data_bus.register(
                EvmTransactionToContractEvmTransactionReceiptTransformer(
                    data_bus=data_bus,
                    blockchain=BlockChain.POLYGON_MAINNET,
                    rpc_client=rpc_client,
                ),
            )

            # Agnostic path
            await data_bus.register(
                EvmTransactionReceiptToEvmLogTransformer(data_bus),
            )
            await data_bus.register(DebugConsumer())

            # Send a block down path 1
            await data_bus.send(
                EvmBlockIDDataPackage(
                    BlockChain.ETHEREUM_MAINNET,
                    HexInt(0x100000),
                ),
            )

            # Send a block down path 2
            await data_bus.send(
                EvmBlockIDDataPackage(
                    BlockChain.POLYGON_MAINNET,
                    HexInt(0x100001),
                ),
            )


@click.command()
@click.argument("RPC_URI")
def command(rpc_uri):
    """
    Provide the RPC URI for Websockets to process the example
    """
    asyncio.run(run(rpc_uri))


if __name__ == "__main__":
    command()
