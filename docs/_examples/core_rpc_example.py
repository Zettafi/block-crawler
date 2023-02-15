import asyncio

from blockcrawler.core.rpc import RpcClient
from blockcrawler.core.stats import StatsService


async def run():
    stats_service = StatsService()
    async with RpcClient(
        # Replace with actual RPC WSS endpoint
        provider_url="wss://some.rpc.provider",
        stats_service=stats_service,
        requests_per_second=100,
    ) as rpc_client:
        block_number = await rpc_client.send("eth_blockNumber")
        print("Block Number", int(block_number, 16))
        print("RPC Calls:", stats_service.get_count(RpcClient.STAT_REQUEST_SENT))
        print("RPC Call ms:", stats_service.get_count(RpcClient.STAT_REQUEST_MS))


if __name__ == "__main__":
    asyncio.run(run())
