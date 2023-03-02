import asyncio

import click

from blockcrawler.core.stats import StatsService
from blockcrawler.core.types import HexInt, Address
from blockcrawler.evm.rpc import EvmRpcClient, ConnectionPoolingEvmRpcClient, EthCall
from blockcrawler.evm.types import Erc721Functions, Erc721Events


async def run(rpc_uri, quantity):
    stats_service = StatsService()
    rpc_clients = [EvmRpcClient(rpc_uri, stats_service) for _ in range(quantity)]
    async with ConnectionPoolingEvmRpcClient(rpc_clients) as rpc_client:
        block_height = await rpc_client.get_block_number()

        block = await rpc_client.get_block(HexInt(6_000_000), False)

        if block.transaction_hashes:
            transaction_receipt = await rpc_client.get_transaction_receipt(
                block.transaction_hashes[0]
            )

        (owner,) = await rpc_client.call(
            EthCall(
                from_=None,
                to=Address("0x6ebeaf8e8e946f0716e6533a6f2cefc83f60e8ab"),
                function=Erc721Functions.OWNER_OF_TOKEN,
                parameters=[1],
                block="latest",
            )
        )

        logs = []
        async for log in rpc_client.get_logs(
            address=Address("0x6ebeaf8e8e946f0716e6533a6f2cefc83f60e8ab"),
            topics=[Erc721Events.TRANSFER.event_signature_hash.hex()],
            from_block=HexInt(6_000_000),
            to_block=HexInt(6_000_100),
        ):
            logs.append(log)

        print("Block Height:", block_height)
        print("Block 6,000,000:", block)
        print("Transaction Receipt:", transaction_receipt)
        print("Owner:", owner)
        print("Logs:", logs)


@click.command()
@click.argument("RPC_URI")
@click.argument("QUANTITY", type=int)
def command(rpc_uri: str, quantity: int):
    """
    Provide the RPC URI for Websockets and the number of clients to process the example
    """
    asyncio.run(run(rpc_uri, quantity))


if __name__ == "__main__":
    command()
