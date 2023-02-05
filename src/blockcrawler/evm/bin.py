import asyncio

import click
from eth_hash.auto import keccak

from blockcrawler.core.stats import StatsService
from blockcrawler.evm.rpc import EvmRpcClient


@click.group
def evm():
    """
    Tools for EVM blockchains
    """
    pass


@evm.command()
@click.argument("FUNCTION_ABI")
@click.option("--log-topic", default=False, is_flag=True)
def function_digest(function_abi, log_topic: bool):
    """
    Helper function to return the hash value needed to identify the contract
    method for an `eth_call` RPC call to an EVM node

    Example:

        function-digest "supportsInterface(bytes4)"

        Should return the value 0x01ffc9a7

    """
    sig = "0x" + keccak(function_abi.encode()).hex()
    retval = sig if log_topic else sig[:8]
    click.echo(retval)


@evm.command()
@click.argument(
    "ARCHIVE_NODE",
    envvar="EVM_ARCHIVE_NODE",
    required=True,
)
def block_number(archive_node):
    """
    Get the current block from ARCHIVE_NODE.

    Environment variable EVM_ARCHIVE_NODE may be used for argument ARCHIVE_NODE
    """
    stats_service = StatsService()
    current_block_number = asyncio.run(get_block(archive_node, stats_service))
    click.echo(current_block_number)


async def get_block(evm_node_uri, stats_service: StatsService):
    rpc = EvmRpcClient(evm_node_uri, stats_service)
    async with rpc:
        block_number_ = await rpc.get_block_number()
    return block_number_.int_value


if __name__ == "__main__":
    evm()
