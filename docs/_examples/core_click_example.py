import click
from hexbytes import HexBytes

from blockcrawler.core.click import (
    BlockChainParamType,
    HexIntParamType,
    AddressParamType,
    HexBytesParamType,
)
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.types import HexInt, Address


@click.command()
@click.argument("BLOCKCHAIN", type=BlockChainParamType())
@click.argument("HEX_INT", type=HexIntParamType())
@click.argument("ADDRESS", type=AddressParamType())
@click.argument("HEX_BYTES", type=HexBytesParamType())
def run(blockchain: BlockChain, hex_int: HexInt, address: Address, hex_bytes: HexBytes):
    print("blockchain:", blockchain)
    print("hex_int:", hex_int)
    print("address:", address)
    print("hex_bytes:", hex_bytes)


if __name__ == "__main__":
    run()
