import click
from eth_hash.auto import keccak


@click.command()
@click.argument("FUNCTION_ABI")
def main(function_abi):
    """
    Helper function to return the hash value needed to identify the contract method for an `eth_call` RPC call to an EVM

    Example:

         python bin/function_digest.py "supportsInterface(bytes4)"
         0x01ffc9a7

    """
    sig = "0x" + keccak(function_abi.encode()).hex()[:8]
    click.echo(sig)


if __name__ == "__main__":
    main()
