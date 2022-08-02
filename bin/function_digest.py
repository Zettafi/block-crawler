import click
from eth_hash.auto import keccak


@click.command()
@click.argument("FUNCTION_ABI")
@click.option("--log-topic", default=False)
def main(function_abi, log_topic: bool):
    """
    Helper function to return the hash value needed to identify the contract
    method for an `eth_call` RPC call to an EVM

    Example:

         python bin/function_digest.py "supportsInterface(bytes4)"
         0x01ffc9a7

    """
    sig = "0x" + keccak(function_abi.encode()).hex()
    retval = sig if log_topic else sig[:10]
    click.echo(retval)


if __name__ == "__main__":
    main()
