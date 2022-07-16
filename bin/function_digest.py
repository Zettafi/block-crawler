import click
from eth_hash.auto import keccak


@click.command()
@click.argument("FUNCTION_ABI")
def main(function_abi):
    sig = "0x" + keccak(function_abi.encode()).hex()[:8]
    click.echo(sig)


if __name__ == "__main__":
    main()
