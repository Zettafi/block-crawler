import sys

import click

from .nft.bin.reset import reset_db
from .nft.bin.nft import nft
from .evm.bin import evm


@click.group
def dev():
    """
    Developer tools
    """
    pass


dev.add_command(reset_db)


@click.group
def main():
    """
    Block Crawler commands
    """
    pass


main.add_command(nft)
main.add_command(evm)
main.add_command(dev)


if __name__ == "__main__":
    main()
    sys.exit(0)
