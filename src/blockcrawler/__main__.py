import sys

import click

from blockcrawler.nft.bin.reset import reset
from .nft.bin.block_crawler import nft
from .evm.bin import evm


@click.group
def dev():
    """
    Developer tools
    """
    pass


dev.add_command(reset)


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
    rc = 1
    try:
        main()
        rc = 0
    except Exception as e:
        print("Error: %s" % e, file=sys.stderr)
    sys.exit(rc)
