User Guide
==========

This guide provides a fundamental understanding of the concepts utilized by Block
Crawler to crawl blockchains and more.

Command Line Interface (CLI)
----------------------------

A command line interface is built into the Block Crawler and will allow you to load
NFT data from an Ethereum Virtual Machine (EVM) based blockchain into a DynamoDB
database. It is just one example of how the modules may be used to build a process
to load and maintain data from one system in another.

To see the available commands, simply access help from the block-crawler command in a
python environment in which the Block Crawler in installed:

.. code-block:: bash

    block-crawler --help

The currently available commands are:

* nft - :doc:`Crawling blockchains for Non-Fungible Token (NFT) data <nft-command>`
* evm - :doc:`Ethereum Virtual Machine (EVM) specific processing <evm-command>`
* dev - :doc:`setting up environments <dev-command>`

.. toctree::
   :hidden:

   nft-command
   evm-command
   dev-command
