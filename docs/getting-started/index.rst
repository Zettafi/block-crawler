Getting Started
===============

Load a small subset of blockchain data and verify that data afterwards. If you wish to
load the entire blockchain, you would replace the block height with the current block
height as available via the `evm block-number` command.


#. Install block-crawler:

    .. code-block:: bash

        python -m pip install block-crawler

#. Setup `AWS credentials for Boto3`_

#. Setup the DynamoDB schema.

    .. warning::

        The following command will reset any previously loaded data!

    .. code-block:: bash

        block-crawler dev reset-db

#. Load the blockchain data from block 5,600,00 to 6,000,000. Nothing before 5,600,000
   will have NFT data.

    .. code-block:: bash

        block-crawler nft --blockchain ethereum-mainnet \
        --evm-rpc-nodes  wss://some-archive-node-uri 1 \
        load --increment-data-version=True 5_600_000 5_600_000 6_000_000

    .. note::

        You may encounter errors while running. If so, resolve the issue and run the load command
        again with the starting block at the block after the last known loaded block based on the
        logs and `increment-data-version` set to `False`. `increment-data-version` should only be
        set to `True` on the first run of the loader.

# Verify the God's Unchained collection up to block 6,000,000. Gods Unchained should
  have been added in the load.

    .. code-block:: bash

        block-crawler nft --blockchain ethereum-mainnet \
        --evm-rpc-nodes  wss://some-archive-node-uri 1 \
        verify 0x6ebeaf8e8e946f0716e6533a6f2cefc83f60e8ab 6_000_000

#. Crawl the blocks from the block 6,000,001 to 6,100,000.

    .. code-block:: bash

        block-crawler nft --blockchain ethereum-mainnet \
        --evm-rpc-nodes  wss://some-archive-node-uri 1 \
        crawl 6_000_001 6_100_000

   .. note::

        Depending on how many blocks you need to crawl, you may need to run the command multiple
        times. You don't want to start tailing until you are within a few hundred blocks of the
        current block height.

#. Verify the God's Unchained collection up to block 6,100,000. The contract/tokens
   should have been updated in the crawl.

    .. code-block:: bash

        block-crawler nft --blockchain ethereum-mainnet \
        --evm-rpc-nodes  wss://some-archive-node-uri 1 \
        verify 0x6ebeaf8e8e946f0716e6533a6f2cefc83f60e8ab 6_100_000

#. Verify the Hyperloot collection up to block 6,100,000. It should have been added in
   the crawl.

    .. code-block:: bash

        block-crawler nft --blockchain ethereum-mainnet \
        --evm-rpc-nodes  wss://some-archive-node-uri 1 \
        verify 0xf766b3e7073f5a6483e27de20ea6f59b30b28f87 6_100_000


#. Seed the database with the last block processed by crawl.

    .. code-block:: bash

        block-crawler nft seed 6_100_000

#. Start the tailer to keep up to date with the latest blocks.

    .. code-block:: bash

        block-crawler nft tail


More detailed information regarding these commands can be found in the
:doc:`User Guide <../user-guide/index>`.

.. _AWS credentials for Boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration