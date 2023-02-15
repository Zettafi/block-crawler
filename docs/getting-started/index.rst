Getting Started
===============

#. Install block-crawler:

    .. code-block:: bash

        python -m pip install block-crawler

#. Setup `AWS credentials for Boto3`_

#. Setup the DynamoDB schema.

    .. warning::

        The following command will reset any previously loaded data!

    .. code-block:: bash

        block-crawler dev reset-db

#. Get the current block height

    .. code-block:: bash

        block-crawler evm block-number wss://some-archive-node-uri

#. Load the blockchain data to the current block height

    .. code-block:: bash

        block-crawler nft --blockchain ethereum-mainnet \
        --evm-rpc-nodes  wss://some-archive-node-uri 1 \
        load --increment-data-version=True 1 16_622_533 16_622_533

    .. note::

        You may encounter errors while running. If so, resolve the issue and run the load command
        again with the starting block at the block after the last known loaded block based on the
        logs and `increment-data-version` set to `False`. `increment-data-version` should only be
        set to `True` on the first run of the loader.

#. Get the current block height again

    .. code-block:: bash

        block-crawler evm block-number wss://some-archive-node-uri

#. Crawl the blocks from the end of the load to the current block height.

    .. code-block:: bash

        block-crawler nft --blockchain ethereum-mainnet \
        --evm-rpc-nodes  wss://some-archive-node-uri 1 \
        crawl 16_622_533 16_701_373

   .. note::

        Depending on how many blocks you need to crawl, you may need to run the command multiple times.
        You don't want to start tailing until you are within a few hundred blocks of the current
        block height.

#. Seed the database with the last block processed by crawl.

    .. code-block:: bash

        block-crawler nft seed 16_701_373

#. Start the tailer to keep up to date with the latest blocks.

    .. code-block:: bash

        block-crawler nft tail


More detailed information regarding these commands can be found in the
:doc:`User Guide <../user-guide/index>`.

.. _AWS credentials for Boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration