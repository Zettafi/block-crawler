# Block Crawler

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/block-crawler/block-crawler/ci-actions.yaml)

Block Crawler is a collection of tools for extracting data from a blockchain and 
storing that data in a database. While it will eventually service a number of 
blockchains and databases, it is currently designed to extract NFT data from EVM 
blockchains. 

There are three stages to processing blockchain data in Block Crawler:

* Load - load historical data to a specific block height. Accuracy is ensured by 
  maintaining a static block height at which all blocks are processed. Loading is 
  designed to be as fast and economical as possible.
* Crawl - process all blocks after the load process completes. The load process can
  take hours to days depending on the data processed and stored.
* Tail - keep up to date with blockchain and process any newly added blocks.

## Quick Start

1. Install block-crawler:
    ```bash
    python -m pip install block-crawler
    ```
   
2. Setup [AWS credentials for Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration)

3. Setup the DynamoDB schema. **WARNING: the following command will 
   reset any previously loaded data!**
    ```bash
    block-crawler dev reset-db
    ```

4. Get the current block height

    ```bash
    block-crawler evm block-number wss://some-archive-node-uri
    ```

5. Load the blockchain data to the current block height

    ```bash
    block-crawler nft --blockchain ethereum-mainnet --evm-rpc-nodes  wss://some-archive-node-uri 1 \
      load --increment-data-version=True 1 16_622_533 16_622_533
    ```
    *You may encounter errors while running. If so, resolve the issue and run the load command 
     again with the starting block at the block after the last known loaded block based on the 
     logs and `increment-data-version` set to `False`. `increment-data-version` should only be 
     set to `True` on the first run of the loader.*

6. Crawl the blocks from the end of the load to the current block height.
    ```bash
    block-crawler evm block-number wss://some-archive-node-uri
    ```
    ```bash
    block-crawler nft --blockchain ethereum-mainnet --evm-rpc-nodes  wss://some-archive-node-uri 1 \
      crawl 16_622_533 16_701_373
    ```
   *Depending on how many blocks you need to crawl, you may need to run the command multiple times.
    You don't want to start tailing until you are within a few hundred blocks of the current
    block height.*
   
7. Seed the database with the last block processed by crawl.
    ```bash
    block-crawler nft seed 16_701_373
    ```
   
8. Start the tailer to keep up to date with the latest blocks.
    ```bash
    block-crawler nft tail
    ```
