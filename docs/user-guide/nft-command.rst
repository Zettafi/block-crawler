NFT Commands
============

NFT commands are for the purpose of crawling blockchains to store data in a database.

.. warning::

    Running the `crawl` or `tail` commands more than once for the same block will cause
    inaccuracies in the NFT data. The `load` and `force` commands are the only NFT
    commands which are idempotent.

Data Version
------------

Data version is used by the NFT commands in the Block Crawl to ensure data accuracy
during data loads. Data is processed by both the `crawl` and `tail` commands in an
asynchronous manner and data version is one manner in which data accuracy is ensured.

.. TODO Add something about how to use data version in a load

Common Options
--------------

:--blockchain: The blockchain being processed. It must be one of the accepted values
    of "ethereum-mainnet" or "polygon-mainnet". This value is for identifying the
    blockchain in the database.

:--evm-rpc-nodes: This is a repeatable option for specifying a websocket RPC API
    endpoint and the number of simultaneous connections to make. For example,
    the value of `wss://some.rpc.node 25` will operate with 25 connections to the
    `wss://some.rpc.node` RPC API endpoint. If you use multiple providers or multiple
    accounts with the same provider, include this option multiple times to fully
    utilize available resources.

:--rpc-requests-per-second: The maximum number of requests per second per RPC node. This
    is not maximum across all nodes.

:--dynamodb-endpoint-url: This is an override of the endpoint the AWS client will use
    to connect to the DynamoDB service. It is normally used only for non-standard
    endpoints such as locally hosted DynamoDB for testing. Normally the location of the
    DynamoDB database would be determined from the AWS default region as part of the
    AWS config.

:--dynamodb-timeout:  Maximum time in seconds to wait for connect or response from
    DynamoDB. This does not need to be altered ordinarily. The Block Crawler greatly
    reduces the connect and response timeout values which are defaulted to 5 minutes by
    the AWS client to identy connectivity issues in a timely manner. If you experience
    timeout issues with DynamoDB, increasing the value with this option may improve
    your experience.

:---dynamodb-table-prefix: The table prefix for DynamoDB tables for data managed by the
    Block Crawler. DynamoDB table names are global and often prefixed for multiple
    environments or to use in access restriction.

:--log-file: This is the location of a file to which the Block Crawler should log. By
    default, STDOUT is the only logging location.

:--debug: Enable debug level logging. This will be an extremely verbose level of
    logging. It is not meant for long-term use in production systems.

AWS Configuration
-----------------

Many of the commands utilize DynamoDb and require AWS credentials setup. Instructions
for configuring the credentials can be found in the `Boto3 documentation`_.

Common Log Output
-----------------

Many of the commands write logs which follow a standard as follows;

Items with two values separated by a `/` are a count and an average time is milliseconds
to execute such as `123/45.67` represents a count of 123 with an average processing time
of 45.67 milliseconds.

:Blocks: The start and end of the current block chunk being processed.

:Conn: Connection statistics

       :C: Connections all time - This includes initial connections plus any reconnects

       :X: RPC clients reconnected

       :R: Connection resets from the endpoint

:RPC: RPC Request Statistics

       :S: Requests sent

       :D: Requests delayed due to request limitations or
            delays resulting from too many request results returned by the endpoint

       :T: Number of "too many request" results returned by the endpoint

       :R: Responses received

:Write: Data write statistics

       :D: Delayed - the number of writes delayed due to request limitations from the
            database

       :C: Collection records written

       :T: Token records written

       :TU: Token records updated

       :X: Token Transfer records written

       :O: Owner records written

       :OU: Owner records updated


Output lines example:

.. line-block::

    2023-02-15 21:18:24,975 Blocks [12,400,001:12,401,000] -- Conn [C:46 X:28 R:0] RPC [S:923,930 D:24,817 T:0 R:923,908/445] -- Write [D:3,228 C:112/153 T:83,748/7 X:252,576/10 O:151,185/9]
    2023-02-15 21:27:45,802 Total Time: 8:29:19.63 -- Blocks 11,900,001 to 12,400,000 at block height 16,378,614

Common Caveats
--------------

Token Quantity
++++++++++++++

Database restrictions on the size of a number can prevent a token from being created
via a mint or updated via a transfer if the resulting quantity exceeds the allowable
size. As the Block Crawler performs the incrementing and decrementing of the quantity
per blockchain transaction in an asynchronous manner, it may be necessary for the
database to store the quantity as a number to allow for atomic operations to increment
and decrement the value of quantity. The maximum size of the number is database
dependent. For example, DynamoDB allows for 38 digits of precision which allows the
quantity value to be very large. However, token from an ERC-1155 contract with a high
value for "decimals" may exceed maximum size. If this occurs, an error will appear in
the log for tracking purposes.

Token URI
+++++++++

Database restrictions on text and/or record size may prevent the storage of token URIs
when the token URI is very large. This tends to be the case
when the contract attempts to place another data URI for the token "image" attribute
which includes the base64 encoded value of the image binary. This is not a common
practice, but it has been identified as a metadata strategy in use by a limited number
of collections. When this occurs, the token URI will not be set/updated and an error
will appear in the log for tracking purposes.

Invalid Token URIs
++++++++++++++++++

A number of collections return data that cannot be parsed properly for token URIs. The
URIs themselves contain binary data that cannot be decoded as a string per the
specification. When this occurs, the token URI will not be set/updated and an error
will appear in the log for tracking purposes.

Collection Description
++++++++++++++++++++++

Database restrictions on text and/or record size may limit the ability to store the
entire collection description in the database. When this occurs, the description will
be truncated to a sane value for the database.

Performance Considerations
--------------------------

Most commands are built to be massively parallel. They may strain the resources of the
system running the command, the database, and RPC endpoints. The resources used for the
commands can be optimized by altering options such as `--dynamodb-parallel-batches`,
`--block-chunk-size`, `--evm-rpc-nodes`, and `--rpc-requests-per-second`. Adjusting
the values of these options is suggested to maximize performance. Command output

Load
----

The `load` command will load NFT data up to a declared block height by processing each
collection as its creation is discovered while traversing the blockchain in reverse
order. The specific block height is necessary to ensure each collection's data is
accurate to the same block height at which time the `crawl` and `tail` commands can
traverse any remaining blocks to bring the NFT data up to dat with the current block
height. Processing blocks in reverse order is necessary It was created to reduce the time and number of RPC requests necessary to load NFT data from large blockchains.


Arguments
+++++++++

:STARTING_BLOCK: The lowest block number you wish to process in this run of the `load`
    command.

:ENDING_BLOCK: The highest block number you wish to process in this run of the `load`
    command.

:BLOCK_HEIGHT: The block height chosen for this data load process. This value should be
    consistent if the `load` command is interrupted and re-run. The command loads log
    entries for the collection from the creation of the collection to the block height
    value. As such, it must be consistent for the duration of a data load to ensure all
    collections are accurate to the same block height and the `crawl` or `tail` command
    can reliably continue after that block.

Options
+++++++

:--increment-data-version: Incrementing the data version should only occur for the
    initial execution of the `load` command for loading data.


:--block-chunk-size: The number of blocks to process at one time. Restricting the
    number of blocks processed simultaneously provides two benefits. First, it limits
    the computing resources utilized for attempting to process large quantities of
    blocks. Second, it allows for a graceful stop at a known break point should it
    be necessary to stop the command. The command will wait until all blocks in the
    block chunk are fully processed before exiting to end in a known state in which
    there is no risk of processing the same block twice.

:--dynamodb-parallel-batches: THe number of DynamoDB parallel batch writes to perform
    simultaneously. In order to maximize performance, you want to keep batches as full
    as possible. Tuning this value can improve data write performance accordingly.

:--block-time-cache-filename: Location and filename for the block time cache. The block
    time cache is critical for reducing RPC calls to get block times. As the `load`
    command traverses the blockchain in reverse order, it stores the block time for each
    block it processes. To ensure any stoppage of the command does not lose the stored
    block times, it will store it is a CSV formatted file. It will then load the data
    from the file when it starts the next time. This persistence of the block times
    is critical to reduce the number of RPC calls to get the block time as the command
    must retrieve the block time from the block chain if it cannot find it in its own
    memory.

    .. warning::

        Running multiple versions of the `load` command will require separate block time
        cache filenames lest they overwrite each other's data.


Crawl
-----

The `crawl` command will crawl each block of a blockchain in ascending order for NFT data.
It process data in chunks of blocks. It discovers new collections, token transfers,
token updates, and owner updates by processing data contained within blocks. It is
faster than the `tail` command but much slower and uses considerably more RPC requests
than load. The command is meant to be used after a `load` command and before a `tail`
command to reduce the number of blocks the the `tail` command will  have to process.


Arguments
+++++++++

:STARTING_BLOCK: The block at which the crawl begins

:ENDING_BLOCK: The block at which the crawl ends

Options
+++++++

:--increment-data-version: Incrementing the data version should only occur in a
    scenario in which the `crawl` command will be used to re-load data in place
    over a previous data load from the origin block.

    .. note::
        Due to the time and resources necessary to initiate a data load via `crawl`,
        it is highly encouraged that you use the `load` command to initiate any data
        load.

:--block-chunk-size: The number of blocks to process at one time. Restricting the
    number of blocks processed simultaneously provides two benefits. First, it limits
    the computing resources utilized for attempting to process large quantities of
    blocks. Second, it allows for a graceful stop at a known break point should it
    be necessary to stop the command. The command will wait until all blocks in the
    block chunk are fully processed before exiting to end in a known state in which
    there is no risk of processing the same block twice.

Tail
----

The `tail` command will continuously check for new blocks and process them in the same
manner as the `crawl` command. The main differences between `crawl` and `tail` are the
tail process one block at a time and persists the last block it has processed. The first
time you attempt to run the `tail` command, it requires having run hte `seed` command
to record the last block processed from wch the `tail` command will continue forward.
Another differentiator for this command will run until it is interrupted. It is meant
to be run as a service to keep the database up to date with the latest changes from the
blockchain.

Arguments
+++++++++

There are no arguments for the command

Options
+++++++

:--trail-blocks: The number of blocks to trail behind the last block. This option
    exists for two reasons, nodes can be ad different stages of completion in with
    regard to the latest block. One node can be completed and list it as the latest
    block while another may not have completed and either error or return partial
    data. It's common to see nodes return a block with no transaction hashes when
    retrieving the incomplete blocks. The second is dealing with reorgs caused by
    blockchain forking. Staying far enough behind any reorg is important until the
    tail command is advanced enough to back out the results of reorganized blocks.

:--process-interval: How often to check for new blocks. The command is currently based
    on polling for the current block of the blockchain to identify new blocks need to
    be processed. To reduce unnecessary process and cost from checking the block height,
    the command will not perform two subsequent checks in less than the interval
    specified. If processing the latest blocks exceeds the interval, it will not wait to
    check again and do so immediately after processing the last block it knows.


Seed
----

The seed command sets the last block processed in the database utilized by the `tail`
command to identify its starting point when processing.

Arguments
+++++++++

:LAST_BLOCK_ID: The last block processed by one of the other commands.

Verify
------

Verify that the collection data stored in the database matches the data in the
blockchain.

Arguments
+++++++++

:COLLECTION_ID: The collection ID to verify

:BLOCK_HEIGHT: The block height at which to verify. Blockchain data is constantly being
    updated. As such, it can only be verified at a specific block height.



Force
----

The `force` command will load NFT data for a single collection up to a declared block
height. The specific block height is necessary to ensure the collection's data is
accurate to the same block height as the rest of the blockchain data.

The force command will load collection regardless of how that collection identifies
itself. It is useful for repairing data for a single collection that either did not
load due to the other commands not recognizing the collection, interrupted in the
loading of collection data, or encountered a bug in the Block Crawler.


Arguments
+++++++++

:COLLECTION_ID: The collection you wish to force load.

:CREATION_TX_HASH: The highest block number you wish to process in this run of the
    command.

:BLOCK_HEIGHT: The block height chosen for this data load process. This value should be
    consistent with the latest block processed to get an accurate data load up to the
    block height of the rest of the collections in the blockchain.

:CREATION_TX_HASH: The transaction hash for the transaction in which the collection was
    created.

:DEFAULT_COLLECTION_TYPE: The collection type to assign to collection if it's type
    cannot be determined programmatically.

Options
+++++++

:--dynamodb-parallel-batches: THe number of DynamoDB parallel batch writes to perform
    simultaneously. In order to maximize performance, you want to keep batches as full
    as possible. Tuning this value can improve data write performance accordingly.

:--block-time-cache-filename: Location and filename for the block time cache. The block
    time cache is critical for reducing RPC calls to get block times.


.. _Boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration