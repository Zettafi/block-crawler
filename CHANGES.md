# version 1.2.3

- Fixes for nft load command
  -  Always get token URI when loading an ERC-721 contract as the function is supported
      regularly without supporting the ERC721Metadata interface
  - Account for the asynchronous nature of nft load when determining the current owner
      of an ERC-721 token
- Update the nft verify command to add verification to owners even if enumerable not 
    supported


# Version 1.2.2

- Fix version/release inconsistency

# Version 1.2.1

- Docs fix to build proper docs

# Version 1.2.0

- Standardize on providing blockchain to EVM transformers and only process data packages
  for the same blockchain 
- Remove loggers from class init methods. Let them get the loggers themselves.
- Bug fix for nft crawl/tail incorrectly identifying operator as from and from as to when
    processing transfers.
- Bug fix for nft verify using static value of 1 for NFT quantity.
- Bug fix for verify not displaying token errors.

# Version 1.1.1

- nft tail fixed to get the block height on start
- nft tail fixed to properly log exceptions
- Allow for empty transactions list in EVM RPC Client as it can actually happen
- Log unrecoverable issues rather than raise

# Version 1.1.0

- Moved all data processing into the data service
  - Allows for all operations to be properly throttled to avoid errors from hot partition
  - Reduce code duplication
  - Puts the logic in the right domain
- Refactored how we write data for tokens and owners
  - No more waiting for tokens to update data in crawler and loader to handle a few issues
    - Contracts that are missed by the loader because they are not recognized (contracts making contracts)
    - Contracts that never mint or burn but place transfers (OpenSea)
    - Order dependence causing lots of extra queries
- Cleaned up commands/bin to be better contained and easier to follow
- Reduced code duplication for stats logging
- Standardized on naming for metadata_uri
- Updated parallel bus to allow for configuration option to raise exceptions caught by consumers
- Updated nft tail command to have bus raise exceptions, log errors from those exceptions, 
  and reprocess the block on which hte error occurred
- Fix bug in tailer where it would get ahead of the block height

# Version 1.0.2

- Add better logging for failing to update token metadata URI
- Add common caveats to nft command docs

# Version 1.0.1

- Add block number to error messages to enable replay after the fact in load
- Fix crawl message line error
- Added initial documentation
- Quiet the logs
- Fix crawl/tail stats - use write instead of write batch stats.

# Version 1.0.0

- Update README with quickstart
- Add build optional deps for build and publish
- Move websocket exceptions in inbound loop of RPC client logging to DEBUG level
- Add stats to nft tail command and added update stats for nft crawl command
- Remove input check for interfaces in nft loader as it prevented loading of proxy contracts
- Don't display extra transfer items from nft verify command

# Version 0.3.0

- Replace signal handler with Signal Manager to properly handle graceful shutdown of crawl and tail nft commands
- Refactor tail to always process one block at a time to always allow for clean shutdown

# Version 0.2.0

- Add signal handler for graceful shutdown of nft tail command on interrupt

# Version 0.1.1

- Bug fix for nft load command

# Version 0.1.0

- Data Bus
- Multi-Source/Connection High-Performance RPC Client
- Memory-Based Stats Service
- EVM-Based Blockchain Support
- DynamoDB Data Service Support
- NFT Commands
  - load - High Speed Load of historical data
  - crawl - Crawl block range to cover faster than tail
  - tail - Catch up to current block and process new blocks
  - seed - Identify last processed block
  - verify - Verify database data against blockchain
- EVM Commands
  - function-digest - Get ABI function digest for function definition
  - block-number - Get the current block height of a node
