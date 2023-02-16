# Version 1.0.2

- Add better logging for failing to update token metadata URI

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
