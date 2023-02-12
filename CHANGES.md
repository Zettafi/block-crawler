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

# Version 0.1.1

- Bug fix for nft load command

# Version 0.2.0

- Add signal handler for graceful shutdown of nft tail command on interrupt

# Version 0.3.0

- Replace signal handler with Signal Manager to properly handle graceful shutdown of crawl and tail nft commands
- Refactor tail to always process one block at a time to always allow for clean shutdown
