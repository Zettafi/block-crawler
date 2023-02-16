# Block Crawler

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/block-crawler/block-crawler/ci-actions.yaml)
[![Documentation Status](https://readthedocs.org/projects/block-crawler/badge/?version=latest)](https://block-crawler.readthedocs.io/en/latest/?badge=latest)

Block Crawler is a collection of tools and an application framework for extracting data from a 
blockchain and storing that data in a database. Block Crawler includes all you need to extract NFT 
data from EVM blockchains and store them in a DynamoDB database and the underlying modules necessary
to construct your own crawler to collect EVM blockchain data from blocks, transactions, transaction
receipts, and logs.

At its core, Block Crawler utilizes an asynchronous data bus and ultra-high performance 
RPC client and data service to expediently and efficiently process large amounts of 
data.

## Getting Started

For a quick primer on how to use the Block Crawler to crawl a blockchain, see the 
[Getting Started Guide](https://block-crawler.readthedocs.io/en/latest/getting-started).

For information on preparing your environment to build your own crawler using the
Block Crawler as a framework, see the 
[Developer Guide](https://block-crawler.readthedocs.io/en/latest/developer-guide).
