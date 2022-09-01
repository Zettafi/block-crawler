# Chain Conductor Block Crawler

## Commands

At its heart, this is app is a set of command line tools related to crawling the blockchain. All commands
are Click-based, so they provide full help for parameters and options. Just add the option `--help` to any command
to bring up the help text.

### Block Crawler

The block crawler crawls block on the blockchain and writes the data to the Chain Conductor database.

```bash
python bin/block_crawler.py --help
```

### Reset DB

This command resets a developer's local instance of DynamoDB

```bash
python bin/reset_db.py --help
```

### Function Digest

This is a helper command to take the ABI specification of function and return the hex encoded Keccak hash bytes
used to identify the function in "Call" RPC methods on an EVM 

```bash
python bin/function_digest.py --help
```

## Developer Setup

### Setup ENV vars

Copy `.env.local` to `.env` and fill in the `ARCHIVE_NODE_URI` entry with a real archive node URI. You should be able
to leave the rest as is if you fallow the defaults in this doc.

### Install the chain-conductor-block-crawler app

Use pip to install the app in "dev" mode
Development dependencies are installed as "optional" dependencies
The dependencies, along with a great deal of other information in the `pyproject.toml`.
There is a `setup.py` but only because finding namespace packages does not appear to 
work properly in the `pyproject.toml` or `setup.cfg`.

```bash
python -m pip install -e ".[dev]"
```

### Start up the local AWS services

Running the services locally saves money on the Amazon bill and reduces transaction 
times. For simplicity's sake, there is a `docker-compose.yml` for just that purpose. 
Run `docker-compose up` from the project root directory, and it will launch the services
on the ports specified in the .env.local as well as initialize DynamoDB and S3.

### (Re-)Initialize the Database or S3

Run the reset_db.py script to reset the database tables used buy the app. You will 
need to provide the endpoint URL for the script to run. As such, it can't be used 
for non-local services at this time.

Example reset of DynamoDB
```bash
python bin/reset.py http://localhost:8000 db
```

Example reset of S3
```bash
python bin/reset.py http://localhost:8000 s3
```

### Running the crawler

The crawler has a number of commands. The most pertinent are crawl, seed, and listen. 
The crawl command will crawl a range of specified blocks,

Give the crawler a spin to make sure everything is working. It requires a beginning and 
ending block to continue.

```bash
python bin/block_crawler.py crawl 1 10
```

The command above will crawl the 1st hundred blocks. It should do that really quickly.

The seed command will set the last processed block in the database for the crawler's 
tail command.


```bash
python bin/block_crawler.py seed 15_000_000
```

The command above will set the last process block number to 15,000,000. THe tail command
will then start at 15,000,001 the next rime it runs.

The tail command will get the last block number from the node and determine if it has 
blocks to process. It will process the blocks, sleep for a bit, and then run the entire 
process again and again until it is stopped,

### Tests

Running unit tests is no different from many other python projects. Because the tests 
are separated according to the source namespace, `discover` is required. 

```bash
python -m unittest discover
```

### Code Auto-Formatting

Black is installed as a development dependency. It helps with formatting code to reduce
merge conflicts. It's easy enough to run.

```bash
black .
```

## Interacting with the database
You can use the AWS CLI to interact with the database from the command line. For the 
local development database, it's really important that these three config elements match
up between the Block Crawler app and the AWS CLI:

* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY
* AWS_DEFAULT_REGION

If the values don't match, you get an error similar to this:

```
An error occurred (ResourceNotFoundException) when calling the Scan operation: Cannot 
do operations on a non-existent table
```

You can either sev ENV vars to match the `.env` file or put them in your AWS CLI config.
The CLI has a command to get help on how to do this:

```bash
aws help config-vars
```

Once your config is all setup, you can run dynamodb commands like this one:

```bash
aws --endpoint-url http://localhost:8000 dynamodb scan --table-name Collections
```

## Deployment

TODO
