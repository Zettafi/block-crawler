# Contributors Guide

## Install the block-crawler app

Use pip to install the app in "dev" mode.
Development dependencies are installed as "optional" dependencies.
The dependencies and a great deal of other information are in the `pyproject.toml`.

```bash
python -m pip install -e ".[dev]"
```

## Start up the local AWS services

Running the services locally saves money on the Amazon bill and reduces transaction
times. For simplicity's sake, there is a `docker-compose.yml` for just that purpose. 
Run `docker-compose up` from the project root directory, and it will launch the services
on the ports specified in the .env.local and initialize DynamoDB and S3.

## (Re-)Initialize the Database

Run the reset_db.py script to reset the database tables used by the app. You will 
need to provide the endpoint URL for the script to run. As such, it can't be used 
for non-local services.

Example reset of DynamoDB
```bash
block-crawler dev reset-db http://localhost:8000
```

## Running the Crawler

The Crawler has several commands. The most pertinent are crawl, seed, and listen. 
The crawl command will crawl a range of specified blocks,

Give the Crawler a spin to make sure everything is working. It requires a beginning and 
ending block to continue.

```bash
block-crawler nft crawl 1 10
```

The command above will crawl the 1st hundred blocks. It should do that quickly.

The seed command will set the last processed block in the database for the Crawler's 
tail command.


```bash
block-crawler nft seed 15_000_000
```

The command above will set the last process block number to 15,000,000. The tail command
will then start at 15,000,001 the next time it runs.

The tail command will get the last block number from the node and determine if it has 
blocks to process. It will process the blocks, sleep for a bit, and then run the entire 
process repeatedly until it is informed to stop.

### Tests

Running unit tests is no different from many other python projects. Because the tests 
are separated according to the source namespace, `discover` is required. 

```bash
python -m unittest discover
```

### Linting, Formatting, and Typing

Block Crawler uses a number of tools like Black, Flake8, and mypy. They are all
configured via pre-commit and checks can be run via the command line tool:

```bash
pre-commit
```

The pre-commit tool can be easily installed as a Git hook to ensure you never commit
and push code that will not pass inspection via continuous integration. It's highly
suggested that you install the hooks via:

```bash
pre-commit install
```


## Interacting with the database
You can use the AWS CLI to interact with the database from the command line. For the 
local development database, it's vital that these three config elements match
up between the Block Crawler app and the AWS CLI:

* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY
* AWS_DEFAULT_REGION

If the values don't match, you get an error similar to this:

```
An error occurred (ResourceNotFoundException) when calling the Scan operation: Cannot 
do operations on a non-existent table
```

You can set ENV vars to match the `.env` file or put them in your AWS CLI config.
The CLI has a command to get help on how to do this:

```bash
aws help config-vars
```

Once your config is all setup, you can run DynamoDB commands like this one:

```bash
aws --endpoint-url http://localhost:8000 dynamodb scan --table-name Collections
```
