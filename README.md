# Chain Conductor Block Crawler

## Developer Setup

### Setup ENV vars

Copy `.env.local` to `.env` and fill in the `ARCHIVE_NODE_URI` entry with a real archive node URI. You should be able
to leave the rest as is if you fallow the defaults in this doc.

### Install the chain-conductor-block-crawler app

Use pip to install the app in "dev" mode
Development dependencies are installed as "optional" dependencies
The dependencies, along with a great deal of other information in the `pyproject.toml`.  There is a `setup.py`
but only because finding namespace packages does not appear to work properly in the `pyproject.toml` o or `setup.cfg`.

```bash
python -m pip install -e .[dev]
```

### Start up the DynamoDB server

There are a number of ways to run a local DynamoDB instance. For simplicity's sake, there is a `docker-compose.yml` for
just that purpose. run `docker-compose up` from the project root directory, and it will launch the container on port
`8000`. The server is memory only because of how slow it was when using disk in a container. AS such, you will
need to initialize the database every time you spin it up.

### (Re-)Initialize the Database

Run the reset_db.py script to reset the database tables used buy the app. You will need to provide the endpoint URL for
the script to run. As such, it can't be used for non-local instances at this time.

```bash
python bin/reset_db.py http://localhost:8000
```

### Running the crawler

Give the crawler a spin to make sure everything is working. It requires a beginning and ending block to continue.

```bash
python bin/block_crawler.py 1 10
```

The command above will crawl the 1st hundred blocks. It should do that really quickly.

### Tests

Running unit tests is no different from many other python projects. Because the tests are separated according
to the source namespace, `discover` is required. 

```bash
python -m unittest discover
```

### Code Auto-Formatting

Black is installed as a development dependency. It helps with formatting code to reduce merge conflicts. It's easy
enough to run.

```bash
black .
```

## Interacting with the database
You can use the AWS CLI to interact with the database from the command line. For the local development database, it's
really important that these three config elements match up between the Block Crawler app and the AWS CLI:

* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY
* AWS_DEFAULT_REGION

If the values don't match, you get an error similar to this:

```
An error occurred (ResourceNotFoundException) when calling the Scan operation: Cannot do operations on a non-existent table
```

You can either sev ENV vars to match the `.env` file or put them in your AWS CLI config. The CLI has a command to
get help on how to do this:

```bash
aws help config-vars
```

Once your config is all setup, you can run dynamodb commands like this one:

```bash
aws --endpoint-url http://localhost:8000 dynamodb scan --table-name Contracts
```

## Deployment

TBD