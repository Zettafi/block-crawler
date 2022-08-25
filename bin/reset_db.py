import asyncio

import aioboto3
import click
from botocore.exceptions import ClientError

from chainconductor.data.models import Collections, TokenTransfers, Tokens, LastBlock

try:  # If dotenv in installed, use it load env vars
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


async def reset_db_async(endpoint_url):
    session = aioboto3.Session()
    async with session.resource(
        "dynamodb",
        endpoint_url=endpoint_url,
    ) as dynamodb:
        for model in (Collections, TokenTransfers, Tokens, LastBlock):
            table = await dynamodb.Table(model.table_name)
            # noinspection PyUnresolvedReferences
            try:
                await table.delete()
            except ClientError as err:
                if type(err).__name__ != "ResourceNotFoundException":
                    # ResourceNotFound means table did not exist which is fine.
                    # Re-raise otherwise.
                    raise
            table = await dynamodb.create_table(**model.schema)
            await table.wait_until_exists()


@click.command()
@click.argument("endpoint_url")
def reset_db(endpoint_url):
    """
    Simple program that initiates/resets a local database. Do not perform
    this with credentials.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(reset_db_async(endpoint_url))
    click.echo(click.style("DB has been reset", fg="green"))


if __name__ == "__main__":
    reset_db()
