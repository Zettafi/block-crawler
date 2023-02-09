import asyncio
import os
from asyncio import Task
from asyncio.exceptions import TimeoutError
from typing import List

import aioboto3
import click
from botocore.exceptions import ConnectionError, ClientError

from blockcrawler.nft.data.models import (
    Collections,
    TokenTransfers,
    Tokens,
    BlockCrawlerConfig,
    Owners,
)

try:  # If dotenv in installed, use it load env vars
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


async def reset_db_async(endpoint_url, table_prefix, retry):
    resource_kwargs = dict(endpoint_url=endpoint_url)
    session = aioboto3.Session()
    async with session.resource("dynamodb", **resource_kwargs) as dynamodb:
        retrying = True
        while retrying:
            try:
                for model in (Collections, TokenTransfers, Tokens, Owners, BlockCrawlerConfig):
                    table_name = table_prefix + model.table_name
                    table = await dynamodb.Table(table_name)
                    try:
                        await table.delete()
                    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
                        pass
                    schema = model.schema.copy()
                    schema["TableName"] = table_name
                    table = await dynamodb.create_table(**schema)
                    await table.wait_until_exists()
                retrying = False
            except (ConnectionError, ClientError, TimeoutError, ValueError):
                if retry:
                    await asyncio.sleep(1)
                else:
                    raise


async def reset_s3_async(
    endpoint_url: str, metadata_bucket: str, region: str, concurrent_deletes: bool, retry: bool
):
    pending_deletes: List[Task] = list()

    async def drain_completed_deletes():
        await asyncio.sleep(0)
        for i, pending_delete in enumerate(pending_deletes):
            if pending_delete.done():
                task = pending_deletes.pop(i)
                task.result()

    retrying = True
    while retrying:
        try:
            session = aioboto3.Session()
            resource_kwargs = dict(endpoint_url=endpoint_url)
            if region is not None:
                resource_kwargs["region_name"] = region
            elif region := os.getenv("AWS_DEFAULT_REGION"):
                pass
            else:
                raise Exception(
                    "Either S3 region must be supplied" " or AWS_DEFAULT_REGION must be set"
                )

            async with session.resource("s3", **resource_kwargs) as resource:  # type: ignore
                bucket = await resource.Bucket(metadata_bucket)
                try:
                    objects_deleted = 1
                    while objects_deleted > 0:
                        objects_deleted = 0
                        async for obj in bucket.objects.all():
                            while len(pending_deletes) > concurrent_deletes:
                                await drain_completed_deletes()
                            await obj.delete()
                            objects_deleted += 1
                    while pending_deletes:
                        await drain_completed_deletes()
                except resource.meta.client.exceptions.NoSuchBucket:
                    await bucket.create(CreateBucketConfiguration=dict(LocationConstraint=region))
            retrying = False
        except (ConnectionError, ClientError, TimeoutError, ValueError):
            if retry:
                await asyncio.sleep(1)
            else:
                raise


@click.command()
@click.argument("ENDPOINT_URL")
@click.option("--retry/--no-retry", default=False)
@click.option(
    "--dynamodb-table-prefix",
    envvar="AWS_DYNAMODB_TABLE_PREFIX",
    help="Prefix for DynamoDB table names",
    default="",
)
def reset_db(endpoint_url, dynamodb_table_prefix, retry):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(reset_db_async(endpoint_url, dynamodb_table_prefix, retry))
    click.echo(click.style("DB has been reset", fg="green"))


if __name__ == "__main__":
    reset_db()
