import asyncio
from asyncio import Task
from asyncio.exceptions import TimeoutError
from typing import List

import aioboto3
import click
from botocore.exceptions import ConnectionError, ClientError

from chainconductor.data.models import Collections, TokenTransfers, Tokens, LastBlock

try:  # If dotenv in installed, use it load env vars
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


async def reset_db_async(endpoint_url, retry):
    session = aioboto3.Session()
    async with session.resource(
        "dynamodb",
        endpoint_url=endpoint_url,
    ) as dynamodb:
        retrying = True
        while retrying:
            try:
                for model in (Collections, TokenTransfers, Tokens, LastBlock):
                    table = await dynamodb.Table(model.table_name)
                    try:
                        await table.delete()
                    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
                        pass
                    table = await dynamodb.create_table(**model.schema)
                    await table.wait_until_exists()
                retrying = False
            except (ConnectionError, ClientError, TimeoutError, ValueError):
                if retry:
                    await asyncio.sleep(1)
                else:
                    raise


async def reset_s3_async(endpoint_url, s3_metadata_bucket, s3_region, concurrent_deletes, retry):
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
            async with session.resource("s3", endpoint_url=endpoint_url) as resource:
                bucket = await resource.Bucket(s3_metadata_bucket)
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
                    await bucket.create(
                        CreateBucketConfiguration=dict(LocationConstraint=s3_region)
                    )
            retrying = False
        except (ConnectionError, ClientError, TimeoutError, ValueError):
            if retry:
                await asyncio.sleep(1)
            else:
                raise


@click.group()
@click.pass_context
@click.argument("endpoint_url")
@click.option("--retry/--no-retry", default=False)
def reset(ctx, endpoint_url, retry):
    """
    Simple program that initiates/resets a local database. Do not perform
    this with credentials.
    """
    ctx.ensure_object(dict)
    ctx.obj["endpoint_url"] = endpoint_url
    ctx.obj["retry"] = retry


@reset.command()
@click.pass_context
def db(ctx):
    endpoint_url = ctx.obj["endpoint_url"]
    retry = ctx.obj["retry"]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(reset_db_async(endpoint_url, retry))
    click.echo(click.style("DB has been reset", fg="green"))


@reset.command()
@click.option(
    "--s3-metadata-bucket",
    envvar="S3_METADATA_BUCKET",
    help="S3 bucket to store metadata files",
    required=True,
)
@click.option(
    "--s3-region",
    envvar="AWS_DEFAULT_REGION",
    help="AWS region to host the S3 bucket",
    required=True,
)
@click.option(
    "--concurrent-deletes",
    envvar="CONCURRENT_DELETES",
    help="The maximum number of concurrent delete operations",
    default=100,
)
@click.pass_context
def s3(ctx, s3_metadata_bucket, s3_region, concurrent_deletes):
    endpoint_url = ctx.obj["endpoint_url"]
    retry = ctx.obj["retry"]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        reset_s3_async(endpoint_url, s3_metadata_bucket, s3_region, concurrent_deletes, retry)
    )
    click.echo(click.style("S3 has been reset", fg="green"))


if __name__ == "__main__":
    reset()
