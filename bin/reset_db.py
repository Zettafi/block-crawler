import asyncio
import click

from chainconductor.contractpuller.commands import reset_db_async

try:  # If dotenv in installed, use it load env vars
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


@click.command()
@click.argument("endpoint_url")
def reset_db(endpoint_url):
    """Simple program that initiates/resets a local database. Do not perform this with credentials."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(reset_db_async(endpoint_url))
    click.echo(click.style("DB has been reset", fg="green"))


if __name__ == "__main__":
    reset_db()
