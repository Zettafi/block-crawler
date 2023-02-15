Dev Commands
============

Reset DB
--------

The `reset-db` command is used to create/reset the DynamoDB NFT database in a
development environment.

Arguments
+++++++++

:--retry / --no-retry: Retry on connection failure. If you are using this command to
    programmatically initialize an environment, this option will allow to compensate for
    race conditions in which the command attempts to run before the database is
    available.

:--dynamodb-table-prefix: The prefix for the tables in the DynamoDB database.
