EVM Commands
============

The EVM commands are a collection of tools for working with and developing
for Ethereum Virtual Machine (EVM) based blockchains.

Block Number
------------

The `block-number` command retrieves the current block height on en EVM node.

Arguments
+++++++++

:ARCHIVE_NODE: The URI for the websocket interface of the EVM node to be queried

Function Digest
---------------

The `function-digest` command takes the definition of a solidity function and
returns the API function selector required for making `eth_call` RPC calls on
an EVM-based RPC API. See the `Solidity documentation`_ for more information.

Arguments
++++++++++

:FUNCTION_ABI: The function definition as defined in the ABI specification

.. _Solidity documentation: https://docs.soliditylang.org/en/latest/abi-spec.html#function-selector
