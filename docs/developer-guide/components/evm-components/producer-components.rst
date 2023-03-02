Producer Components
===================

Producer components are EVM-specific implementations of Data Bus Producers

BlockIDProducer
---------------

Will produce a for a range of block IDs. Each Block ID will be contained within an
`EvmBlockIDDataPackage` and sent to the Data Bus. The range is processed via a `step`
parameter which determine the next item in the range in the same manner as the `step`
parameter inm hte Python `range()` function.

Example:

.. literalinclude:: /_examples/evm_producers_example.py
   :language: python
   :lines: 9-10,13-19


Complete Simple Example
-----------------------

.. literalinclude:: /_examples/evm_producers_example.py
   :language: python

Executing the above example will result in something similar to following being
displayed in the console:


.. line-block::

    2023-03-01T16:35:25 - EvmBlockIDDataPackage(blockchain=<BlockChain.ETHEREUM_MAINNET: 'ethereum-mainnet'>, block_id=HexInt('0x0'))

    2023-03-01T16:35:25 - EvmBlockIDDataPackage(blockchain=<BlockChain.ETHEREUM_MAINNET: 'ethereum-mainnet'>, block_id=HexInt('0x1'))

    2023-03-01T16:35:25 - EvmBlockIDDataPackage(blockchain=<BlockChain.ETHEREUM_MAINNET: 'ethereum-mainnet'>, block_id=HexInt('0x2'))

    2023-03-01T16:35:25 - EvmBlockIDDataPackage(blockchain=<BlockChain.ETHEREUM_MAINNET: 'ethereum-mainnet'>, block_id=HexInt('0x3'))

    2023-03-01T16:35:25 - EvmBlockIDDataPackage(blockchain=<BlockChain.ETHEREUM_MAINNET: 'ethereum-mainnet'>, block_id=HexInt('0x4'))

    2023-03-01T16:35:25 - EvmBlockIDDataPackage(blockchain=<BlockChain.ETHEREUM_MAINNET: 'ethereum-mainnet'>, block_id=HexInt('0x5'))

    2023-03-01T16:35:25 - EvmBlockIDDataPackage(blockchain=<BlockChain.ETHEREUM_MAINNET: 'ethereum-mainnet'>, block_id=HexInt('0x6'))

    2023-03-01T16:35:25 - EvmBlockIDDataPackage(blockchain=<BlockChain.ETHEREUM_MAINNET: 'ethereum-mainnet'>, block_id=HexInt('0x7'))

    2023-03-01T16:35:25 - EvmBlockIDDataPackage(blockchain=<BlockChain.ETHEREUM_MAINNET: 'ethereum-mainnet'>, block_id=HexInt('0x8'))

    2023-03-01T16:35:25 - EvmBlockIDDataPackage(blockchain=<BlockChain.ETHEREUM_MAINNET: 'ethereum-mainnet'>, block_id=HexInt('0x9'))


:download:`Download the example </_examples/evm_producers_example.py>` and try it yourself.
