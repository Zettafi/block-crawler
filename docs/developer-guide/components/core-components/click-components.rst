Click Components
================

The core click components are built to provide required functionality
to the Click library which is used for creating commands.

BlockChainParamType
-------------------

A click param type to validate and transform the input name for a blockchain
and provide a `blockcrawler.core.entities.BlockChain` value.

Example:

.. literalinclude:: /_examples/core_click_example.py
   :language: python
   :lines: 15


HexIntParamType
---------------

A click param type to validate and transform the input hex or int and provide a
`blockcrawler.core.types.HexInt` value.

Example:

.. literalinclude:: /_examples/core_click_example.py
   :language: python
   :lines: 16


AddressParamType
----------------

A click param type to validate and transform the input hex for a blockchain address
and provide a `blockcrawler.core.entities.Address` value.

Example:

.. literalinclude:: /_examples/core_click_example.py
   :language: python
   :lines: 17


HexBytesParamType
-----------------

A click param type to validate and transform the input hex
and provide a `hexbytes.HexBytes` value.

Example:

.. literalinclude:: /_examples/core_click_example.py
   :language: python
   :lines: 18


Complete Simple Example
-----------------------

.. literalinclude:: /_examples/core_click_example.py
   :language: python

Executing the above example will result in something similar to the following being
displayed in the console:

.. line-block::
    blockchain: BlockChain.ETHEREUM_MAINNET
    hex_int: 0x1
    address: 0x7581871e1c11f85ec7f02382632b8574fad11b22
    hex_bytes: b'}\x00\x15U\x97&\x13\xbf\xd9\x14\x86\x12U/6\x05\xa1&~\xde\xe3\xe0\\\xee`\x91\xd1\x19]\x93\x01E'


:download:`Download the example </_examples/core_click_example.py>` and try it yourself.
