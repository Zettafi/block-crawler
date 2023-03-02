Service Components
==================

Components that perform logic services for the Block Crawler

MemoryBlockTimeCache
--------------------

A simple, memory-based cache to store block times. It may be pre-loaded as well as
iterated upon. It's primary reason for being is to provide a cache for the
`BlockTimeService`.

Example:

.. literalinclude:: /_examples/evm_services_example.py
   :language: python
   :lines: 12-17,29-32


BlockTimeService
----------------

A service to get the timestamp for a block. The service will utilize the provided
RPC client to get the timestamp from block data on the blockchain if the timestamp for
the block does not exist in the provided cache. Once retrieved it will be stored in the
cache.

Example:

.. literalinclude:: /_examples/evm_services_example.py
   :language: python
   :lines: 12,20-22
   :emphasize-lines: 3-4


Complete Simple Example
-----------------------

.. literalinclude:: /_examples/evm_services_example.py
   :language: python

Executing the above example providing a RPC API Websocket URI will result in something
similar to the following being displayed in the console:


.. line-block::

    Block 1 Time: 12345
    Block 2 Time: None
    Block 1 Time: 12345
    Block 2 Time: 1438270017
    Get Block Calls: 1
    Block 2 Time: 1438270017
    Get Block Calls: 1

    Cache Data:
      Block/Time: 1/12345
      Block/Time: 2/1438270017


:download:`Download the example </_examples/evm_services_example.py>` and try it yourself.
