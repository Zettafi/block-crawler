Utility Components
==================

Components which provide specific utility for the EVM package


BlockTimeCacheManager
---------------------

Utility for writing and reading block time caches to a CSV file to preserve the cache
between application executions.

Complete Simple Example
-----------------------

.. literalinclude:: /_examples/evm_utils_example.py
   :language: python

Executing the above example providing a file patch will result in something
similar to the following being displayed in the console:


.. line-block::

    File Contents:
    1,12345
    2,23456
    3,34567

    Block 1 Time: 12345



:download:`Download the example </_examples/evm_utils_example.py>` and try it yourself.
