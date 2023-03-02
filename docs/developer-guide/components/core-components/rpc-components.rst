RPC Components
==============

The core RPC components are the building blocks for high-performance
asynchronous RPC communication with RPC APIs like the Ethereum Virtual
Machine (EVM) RPC API.

RPC Client
----------

The RPC client is designed to process as many requests as quickly as is feasible with
the RPC API endpoint. This is achieved by using the asynchronous nature of websocket
connections to send as many simultaneous request as allowed. The client has the
capability of limiting the number of requests per second and the number of simultaneous
concurrent requests being processed. This ensures that crawlers can tune their
throughput for RPC APIs for maximum speed.

To track and optimize performance, the RPC Client utilizes another core component in the
Stats Service which tracks a number of RPC client metrics such as requests sent,
responses received, total request processing time, the number of requests delayed due
to exceeding the maximum number of requests or waiting for the required time for a
"too many requests" response from the RPC API, the number of connection resets, and
more.

The RPC Client is an async context manager and must be utilized as such. Below is an
example of making an EVM RPC request to get the block height:


.. literalinclude:: /_examples/core_rpc_example.py
   :language: python
   :pyobject: run


Complete Simple Example
-----------------------

.. literalinclude:: /_examples/core_rpc_example.py
   :language: python

Executing the above example will result in something similar to the following being
displayed in the console:

.. line-block::
    Block Number 16636314
    RPC Calls: 1
    RPC Call ms: 184


:download:`Download the example </_examples/core_rpc_example.py>` and try it yourself.
