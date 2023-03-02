Stats Components
================

The core stats component is designed to allow for tracking of statistics withing the
libraries and across a crawler implementation. Examples of its use can be seen in the
various nft commands.

StatsService
------------

StatsService is a memory implementation of a stats service. It can process incrementing
a stat, recording individual timings, or summarizing all timings as a single value.
Timing methods are synchronous context managers for ease of implementation.

Complete Simple Example
-----------------------

.. literalinclude:: /_examples/core_stats_example.py
   :language: python

Executing the above example will result in something similar to following being
displayed in the console:

.. line-block::
    timer: [103774451]
    ms_counter: 101
    counter: 2


:download:`Download the example </_examples/core_stats_example.py>` and try it yourself.
