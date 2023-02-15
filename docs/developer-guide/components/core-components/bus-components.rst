Bus Components
==============

Data Bus
++++++++

The Data Bus is the heart of the Block Crawler. It's how data is passed between the
various components implemented. It is also the name of an abstract base class defining
the interface for a Data Bus implementation. Data is placed on the Data Bus by Data
Producers. It is then sent to every registered Data Consumer to handle as they please.
Below is an example of a simple Data Bus:

.. literalinclude:: /_examples/core_bus_example.py
   :language: python
   :pyobject: SimpleDataBus

Data Bus implementations are asynchronous context managers and must be utilized as such:

.. literalinclude:: /_examples/core_bus_example.py
   :language: python
   :pyobject: run
   :emphasize-lines: 4

Data Package
++++++++++++

A Data Package is a construct for placing data on the Data Bust be consumed. Having
typed data being passed around makes it easier for Data Consumers to filter out data
they do not wish to process. Below is an example of a simple Data Package:

.. literalinclude:: /_examples/core_bus_example.py
   :language: python
   :pyobject: SimpleIntegerDataPackage

Data Producer
+++++++++++++

Data Producers place Data Packages on the Data Bus via the `send` method. There is an
abstract base class that Data Producers included withing the Block Crawler extend. Below
is an example of a simple Data Producer:

.. literalinclude:: /_examples/core_bus_example.py
   :language: python
   :pyobject: SimpleProducer

Data Consumer
+++++++++++++

Data Consumers receive Data Packages from the Data Bus via their `receive` methods. When
processing the Data Package, It is expected that exceptions will be caught and
transformed into ConsumerError exceptions. Below is an example of a simple Data
Consumer:

.. literalinclude:: /_examples/core_bus_example.py
   :language: python
   :pyobject: SimpleConsumer

Data Consumers must register with the Data Bus via the register method:

.. literalinclude:: /_examples/core_bus_example.py
   :language: python
   :pyobject: run
   :emphasize-lines: 5

Data Transformer
++++++++++++++++

Data Transformers are a hybrid of a Data Consumer and a Data Producer. They receive
information from the Data Bus, transform it in some way, and place it back on the
Data Bus to be consumed by another Data Consumer. Data Transformers can be the majority
of logic for a crawler implementation. Below is an example of a simple Data Transformer:

.. literalinclude:: /_examples/core_bus_example.py
   :language: python
   :pyobject: SimpleTransformer


Complete Simple Example
-----------------------

.. literalinclude:: /_examples/core_bus_example.py
   :language: python

Executing the above example will result in following displayed in the console:

.. line-block::
    1,000
    2,000
    3,000
    4,000
    5,000
    6,000
    7,000
    8,000
    9,000
    10,000

:download:`Download the example </_examples/core_bus_example.py>` and try it yourself.