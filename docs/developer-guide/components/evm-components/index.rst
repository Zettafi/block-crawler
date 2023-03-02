EVM Components
==============

EVM components are the building blocks for building your own crawler against EVM blockchains.
They include:

* :doc:`producer-components` to send EVM-specific data to the Data Bus
* :doc:`transformer-components` to receive EVM-specific data and transform that data
  before sending it to the Data Bus
* :doc:`rpc-components` to communicate with EVM RPC APIs
* :doc:`service-components` to provide domain specific logic
* :doc:`utility-components` to provide tools for service components



.. toctree::
   :hidden:

   producer-components
   transformer-components
   rpc-components
   service-components
   utility-components