"""EVM specific RPC Clients"""

import logging
import math
from typing import Optional, Union, List, Any, AsyncIterable, Literal

from eth_abi import decode, encode
from eth_utils import decode_hex
from hexbytes import HexBytes

from .types import (
    EvmBlock,
    EvmLog,
    EvmTransactionReceipt,
    EvmTransaction,
    Function,
)
from ..core.types import Address, HexInt
from .. import LOGGER_NAME
from ..core.rpc import RpcClient, RpcServerError, RpcDecodeError, RpcClientError


class EthCall:
    """
    Python representation of the properties of an eth_call to execute a function for a
    smart contract on an Ethereum Virtual Machine (EVM)

    :param from_: Address from which a transaction would originate. This is optional for
        view function calls.
    :param to:  Address of the contract whose function you will be calling.
    :param function: The function class representation of the contract function
    :param parameters: The list of ordered function parameters to send
    :param block: The block height at which to execute the function
    """

    def __init__(
        self,
        from_: Optional[str],
        to: str,
        function: Function,
        parameters: Optional[list] = None,
        block: Optional[
            Union[HexInt, Literal["latest"], Literal["earliest"], Literal["pending"]]
        ] = "latest",
    ):
        self.__from = from_
        self.__to = to
        self.__function = function
        self.__parameters = [] if parameters is None else parameters.copy()
        self.__block = block.hex_value if isinstance(block, HexInt) else block

    def __repr__(self) -> str:  # pragma: no cover
        return (
            str(self.__class__)
            + {
                "from": self.__from,
                "to": self.__to,
                "function": self.__function,
                "parameters": self.__parameters,
                "block": self.__block,
            }.__repr__()
        )

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.from_ == other.from_
            and self.to == other.to
            and self.function == other.function
            and self.parameters == other.parameters
            and self.block == other.block
        )

    @property
    def from_(self):
        return self.__from

    @property
    def to(self):
        return self.__to

    @property
    def function(self):
        return self.__function

    @property
    def parameters(self):
        return self.__parameters.copy()

    @property
    def block(self):
        return self.__block


class CallError(Exception):
    """Exception for issues arising while calling the `call` method"""

    pass


class EvmRpcClient(RpcClient):
    """RPC Client for EVM RPC calls"""

    STAT_GET_BLOCK_NUMBER = "rpc.eth.block_number"
    """Stat name for counts of `eth_blockNumber` RPC calls"""

    STAT_GET_BLOCK = "rpc.eth.get_block_by_number"
    """Stat name for counts of `eth_getBlockByNumber` RPC calls"""

    STAT_GET_TRANSACTION_RECEIPT = "rpc.eth.get_transaction_receipt"
    """Stat name for counts of `eth_getTransactionReceipt` RPC calls"""

    STAT_CALL = "rpc.eth.call"
    """Stat name for counts of `eth_call` RPC calls"""

    STAT_GET_LOGS = "rpc.eth.get_logs"
    """Stat name for counts of `eth_getLogs` RPC calls"""

    async def get_block_number(self) -> HexInt:
        """Get the current block height via
        `eth_blockNumber <https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_blocknumber>`_
        """

        result = await self.send("eth_blockNumber")
        block_number = HexInt(result)
        return block_number

    async def get_block(self, block_num: HexInt, full_transactions: bool = False) -> EvmBlock:
        """Get a block via
        `eth_getBlockByNumber <https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getblockbynumber>`_

        :param block_num: The block number for the block you wish to get.
        :param full_transactions: Return full transactions flag. If True,
            `transactions` attribute on the returned block will contain EVMTransaction
            objects and the `transaction_hashes` attribute will contain transaction hashes. If
            False, the `transaction_hashes` attribute will contain transaction hashes and the
            `transactions` attribute will be None.
        """  # noqa: E501

        result = await self.send("eth_getBlockByNumber", block_num.hex_value, full_transactions)

        if result is None:
            raise RpcClientError("Error retrieving block: no block returned")

        if result["transactions"] is None:
            # Sometimes, nodes return no transactions. Blocks must have transactions.
            raise RpcClientError("Error retrieving block: transactions attribute was null")

        if full_transactions:
            transactions = [
                EvmTransaction(
                    block_hash=HexBytes(tx["blockHash"]),
                    block_number=HexInt(tx["blockNumber"]),
                    from_=Address(tx["from"]),
                    gas=HexInt(tx["gas"]),
                    gas_price=HexInt(tx["gasPrice"]),
                    hash=HexBytes(tx["hash"]),
                    input=HexBytes(tx["input"]),
                    nonce=HexInt(tx["nonce"]),
                    transaction_index=HexInt(tx["transactionIndex"]),
                    v=HexInt(tx["v"]),
                    r=HexBytes(tx["r"]),
                    s=HexBytes(tx["s"]),
                    to_=Address(tx.get("to")),
                    value=HexInt(tx["value"]),
                )
                for tx in result["transactions"]
            ]
            transaction_hashes = [HexBytes(tx["hash"]) for tx in result["transactions"]]
        else:
            transactions = None
            transaction_hashes = [HexBytes(tx) for tx in result["transactions"]]
        block = EvmBlock(
            number=HexInt(result["number"]),
            hash=HexBytes(result["hash"]),
            parent_hash=HexBytes(result["parentHash"]),
            nonce=HexBytes(result["nonce"]),
            sha3_uncles=HexBytes(result["sha3Uncles"]),
            logs_bloom=HexInt(result["logsBloom"]),
            transactions_root=HexBytes(result["transactionsRoot"]),
            state_root=HexBytes(result["stateRoot"]),
            receipts_root=HexBytes(result["receiptsRoot"]),
            miner=Address(result["miner"]),
            mix_hash=HexBytes(result["mixHash"]),
            difficulty=HexInt(result["difficulty"]),
            total_difficulty=HexInt(result["totalDifficulty"]),
            extra_data=HexBytes(result["extraData"]),
            size=HexInt(result["size"]),
            gas_limit=HexInt(result["gasLimit"]),
            gas_used=HexInt(result["gasUsed"]),
            timestamp=HexInt(result["timestamp"]),
            transaction_hashes=transaction_hashes,
            transactions=transactions,
            uncles=[HexBytes(uncle) for uncle in result["uncles"]],
        )
        return block

    async def get_block_by_timestamp(self, timestamp: HexInt) -> EvmBlock:
        """Get block number by timestamp.

        :param timestamp: The timestamp of the block you wish to get.
        :returns: The block which timestamp is equal to given timestamp
            or the nearest block if timestamp is not exact.
        """

        return await self.__get_block_by_timestamp(timestamp)

    async def __get_block_by_timestamp(
        self,
        timestamp: HexInt,
        left_block: Optional[EvmBlock] = None,
        right_block: Optional[EvmBlock] = None,
    ):
        """Recursively get block number by timestamp until exact match.

        :param timestamp: The timestamp of the block you wish to get.
        :param left_block: The left bound block limit of binary search.
        :param right_block: The right bound block limit of binary search.
        :returns: The block which timestamp is equal to given timestamp
            or the nearest block if timestamp is not exact.
        """

        if not left_block or not right_block:
            # Ethereum's Paris Network Upgrade (a.k.a. the Merge!)
            #   Block Number: 15537394
            #   Timestamp: 1663224179
            the_merge_timestamp = HexInt(1663224179)
            the_merge_block_number = HexInt(15537394)

            if timestamp == the_merge_timestamp:
                # Return known value.
                return await self.get_block(the_merge_block_number)

            right_block = await self.get_block(await self.get_block_number())

            # Check if timestamp is later than the network upgrade event
            #   where each block's duration is always greater than or equal to 12
            if timestamp > the_merge_timestamp:
                min_block_duration = HexInt(12)

                estimated_adjustment = (right_block.timestamp - timestamp) / min_block_duration
                estimated_block_number = right_block.number - estimated_adjustment

                # Set left bound block to the estimated block number.
                # Estimated block is always less than or equal to the expected block.
                left_block = await self.get_block(
                    max(estimated_block_number, the_merge_block_number)
                )

            # Use pure binary search for older blocks,
            #   we don't want wrong bounds from inaccurate estimation.
            else:
                left_block = await self.get_block(HexInt(1))

        if left_block == right_block:
            return left_block

        # Return the closer one, if we're already between blocks
        if (
            left_block.number == right_block.number - 1
            or timestamp <= left_block.timestamp
            or timestamp >= right_block.timestamp
        ):
            return (
                left_block
                if abs(timestamp - left_block.timestamp) < abs(timestamp - right_block.timestamp)
                else right_block
            )

        # k is how far inbetween left and right we're expected to be
        k = (timestamp.int_value - left_block.timestamp.int_value) / (
            right_block.timestamp.int_value - left_block.timestamp.int_value
        )
        # We bound, to ensure logarithmic time even when guesses aren't great
        k = min(max(k, 0.05), 0.95)
        # We get the expected block number from K
        estimated_block_number = HexInt(
            round(
                left_block.number.int_value
                + k * (right_block.number.int_value - left_block.number.int_value)
            )
        )
        # Make sure to make some progress
        estimated_block_number = min(
            max(estimated_block_number, left_block.number + 1), right_block.number - 1
        )

        # Get the actual timestamp for that block
        expected_block = await self.get_block(estimated_block_number)

        # print(expected_block.number.int_value)

        # Adjust bound using our estimated block
        if expected_block.timestamp < timestamp:
            left_block = expected_block
        elif expected_block.timestamp > timestamp:
            right_block = expected_block
        else:
            # Return the perfect match
            return expected_block

        # Recurse using tightened bounds
        return await self.__get_block_by_timestamp(
            timestamp,
            left_block,
            right_block,
        )

    async def get_transaction_receipt(self, tx_hash: HexBytes) -> EvmTransactionReceipt:
        """Get a transaction receipt by hash via
        `eth_getTransactionReceipt <https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_gettransactionreceipt>`_

        :param tx_hash: Transaction hash of transaction you wish to retrieve.
        """  # noqa: E501

        result = await self.send("eth_getTransactionReceipt", tx_hash.hex())
        logs: List[EvmLog] = []
        for log in result["logs"]:
            logs.append(
                EvmLog(
                    removed=log["removed"],
                    log_index=HexInt(log["logIndex"]),
                    transaction_index=HexInt(log["transactionIndex"]),
                    transaction_hash=HexBytes(log["transactionHash"]),
                    block_hash=HexBytes(log["blockHash"]),
                    block_number=HexInt(log["blockNumber"]),
                    address=log["address"],
                    data=HexBytes(log["data"]),
                    topics=[HexBytes(topic) for topic in log["topics"]],
                )
            )
        receipt = EvmTransactionReceipt(
            transaction_hash=HexBytes(result["transactionHash"]),
            transaction_index=HexInt(result["transactionIndex"]),
            block_hash=HexBytes(result["blockHash"]),
            block_number=HexInt(result["blockNumber"]),
            from_=result["from"],
            to_=result.get("to"),
            cumulative_gas_used=HexInt(result["cumulativeGasUsed"]),
            gas_used=HexInt(result["gasUsed"]),
            contract_address=result["contractAddress"],
            logs=logs,
            logs_bloom=HexInt(result["logsBloom"]),
            root=HexBytes(result["root"]) if "root" in result else None,
            status=HexInt(result["status"]) if "status" in result else None,
        )
        return receipt

    async def call(self, request: EthCall) -> Any:
        """
        Call a function on a smart contract via
        `eth_call <https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_call>`_

        :param request: Object representation of the call

        :returns: Decoded values returned by the smart contract function.
            If there is no return type for the function, the result will be None.
            Otherwise, it will be a tuple of response types as functions can return
            multiple values. For example::

                (result,) = rpc_client.call(request)

        :raises: CallError

        """

        if len(request.parameters) == 0:
            encoded_params = ""
        else:
            encoded_param_bytes = encode(request.function.param_types, request.parameters)
            encoded_params = encoded_param_bytes.hex()

        call_data = f"{request.function.function_signature_hash.hex()}{encoded_params}"
        result = await self.send(
            "eth_call",
            {"from": request.from_, "to": request.to, "data": call_data},
            request.block,
        )

        encoded_response: str = result
        if len(request.function.return_types) == 0:
            response = None
        else:
            try:
                encoded_response_bytes = decode_hex(encoded_response)
                if encoded_response_bytes == b"":
                    response = (None,)
                else:
                    response = decode(
                        request.function.return_types,
                        encoded_response_bytes,
                    )
            except Exception as e:
                raise RpcDecodeError(
                    "Response Decode Error",
                    e,
                )
        return response

    async def get_logs(
        self,
        topics: List[Union[str, List[str]]],
        from_block: HexInt,
        to_block: HexInt,
        address: Address,
        starting_block_range_size: Optional[int] = None,
    ) -> AsyncIterable[EvmLog]:
        """
        Get logs for the provided topic from one block to another for an address by
        an optional block size via
        `eth_getLogs <https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getlogs>`_.

        This method will react to errors for the endpoint returning an error as the
        block range or result is too large and iterate for as many calls as is
        necessary to return all the logs.

        :param topics: List of topics that are ABI encoded. The list is dependent on
            the log type you wish to search. Explaining how to search logs takes more
            room than this document could provide.
        :param from_block: The lowest block from which to search for logs
        :param to_block: The highest block from which to search for logs
        :param address: The log address to filter.
        :param starting_block_range_size: The starting size for the block range to
            query for the logs. The default size is the entire range. This may be not
            be optimal if the range will house a large number of logs and the method
            will have to react. Optimizing this size can reduce time spent on RPC
            calls that will always error.1

        :returns: An asynchronous iterator to iterate through the logs.

        Here's an example for retrieving all the URI events for a contract::

            async for log in await rpc_client.get_logs(
                [Erc1155Events.URI.event_signature_hash.hex()],
                HexInt(0),
                HexInt(16_734_967),
                Address("0x19c8a3f0b290a36de59a50b4c70a23f9c045ec74"),
            ):
                print(log)
        """
        if not starting_block_range_size:
            starting_block_range_size = to_block.int_value - from_block.int_value + 1
        current_block = from_block
        block_range_size = starting_block_range_size
        while current_block <= to_block:
            processed = False
            while not processed:
                end_block = current_block + block_range_size - 1
                if end_block > to_block:
                    end_block = to_block
                    block_range_size = 1 + end_block - current_block
                try:
                    logs = await self.send(
                        "eth_getLogs",
                        {
                            "topics": topics,
                            "fromBlock": current_block.hex_value,
                            "toBlock": end_block.hex_value,
                            "address": str(address),
                        },
                    )
                    if logs:
                        for log in logs:
                            yield EvmLog(
                                removed=log["removed"],
                                log_index=HexInt(log["logIndex"]),
                                transaction_index=HexInt(log["transactionIndex"]),
                                transaction_hash=HexBytes(log["transactionHash"]),
                                block_hash=HexBytes(log["blockHash"]),
                                block_number=HexInt(log["blockNumber"]),
                                address=Address(log["address"]),
                                data=HexBytes(log["data"]),
                                topics=[HexBytes(topic) for topic in log["topics"]],
                            )
                    current_block = end_block + 1
                    processed = True
                except RpcServerError as e:
                    if e.error_code in (
                        -32005,  # Infura
                        -32602,  # Alchemy
                        -32000,  # Alchemy generic server error which means timeout in the context
                    ):
                        old_block_range_size = block_range_size
                        block_range_size = math.floor(block_range_size / 10)
                        if old_block_range_size <= block_range_size:
                            raise
                    else:
                        raise


class ConnectionPoolingEvmRpcClient(EvmRpcClient):
    """
    Pooled EVM RPC Client. THis client takes a list of EVM RPC clients to use in a
    round-robin strategy for sending RPC requests. The client is for high-volume
    applications that would exceed their request-per-second limit with a single
    connection or even a single provider.

    :param pool: List of RPC clients
    """

    # noinspection PyMissingConstructor
    def __init__(
        self,
        pool: List[EvmRpcClient],
    ) -> None:
        self.__pool = pool[:]
        self.__pool_length = len(self.__pool)
        self.__pool_index = self.__pool_length

    async def __aenter__(self):
        for client in self.__pool:
            await client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for client in self.__pool:
            await client.__aexit__(exc_type, exc_val, exc_tb)

    async def send(self, method, *params) -> Any:
        while self.__pool_length > 0:
            self.__pool_index += 1
            if self.__pool_index >= self.__pool_length:
                self.__pool_index = 0
            try:
                return await self.__pool[self.__pool_index].send(method, *params)
            except RpcClientError:
                logging.getLogger(LOGGER_NAME).error(
                    "RPC Client not able to receive requests. Removing from pool"
                )
                self.__pool.pop(self.__pool_index)
                self.__pool_length -= 1

        raise RpcClientError("Connection pool fully depleted. Unable to send!")
