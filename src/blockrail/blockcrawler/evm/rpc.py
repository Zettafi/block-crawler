import math
from typing import Optional, Union, List, Any, AsyncIterable

from eth_abi import decode, encode
from eth_utils import decode_hex
from hexbytes import HexBytes

from .types import (
    EvmBlock,
    HexInt,
    EvmLog,
    EvmTransactionReceipt,
    Address,
    EvmTransaction,
)
from .util import Function
from ..core.rpc import RpcClient, RpcServerError, RpcDecodeError
from ..core.stats import StatsService


class EthCall:
    def __init__(
        self,
        from_: Optional[str],
        to: str,
        function: Function,
        parameters: Optional[list] = None,
        block: Optional[Union[str, int]] = "latest",
    ):
        self.__from = from_
        self.__to = to
        self.__function = function
        self.__parameters = list() if parameters is None else parameters.copy()
        self.__block = block

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
    pass


class EvmRpcClient(RpcClient):
    STAT_GET_BLOCK_NUMBER = "rpc.eth.block_number"
    STAT_GET_BLOCK = "rpc.eth.get_block_by_number"
    STAT_GET_TRANSACTION_RECEIPT = "rpc.eth.get_transaction_receipt"
    STAT_CALL = "rpc.eth.call"
    STAT_GET_LOGS = "rpc.eth.get_logs"

    async def get_block_number(self) -> HexInt:
        result = await self.send("eth_blockNumber")
        block_number = HexInt(result)
        return block_number

    async def get_block(self, block_num: HexInt, full_transactions: bool = False) -> EvmBlock:
        result = await self.send("eth_getBlockByNumber", block_num.hex_value, full_transactions)
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

    async def get_transaction_receipt(self, tx_hash: HexBytes) -> EvmTransactionReceipt:
        result = await self.send("eth_getTransactionReceipt", tx_hash.hex())
        logs: List[EvmLog] = list()
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
        self, topics: list, from_block: HexInt, to_block: HexInt, address: Address
    ) -> AsyncIterable[EvmLog]:
        current_block = from_block
        while current_block <= to_block:
            block_range_size = 100_000
            processed = False
            while not processed:
                end_block = current_block + block_range_size - 1
                if end_block > to_block:
                    end_block = to_block
                    block_range_size = 1 + end_block - current_block
                try:
                    logs = await self.send(
                        "eth_getLogs",
                        dict(
                            topics=topics,
                            fromBlock=current_block.hex_value,
                            toBlock=end_block.hex_value,
                            address=str(address),
                        ),
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
                    ):
                        old_block_range_size = block_range_size
                        block_range_size = math.floor(block_range_size / 10)
                        if old_block_range_size <= block_range_size:
                            raise
                    else:
                        raise


class ConnectionPoolingEvmRpcClient(EvmRpcClient):

    # noinspection PyMissingConstructor
    def __init__(
        self,
        provider_url: str,
        stats_service: StatsService,
        requests_per_second: Optional[int] = None,
        connection_pool_size=10,
    ) -> None:
        self.__pool: List[EvmRpcClient] = list()
        for _ in range(connection_pool_size):
            self.__pool.append(EvmRpcClient(provider_url, stats_service, requests_per_second))
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
        self.__pool_index += 1
        if self.__pool_index >= self.__pool_length:
            self.__pool_index = 0
        return await self.__pool[self.__pool_index].send(method, *params)


class MultiProviderEvmRpcClient(EvmRpcClient):

    # noinspection PyMissingConstructor
    def __init__(
        self,
        provider_urls: List[str],
        stats_service: StatsService,
        requests_per_second: Optional[int] = None,
    ) -> None:
        self.__pool: List[EvmRpcClient] = list()
        for provider_url in provider_urls:
            self.__pool.append(EvmRpcClient(provider_url, stats_service, requests_per_second))
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
        self.__pool_index += 1
        if self.__pool_index >= self.__pool_length:
            self.__pool_index = 0
        return await self.__pool[self.__pool_index].send(method, *params)
