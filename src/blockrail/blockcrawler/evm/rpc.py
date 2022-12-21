from typing import Optional, Union, List, Any

from eth_abi import decode, encode
from eth_utils import decode_hex
from hexbytes import HexBytes

from .types import (
    EVMBlock,
    HexInt,
    EVMLog,
    EVMTransactionReceipt,
    Address,
)
from .util import Function
from ..core.rpc import RPCClient, RPCError


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


class EVMRPCClient(RPCClient):
    async def get_block_number(self) -> HexInt:
        result = await self.send("eth_blockNumber")
        block_number = HexInt(result)
        return block_number

    async def get_block(self, block_num: HexInt) -> EVMBlock:
        result = await self.send("eth_getBlockByNumber", block_num.hex_value, False)
        block = EVMBlock(
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
            transactions=[HexBytes(tx) for tx in result["transactions"]],
            uncles=[HexBytes(uncle) for uncle in result["uncles"]],
        )
        return block

    async def get_transaction_receipt(self, tx_hash: HexBytes) -> EVMTransactionReceipt:
        result = await self.send("eth_getTransactionReceipt", tx_hash.hex())
        logs: List[EVMLog] = list()
        for log in result["logs"]:
            logs.append(
                EVMLog(
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
        receipt = EVMTransactionReceipt(
            transaction_hash=HexBytes(result["transactionHash"]),
            transaction_index=HexInt(result["transactionIndex"]),
            block_hash=HexBytes(result["blockHash"]),
            block_number=HexInt(result["blockNumber"]),
            from_=result["from"],
            to_=result["to"],
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
                raise RPCError(
                    "Response Decode Error",
                    e,
                )
        return response
