import asyncio
from typing import Optional, Union, Dict, List, Set, Tuple, Sequence

import aiohttp
from aiohttp import ClientError
from eth_abi import decode, encode
from eth_utils import decode_hex

from chainconductor.web3.types import Block, HexInt, Log, TransactionReceipt
from chainconductor.web3.util import Function


class EthCall:
    def __init__(
        self,
        identifier: str,
        from_: Optional[str],
        to: str,
        function: Function,
        parameters: Optional[list] = None,
        block: Optional[Union[str, int]] = "latest",
    ):
        self.__identifier = identifier
        self.__from = from_
        self.__to = to
        self.__function = function
        self.__parameters = list() if parameters is None else parameters.copy()
        self.__block = block

    def __repr__(self) -> str:  # pragma: no cover
        return (
            str(self.__class__)
            + {
                "identifier": self.__identifier,
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
            and self.identifier == other.identifier
            and self.from_ == other.from_
            and self.to == other.to
            and self.function == other.function
            and self.parameters == other.parameters
            and self.block == other.block
        )

    @property
    def identifier(self):
        return self.__identifier

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


class RPCError(Exception):
    pass


class RPCTransportError(RPCError):
    pass


class RPCServerError(RPCError):
    def __init__(self, rpc_version, request_id, error_code, error_message) -> None:
        super().__init__(f"RPC {rpc_version} - Req {request_id} - {error_code}: {error_message}")
        self.__rpc_version = rpc_version
        self.__request_id = request_id
        self.__error_code = error_code
        self.__error_message = error_message

    @property
    def rpc_version(self):
        return self.__rpc_version

    @property
    def request_id(self):
        return self.__request_id

    @property
    def error_code(self):
        return self.__error_code

    @property
    def error_message(self):
        return self.__error_message


class RPCResponse:
    def __init__(self, rpc_version, request_id, result):
        self.__rpc_version = rpc_version
        self.__request_id = request_id
        self.__result = result

    @property
    def rpc_version(self):
        return self.__rpc_version

    @property
    def request_id(self):
        return self.__request_id

    @property
    def result(self):
        return self.__result


class RPCClient:
    def __init__(self, provider_url) -> None:
        self.__nonce = 0
        self.__provider_url = provider_url

    async def __call(self, method, *params) -> RPCResponse:
        request = self.__get_rpc_request(method, params)
        return await self.__call_rpc_single(request)

    async def __call_rpc(self, rpc_request: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.__provider_url, json=rpc_request) as response:
                    return await response.json()

        except (ClientError, asyncio.TimeoutError) as cause:
            raise RPCTransportError(cause)

    async def __call_rpc_single(self, rpc_request: Dict) -> RPCResponse:
        response = await self.__call_rpc(rpc_request)
        if isinstance(response, list):
            raise RPCError("Unexpected response returned")
        rpc_response = self.__get_rpc_response_single(response)

        if isinstance(rpc_response, RPCError):
            raise rpc_response
        if not isinstance(rpc_response, RPCResponse):
            raise RPCError(f"Unexpected response returned {rpc_response}")
        return rpc_response

    async def __call_rpc_batch(
        self, rpc_request: List[Dict]
    ) -> List[Union[RPCResponse, RPCServerError]]:
        response = await self.__call_rpc(rpc_request)
        if isinstance(response, RPCError):
            raise response
        if not isinstance(response, list):
            raise RPCError(f"Unexpected response returned {response}")
        rpc_response = self.__get_rpc_response_batch(response)
        return rpc_response

    def __get_rpc_request(self, method, params):
        data = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self.__nonce,
        }
        self.__nonce += 1
        return data

    @staticmethod
    def __get_rpc_response_single(response: Dict) -> Union[RPCError, RPCResponse]:
        if "error" in response:
            return RPCServerError(
                response["jsonrpc"],
                response["id"],
                response["error"]["code"],
                response["error"]["message"],
            )
        return RPCResponse(
            response["jsonrpc"],
            response["id"],
            response["result"],
        )

    @classmethod
    def __get_rpc_response_batch(cls, response_items: List[Dict]):
        rpc_response = list()
        for response_item in response_items:
            rpc_response.append(cls.__get_rpc_response_single(response_item))
        return rpc_response

    async def get_block_number(self) -> HexInt:
        rpc_response = await self.__call("eth_blockNumber")
        block_number = HexInt(rpc_response.result)
        return block_number

    async def get_blocks(self, block_nums: Set[int]) -> List[Block]:
        rpc_requests = list()
        for block_num in block_nums:
            rpc_requests.append(
                self.__get_rpc_request("eth_getBlockByNumber", (hex(block_num), False))
            )
        rpc_responses = await self.__call_rpc_batch(rpc_requests)
        blocks = list()
        for rpc_response in rpc_responses:
            if isinstance(rpc_response, RPCError):
                raise rpc_response
            transactions: List[str] = rpc_response.result["transactions"]
            blocks.append(
                Block(
                    number=rpc_response.result["number"],
                    hash=rpc_response.result["hash"],
                    parent_hash=rpc_response.result["parentHash"],
                    nonce=rpc_response.result["nonce"],
                    sha3_uncles=rpc_response.result["sha3Uncles"],
                    logs_bloom=rpc_response.result["logsBloom"],
                    transactions_root=rpc_response.result["transactionsRoot"],
                    state_root=rpc_response.result["stateRoot"],
                    receipts_root=rpc_response.result["receiptsRoot"],
                    miner=rpc_response.result["miner"],
                    mix_hash=rpc_response.result["mixHash"],
                    difficulty=rpc_response.result["difficulty"],
                    total_difficulty=rpc_response.result["totalDifficulty"],
                    extra_data=rpc_response.result["extraData"],
                    size=rpc_response.result["size"],
                    gas_limit=rpc_response.result["gasLimit"],
                    gas_used=rpc_response.result["gasUsed"],
                    timestamp=rpc_response.result["timestamp"],
                    transactions=transactions[:],
                    uncles=rpc_response.result["uncles"],
                )
            )
        return blocks

    async def get_transaction_receipts(self, tx_hashes: List[str]) -> List[TransactionReceipt]:
        rpc_requests = list()
        for tx_hash in tx_hashes:
            rpc_requests.append(self.__get_rpc_request("eth_getTransactionReceipt", (tx_hash,)))
        rpc_responses: Sequence[Union[RPCResponse, RPCError]] = await self.__call_rpc_batch(
            rpc_requests
        )
        receipts: List[TransactionReceipt] = list()
        for rpc_response in rpc_responses:
            logs: List[Log] = list()
            if isinstance(rpc_response, RPCError):
                raise RPCError
            for log in rpc_response.result["logs"]:
                logs.append(
                    Log(
                        removed=log["removed"],
                        log_index=log["logIndex"],
                        transaction_index=log["transactionIndex"],
                        transaction_hash=log["transactionHash"],
                        block_hash=log["blockHash"],
                        block_number=log["blockNumber"],
                        address=log["address"],
                        data=log["data"],
                        topics=log["topics"].copy(),
                    )
                )
            receipts.append(
                TransactionReceipt(
                    transaction_hash=rpc_response.result["transactionHash"],
                    transaction_index=rpc_response.result["transactionIndex"],
                    block_hash=rpc_response.result["blockHash"],
                    block_number=rpc_response.result["blockNumber"],
                    from_=rpc_response.result["from"],
                    to_=rpc_response.result["to"],
                    cumulative_gas_used=rpc_response.result["cumulativeGasUsed"],
                    gas_used=rpc_response.result["gasUsed"],
                    contract_address=rpc_response.result["contractAddress"],
                    logs=logs,
                    logs_bloom=rpc_response.result["logsBloom"],
                    root=rpc_response.result["root"] if "root" in rpc_response.result else None,
                    status=rpc_response.result["status"]
                    if "status" in rpc_response.result
                    else None,
                )
            )
        return receipts

    async def calls(self, requests: List[EthCall]) -> Dict[str, Union[Tuple, RPCServerError, None]]:
        rpc_requests = list()
        rpc_request_id_lookup: Dict[int, EthCall] = dict()
        for request in requests:
            if len(request.parameters) == 0:
                encoded_params = ""
            else:
                encoded_param_bytes = encode(request.function.param_types, request.parameters)
                encoded_params = encoded_param_bytes.hex()

            call_data = f"{request.function.function_hash}{encoded_params}"
            rpc_request = self.__get_rpc_request(
                "eth_call",
                (
                    {"from": request.from_, "to": request.to, "data": call_data},
                    request.block,
                ),
            )
            rpc_request_id_lookup[rpc_request["id"]] = request
            rpc_requests.append(rpc_request)

        rpc_responses = await self.__call_rpc_batch(rpc_requests)
        responses: Dict[str, Union[Tuple, RPCServerError, None]] = dict()
        for rpc_response in rpc_responses:
            response_request = rpc_request_id_lookup[rpc_response.request_id]
            if isinstance(rpc_response, RPCServerError):
                response = rpc_response
            else:
                encoded_response: str = rpc_response.result
                response_request = rpc_request_id_lookup[rpc_response.request_id]
                if len(response_request.function.return_types) == 0:
                    response = None
                else:
                    try:
                        encoded_response_bytes = decode_hex(encoded_response)
                        response = decode(
                            response_request.function.return_types,
                            encoded_response_bytes,
                        )
                    except Exception as e:
                        response = RPCServerError(
                            rpc_response.rpc_version,
                            rpc_response.request_id,
                            "Response Decode Error",
                            str(e),
                        )
            responses[response_request.identifier] = response
        return responses
