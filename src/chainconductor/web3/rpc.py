from typing import Union, Dict, List, Set
import ujson as json
import aiohttp
from eth_abi import encode_abi, decode_abi
from eth_utils import decode_hex
from .types import Block, Transaction, HexInt, Log, TransactionReceipt


class RPCError(Exception):
    def __init__(self, error_code, error_message) -> None:
        super().__init__(f"{error_code}: {error_message}")


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
        return await self.__call_rpc(request)

    async def __call_rpc(
        self, rpc_request: Union[Dict, List[Dict]]
    ) -> Union[RPCResponse, List[RPCResponse]]:
        async with aiohttp.ClientSession() as session:
            async with session.post(self.__provider_url, json=rpc_request) as response:
                response_json = await response.json()
                rpc_response = self.__get_rpc_response(response_json)
                return rpc_response

    async def __ws_call(self, method, *params):
        request = self.__get_rpc_request(method, params)
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.__provider_url) as ws:
                await ws.send_json(request)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if msg.data == "close cmd":
                            await ws.close()
                            raise Exception("Remote server requested socket close!")
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        raise Exception(
                            "Remote server returned socket error: " + msg.data
                        )
                    response_json = json.loads(msg.data)
                    rpc_response = self.__get_rpc_response(response_json)
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

    def __get_rpc_response(self, response_json: Dict):
        if "error" in response_json:
            raise RPCError(
                response_json["error"]["code"],
                response_json["error"]["message"],
            )

        if isinstance(response_json, list):
            rpc_response = [
                RPCResponse(
                    response_json["jsonrpc"],
                    response_json["id"],
                    response_json["result"],
                )
                for response_json in response_json
            ]
        else:
            rpc_response = RPCResponse(
                response_json["jsonrpc"],
                response_json["id"],
                response_json["result"],
            )
        return rpc_response

    async def get_block_number(self) -> HexInt:
        rpc_response = await self.__call("eth_blockNumber")
        block_number = HexInt(rpc_response.result)
        return block_number

    async def get_blocks(
        self, block_nums: Set[int], full_transactions: bool = False
    ) -> List[Block]:
        rpc_requests = list()
        for block_num in block_nums:
            rpc_requests.append(
                self.__get_rpc_request(
                    "eth_getBlockByNumber", (hex(block_num), full_transactions)
                )
            )
        rpc_responses = await self.__call_rpc(rpc_requests)
        blocks = list()
        for rpc_response in rpc_responses:
            transactions = list()
            for transaction in rpc_response.result["transactions"]:
                if full_transactions:
                    transactions.append(
                        Transaction(
                            block_hash=transaction["blockHash"],
                            block_number=transaction["blockNumber"],
                            from_=transaction["from"],
                            gas=transaction["gas"],
                            gas_price=transaction["gasPrice"],
                            hash=transaction["hash"],
                            input=transaction["input"],
                            nonce=transaction["nonce"],
                            to_=transaction["to"],
                            transaction_index=transaction["transactionIndex"],
                            value=transaction["value"],
                            v=transaction["v"],
                            r=transaction["r"],
                            s=transaction["s"],
                        )
                    )
                else:
                    transactions.append(transaction)
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
                    difficulty=rpc_response.result["difficulty"],
                    total_difficulty=rpc_response.result["totalDifficulty"],
                    extra_data=rpc_response.result["extraData"],
                    size=rpc_response.result["size"],
                    gas_limit=rpc_response.result["gasLimit"],
                    gas_used=rpc_response.result["gasUsed"],
                    timestamp=rpc_response.result["timestamp"],
                    transactions=transactions.copy(),
                    uncles=rpc_response.result["uncles"],
                )
            )
        return blocks

    async def get_transaction_receipts(
        self, tx_hashes: List[str]
    ) -> List[TransactionReceipt]:
        rpc_requests = list()
        for tx_hash in tx_hashes:
            rpc_requests.append(
                self.__get_rpc_request("eth_getTransactionReceipt", (tx_hash,))
            )
        rpc_responses = await self.__call_rpc(rpc_requests)
        receipts: List[TransactionReceipt] = list()
        logs: List[Log] = list()
        for rpc_response in rpc_responses:
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
                    root=rpc_response.result["root"]
                    if "root" in rpc_response.result
                    else None,
                    status=rpc_response.result["status"]
                    if "status" in rpc_response.result
                    else None,
                )
            )
        return receipts

    async def call(
        self,
        from_: str,
        to_: str,
        method_hash: str,
        parameter_types: List[str],
        params: list,
        response_type=None,
        block: Union[str, int] = "latest",
    ):

        if len(params) == 0:
            encoded_params = ""
        else:
            encoded_param_bytes = encode_abi(
                parameter_types, params
            )
            encoded_params = encoded_param_bytes.hex()

        call_data = f"{method_hash}{encoded_params}"
        rpc_request = self.__get_rpc_request(
            "eth_call",
            ({"from": from_, "to": to_, "data": call_data}, block),
        )
        rpc_response = await self.__call_rpc(rpc_request)
        encoded_response: str = rpc_response.result
        if response_type is None:
            response = None
        else:
            encoded_response_bytes = decode_hex(encoded_response)
            response = decode_abi([response_type], encoded_response_bytes)
        return response

    async def get_code(
        self, address: str, default_block: Union[HexInt, str] = "latest"
    ):
        rpc_request = self.__get_rpc_request(
            "eth_getCode",
            (
                address,
                str(default_block),
            ),
        )
        rpc_response = await self.__call_rpc(rpc_request)
        return rpc_response.result
