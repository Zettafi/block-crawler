"""
Contracts
    blockchain - Ethereum Mainnet
    address -
    creation_block
    creation_tx
    date_created
    contract_specification
    abi
"""
import asyncio
import json
from collections import namedtuple
from datetime import datetime
from typing import Coroutine, Set, Dict, Union, List

import csv

import aioboto3
import aiohttp

""" Collect all smart contracts from blockchain
@provider_url Specify url for archive node
@from_block Which block to start from
"""


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
                    if "error" in response_json:
                        raise RPCError(
                            response_json["error"]["code"],
                            response_json["error"]["message"],
                        )
                    rpc_response = RPCResponse(
                        response_json["jsonrpc"],
                        response_json["id"],
                        response_json["result"],
                    )
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

    async def get_blocks(self, block_nums: Set[int], full_transactions: bool = False):
        rpc_requests = list()
        for block_num in block_nums:
            rpc_requests.append(
                self.__get_rpc_request(
                    "eth_getBlockByNumber", (hex(block_num), full_transactions)
                )
            )
        rpc_responses = await self.__call_rpc(rpc_requests)
        blocks = (rpc_response.result for rpc_response in rpc_responses)
        return blocks

    async def get_transaction_receipts(self, tx_hashes: List[str]):
        rpc_requests = list()
        for tx_hash in tx_hashes:
            rpc_requests.append(
                self.__get_rpc_request("eth_getTransactionReceipt", (tx_hash,))
            )
        rpc_responses = await self.__call_rpc(rpc_requests)
        receipts = [rpc_response.result for rpc_response in rpc_responses]
        return receipts


TXRow = namedtuple(
    "TXRow", ["network", "block_number", "tx_hash", "creator", "timestamp"]
)


# noinspection DuplicatedCode
async def process_blocks(rpc_client: RPCClient, dynamodb, block_nums: Set[int]):
    block_count = 0
    transaction_count = 0
    receipt_count = 0
    contract_count = 0
    contracts = await dynamodb.Table("Contracts")
    blocks = await rpc_client.get_blocks(block_nums, full_transactions=True)
    tx_hashes = list()
    rows: Dict[str, TXRow] = dict()
    for block in blocks:
        block_count += 1
        for tx in block["transactions"]:
            if tx["to"] is None:
                transaction_count += 1
                tx_hash = tx["hash"]
                tx_hashes.append(tx_hash)
                creator = tx["from"]
                rows[block["number"]] = TXRow(
                    "Ethereum Mainnet",
                    block["number"],
                    tx_hash,
                    creator,
                    block["timestamp"],
                )

    if len(tx_hashes) > 0:
        receipts = await rpc_client.get_transaction_receipts(tx_hashes)
        while len(receipts) > 0:
            batch_size = 25 if len(receipts) > 25 else len(receipts)
            for _ in range(batch_size):
                async with contracts.batch_writer() as batch:
                    receipt = receipts.pop()
                    receipt_count += 1
                    contract_address = receipt["contractAddress"]
                    tx_row = rows[receipt["blockNumber"]]
                    contract_count += 1
                    await batch.put_item(
                        Item={
                            "blockchain": "Ethereum Mainnet",
                            "address": contract_address,
                            "block_number": tx_row.block_number,
                            "transaction_hash": tx_row.tx_hash,
                            "creator": tx_row.creator,
                            "timestamp": tx_row.timestamp,
                        }
                    )
    return block_count, transaction_count, receipt_count, contract_count


# noinspection DuplicatedCode
async def collect_all_contracts(
    rpc_client: RPCClient, dynamodb_uri, from_block=0, to_block=1000, batch_size=100
):
    dynamo_kwargs = {}
    if dynamodb_uri is not None:  # This would only be in non-deployed environments
        dynamo_kwargs["endpoint_url"] = dynamodb_uri
    session = aioboto3.Session()
    async with session.resource("dynamodb", **dynamo_kwargs) as dynamodb:
        tasks: Set[Coroutine] = set()
        blocks_ids = range(from_block, to_block + 1)
        block_id_batches = [
            blocks_ids[i : i + batch_size]
            for i in range(0, len(blocks_ids), batch_size)
        ]
        for block_id_batch in block_id_batches:
            tasks.add(process_blocks(rpc_client, dynamodb, set(block_id_batch)))

        results = await asyncio.gather(*tasks)
        all_blocks = all_transactions = all_receipts = all_contracts = 0
        for blocks, transactions, receipts, contracts in results:
            all_blocks += blocks
            all_transactions += transactions
            all_receipts += receipts
            all_contracts += contracts
        return all_blocks, all_transactions, all_receipts, all_contracts


if __name__ == "__main__":
    provider_url = (
        "wss://dawn-old-feather.quiknode.pro/6cd19e9c836664d7ed84d98dff8ea26c96b3a596/"
    )
    dynamodb_uri = "http://localhost:8000"
    start = datetime.utcnow()
    loop = asyncio.get_event_loop()
    rpc_client = RPCClient(provider_url)
    blocks, transactions, receipts, contracts = loop.run_until_complete(
        collect_all_contracts(rpc_client, dynamodb_uri, 10_000_000, 10_002_000, 100)
    )
    end = datetime.utcnow()
    print("Total Time:", end - start)
    print("Total Blocks:", blocks)
    print("Total Transactions:", transactions)
    print("Total Transaction Receipts:", receipts)
    print("Total Contracts:", contracts)
