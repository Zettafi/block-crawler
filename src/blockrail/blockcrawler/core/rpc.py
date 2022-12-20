import asyncio
import warnings
from asyncio import Future
from math import floor
from typing import Dict, List, Optional

import aiohttp


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
    # TODO: Gracefully handle ConnectionResetError from
    def __init__(self, provider_url: str, requests_per_second: Optional[int] = None) -> None:
        self.__provider_url: str = provider_url
        self.__nonce: int = 0
        self.__requests: List[Dict] = list()
        self.__ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.__pending: Dict[str, Future] = dict()
        self.__context_manager_running: bool = False
        self.__requests_per_second: Optional[int] = requests_per_second
        self.__this_second: int = 0
        self.__requests_this_second: int = 0

    async def __aenter__(self):
        self.__client = aiohttp.ClientSession()
        self.__ws = await self.__client.ws_connect(self.__provider_url)
        self.__inbound_loop_task = asyncio.create_task(
            self.__outbound_loop(self.__ws), name="outbound"
        )
        self.__outbound_loop_task = asyncio.create_task(
            self.__inbound_loop(self.__ws), name="inbound"
        )
        self.__context_manager_running = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.__client.close()
        if self.__pending:
            warnings.warn("Context manger exited with pending responses!")
        self.__context_manager_running = False

    async def __outbound_loop(self, wsc: aiohttp.ClientWebSocketResponse):
        while True:
            if self.__requests:
                request = self.__requests.pop(0)
                try:
                    await self.__wait_for_ready_to_process(request)
                    await wsc.send_json(request)
                except Exception as e:
                    if isinstance(e, aiohttp.ClientError) or isinstance(e, asyncio.TimeoutError):
                        exception: Exception = RPCError(e)
                    else:
                        exception = e
                    future = self.__pending.pop(request["id"])
                    future.set_exception(exception)
            else:
                await asyncio.sleep(0)

    async def __inbound_loop(self, wsc: aiohttp.ClientWebSocketResponse):
        while not wsc.closed:
            try:
                response = await wsc.receive_json()
                if "id" not in response:
                    continue
                    # TODO: Log this error

                try:
                    future = self.__pending.pop(response["id"])
                except KeyError:
                    future.set_exception(
                        RPCError(f'No request with "id" awaiting response: {response}')
                    )
                    continue

                if "error" in response:
                    future.set_exception(
                        RPCServerError(
                            response["jsonrpc"],
                            response["id"],
                            response["error"]["code"],
                            response["error"]["message"],
                        )
                    )
                elif "result" in response:
                    future.set_result(response["result"])
                else:
                    future.set_exception(RPCError(f"No result or error in response: {response}"))

            except TypeError:  # This is for an idiosyncracy in the ws client
                pass

    def __get_rpc_request(self, method, params):
        self.__nonce += 1
        data = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": str(self.__nonce),
        }
        return data

    async def __wait_for_ready_to_process(self, request: dict):
        loop = asyncio.get_running_loop()
        if self.__requests_per_second:

            time = loop.time()
            second = floor(time)

            while (
                second == self.__this_second
                and self.__requests_this_second >= self.__requests_per_second
            ):
                await asyncio.sleep(0)
                second = floor(loop.time())

            if self.__this_second <= second:
                self.__this_second = second
                self.__requests_this_second = 0

            self.__requests_this_second += 1

    def send(self, method, *params) -> Future:
        if not self.__context_manager_running:
            raise RPCError("Requests must be sent using a context manager instance!")
        request = self.__get_rpc_request(method, params)
        future = asyncio.get_running_loop().create_future()
        self.__pending[request["id"]] = future
        self.__requests.append(request)
        return future
