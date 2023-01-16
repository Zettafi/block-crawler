import asyncio
import re
import time
import warnings
from asyncio import Future, Task, CancelledError
from math import floor
from re import Pattern
from typing import Dict, List, Optional, Any, Tuple

import aiohttp

from blockrail.blockcrawler.core.stats import StatsService

TOO_MANY_REQUESTS_ERROR_CODES: Dict[int, Pattern] = {
    429: re.compile(r"."),  # Alchemy
    -32005: re.compile(r".*rate"),  # Infura reuses their code...sigh!
}


class RpcError(Exception):
    pass


class RpcTransportError(RpcError):
    pass


class RpcDecodeError(RpcError):
    pass


class RpcServerError(RpcError):
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


class RpcResponse:
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


class RpcClient:

    STAT_CONNECT = "rpc.connect"
    STAT_CONNECTION_RESET = "rpc.connection-reset"
    STAT_RECONNECT = "rpc.reconnect"
    STAT_REQUEST_SENT = "rpc.request-sent"
    STAT_REQUEST_MS = "rpc.request-ms"
    STAT_REQUEST_DELAYED = "rpc-request-delayed"
    STAT_RESPONSE_RECEIVED = "rpc.response-received"
    STAT_RESPONSE_TOO_MANY_REQUESTS = "rpc.response-too-many-requests"
    STAT_RESPONSE_NO_ID = "rpc.response-without-id"
    STAT_RESPONSE_UNKNOWN_ID = "rpc.response-unknown-id"
    STAT_RESPONSE_UNKNOWN_FORMAT = "rpc.response-unknown-format"
    STAT_ORPHANED_REQUESTS = "rpc.orphaned-requests"

    def __init__(
        self,
        provider_url: str,
        stats_service: StatsService,
        requests_per_second: Optional[int] = None,
    ) -> None:
        self._stats_service = stats_service
        self.__inbound_loop_task: Optional[Task] = None
        self.__provider_url: str = provider_url
        self.__nonce: int = 0
        self.__ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self.__pending: Dict[str, Tuple[Future, Dict[str, Any], int]] = dict()
        self.__context_manager_running: bool = False
        self.__requests_per_second: Optional[int] = requests_per_second
        self.__paused_for_too_many_requests: Optional[str] = None
        self.__this_second: int = 0
        self.__requests_this_second: int = 0

    async def __aenter__(self):
        self.__client = aiohttp.ClientSession()
        await self.__connect()
        self.__context_manager_running = True
        return self

    async def __connect(self):
        self._stats_service.increment(self.STAT_CONNECT)
        self.__ws = await self.__client.ws_connect(
            self.__provider_url, max_msg_size=100 * 1024 * 1024
        )

        self.__inbound_loop_task = asyncio.create_task(self.__inbound_loop(), name="inbound")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.__client.close()
        self.__context_manager_running = False

    async def __inbound_loop(self):
        running = True
        loop = asyncio.get_running_loop()
        self.__reconnected = False
        while running:
            while not self.__ws.closed:
                try:
                    response = await self.__ws.receive_json()
                    if "id" not in response:
                        warnings.warn(f'Response received without "id" attribute -- {response}')
                        continue

                    try:
                        future, request, start_time = self.__pending.pop(response["id"])
                        end_time = time.perf_counter_ns()
                        duration = int((end_time - start_time) / 1_000_000)
                        self._stats_service.increment(self.STAT_REQUEST_MS, duration)
                        self._stats_service.increment(
                            f"{self.STAT_REQUEST_MS}.{request['method']}", duration
                        )
                    except KeyError:
                        self._stats_service.increment(self.STAT_RESPONSE_UNKNOWN_ID)
                        warnings.warn(f"Response received for unknown id -- {response}")
                        continue

                    self._stats_service.increment(self.STAT_RESPONSE_RECEIVED)
                    if "error" in response:
                        try:
                            if response["error"][
                                "code"
                            ] in TOO_MANY_REQUESTS_ERROR_CODES and TOO_MANY_REQUESTS_ERROR_CODES[
                                response["error"]["code"]
                            ].match(
                                response["error"]["message"]
                            ):

                                if (
                                    "data" in response["error"]
                                    and "backoff_seconds" in response["error"]["data"]
                                ):
                                    backoff_seconds = float(
                                        response["error"]["data"]["backoff_seconds"]
                                    )
                                else:
                                    backoff_seconds = 1.0

                                warnings.warn(
                                    f"Received too many request from RPC API. "
                                    f"Retrying in {backoff_seconds} seconds."
                                )
                                self._stats_service.increment(self.STAT_RESPONSE_TOO_MANY_REQUESTS)
                                self.__paused_for_too_many_requests = request["id"]
                                loop.create_task(
                                    self.__replay_to_many_requests_request(
                                        backoff_seconds, future, request
                                    )
                                )

                            else:
                                future.set_exception(
                                    RpcServerError(
                                        response["jsonrpc"],
                                        response["id"],
                                        response["error"]["code"],
                                        response["error"]["message"],
                                    )
                                )
                        except KeyError as e:
                            warnings.warn(
                                f"Invalid error response received:"
                                f" Missing Key {e} -- {response}"
                            )
                    elif "result" in response:
                        future.set_result(response["result"])
                    else:
                        self._stats_service.increment(self.STAT_RESPONSE_UNKNOWN_FORMAT)
                        future.set_exception(
                            RpcError(f"No result or error in response: {response}")
                        )

                except TypeError:  # This is for an idiosyncrasy in the ws client
                    pass
                except ConnectionResetError:
                    self._stats_service.increment(self.STAT_CONNECTION_RESET)
                    await self.__reconnect()
                    break
                except CancelledError:
                    raise
                except Exception as e:
                    warnings.warn(
                        f"An error occurred processing the transport response -- {e}", source=e
                    )

            if wse := self.__ws.exception():  # Exception means implicit close due to error
                warnings.warn(f"Web Socket exception received: {wse}", source=wse)
                await self.__reconnect()
            else:  # No exception should mean explicit connection close
                running = False
        if not self.__reconnected:
            for _, (future, _) in self.__pending.items():
                self._stats_service.increment(self.STAT_ORPHANED_REQUESTS)
                future.set_exception(RpcError("Transport closed before response received"))

    async def __replay_to_many_requests_request(self, backoff_seconds, future, request):
        await asyncio.sleep(backoff_seconds)
        if self.__paused_for_too_many_requests == request["id"]:
            self.__paused_for_too_many_requests = None
        start_time = await self.__send_json(request)
        self.__pending[request["id"]] = (future, request, start_time)

    async def __send_json(self, request):
        start_time = None
        while not start_time:
            await self.__wait_for_ready_to_send_json()
            try:
                await self.__ws.send_json(request)
                start_time = time.perf_counter_ns()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                raise RpcError(e)
            except ConnectionResetError:
                self._stats_service.increment(self.STAT_CONNECTION_RESET)
                await self.__reconnect()
        self._stats_service.increment(self.STAT_REQUEST_SENT)
        return start_time

    async def __reconnect(self) -> None:
        warnings.warn(
            f"Reconnecting to {self.__provider_url} "
            f"and replaying {len(self.__pending)} requests.",
            RuntimeWarning,
        )
        self._stats_service.increment(self.STAT_RECONNECT)
        replays: List[Tuple[Future, Dict, int]] = list()
        try:
            while True:
                _, value = self.__pending.popitem()
                replays.append(value)
        except KeyError:
            pass
        await self.__connect()
        for future, request, _old_start_time in replays:
            start_time = await self.__send_json(request)
            self.__pending[request["id"]] = (future, request, start_time)
            await asyncio.sleep(0)  # Ensure replaying doesn't stop all other processing
        self.__reconnected = True

    def __get_rpc_request(self, method, params) -> Dict[str, Any]:
        self.__nonce += 1
        data = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": str(self.__nonce),
        }
        return data

    async def __wait_for_ready_to_send_json(self):
        loop = asyncio.get_running_loop()

        delayed = False
        if self.__requests_per_second:
            second = floor(loop.time())

            while (
                second == self.__this_second
                and self.__requests_this_second >= self.__requests_per_second
            ):
                delayed = True
                await asyncio.sleep(0)
                second = floor(loop.time())

            if self.__this_second < second:
                self.__this_second = second
                self.__requests_this_second = 0

            self.__requests_this_second += 1

        while self.__paused_for_too_many_requests:
            delayed = True
            await asyncio.sleep(0)

        if delayed:
            self._stats_service.increment(self.STAT_REQUEST_DELAYED)

    async def send(self, method, *params) -> Any:
        if not self.__context_manager_running or not self.__ws:
            raise RpcError("Requests must be sent using a context manager instance!")

        request = self.__get_rpc_request(method, params)
        start_time = await self.__send_json(request)
        self._stats_service.increment(f"{self.STAT_REQUEST_SENT}.{method}")
        future = asyncio.get_running_loop().create_future()
        self.__pending[request["id"]] = (future, request, start_time)
        return await future
