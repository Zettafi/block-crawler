import asyncio
import time
import unittest
from logging import DEBUG
from typing import Optional
from unittest.mock import patch, AsyncMock, ANY, Mock, call, MagicMock

import ddt
from aiohttp import ClientError

from blockcrawler import LOGGER_NAME
from blockcrawler.core.rpc import RpcClient, RpcClientError
from blockcrawler.core.stats import StatsService


# noinspection PyPep8Naming
@ddt.ddt()
class RPCClientTestCase(unittest.IsolatedAsyncioTestCase):
    def __init__(self, methodName: str) -> None:
        super().__init__(methodName)
        self._ws_response: Optional[dict] = None

    async def _ws_feedback_loop(self, request: dict):
        request_id = request["id"]
        if not self._ws_response:
            raise Exception("Expected _ws_response to be set")
        self._ws_response["id"] = request_id

    async def _ws_response_json(self):
        while not self._ws_response or "id" not in self._ws_response:
            await asyncio.sleep(0)

        response = self._ws_response.copy()
        del self._ws_response["id"]
        return response

    async def _open(self, url, max_msg_size):
        self._ws.closed = False
        self._ws.exception.return_value = None
        await asyncio.sleep(0)
        return self._ws

    async def _close(self):
        self._ws.closed = True
        await asyncio.sleep(0)

    def _get_ws_mock(self):
        ws = AsyncMock()
        ws.exception = Mock(return_value=None)
        ws.send_json.side_effect = self._ws_feedback_loop
        ws.receive_json.side_effect = self._ws_response_json
        return ws

    async def asyncSetUp(self) -> None:
        self._ws_response = dict(result=None)
        patcher = patch("aiohttp.ClientSession")
        patched = patcher.start()
        patched.return_value = self._aiohttp_client_session = AsyncMock()
        self._aiohttp_client_session.close.side_effect = self._close
        self._ws = self._get_ws_mock()
        self._aiohttp_client_session.ws_connect.side_effect = self._open
        self.addAsyncCleanup(patcher.stop)  # type: ignore
        self._stats_service = MagicMock(StatsService)
        patcher = patch("uuid.uuid1")
        self.__uuid1 = patcher.start()
        self.addCleanup(patcher.stop)

    async def asyncTearDown(self) -> None:
        self._ws.closed = True

    async def test_connects_with_expected_uri(self):
        expected = "expected"
        async with RpcClient(expected[:], self._stats_service):
            self._aiohttp_client_session.ws_connect.assert_awaited_once_with(
                expected, max_msg_size=ANY
            )

    async def test_connects_with_no_max_message_size(self):
        async with RpcClient("", self._stats_service):
            self._aiohttp_client_session.ws_connect.assert_awaited_once_with(ANY, max_msg_size=0)

    async def test_sends_expected_params_with_request_when_no_params(self):
        async with RpcClient("", self._stats_service) as rpc_client:
            await rpc_client.send("method")
        self._ws.send_json.assert_awaited_once_with(
            {
                "jsonrpc": ANY,
                "method": ANY,
                "params": tuple(),
                "id": ANY,
            }
        )

    async def test_sends_expected_params_with_request(self):
        async with RpcClient("", self._stats_service) as rpc_client:
            await rpc_client.send("method", "param1", "param2")
        self._ws.send_json.assert_awaited_once_with(
            {
                "jsonrpc": ANY,
                "method": ANY,
                "params": ("param1", "param2"),
                "id": ANY,
            }
        )

    async def test_sends_expects_jsonrpc_version_with_request(self):
        async with RpcClient("", self._stats_service) as rpc_client:
            await rpc_client.send("ANY")  # HERE
        self._ws.send_json.assert_awaited_once_with(
            {
                "jsonrpc": "2.0",
                "method": ANY,
                "params": ANY,
                "id": ANY,
            }
        )

    async def test_sends_different_id_with_each_request(self):
        async with RpcClient("", self._stats_service) as rpc_client:
            await rpc_client.send("ANY")
            await rpc_client.send("ANY")

        id_1, id_2 = [call.args[0]["id"] for call in self._ws.send_json.call_args_list]
        self.assertNotEqual(id_1, id_2)

    async def test_send_pauses_transmission_when_requests_per_second_is_exceeded(self):
        async with RpcClient("", self._stats_service, 1) as rpc_client:
            start = time.perf_counter()
            await asyncio.gather(
                rpc_client.send("a"),
                rpc_client.send("a"),
                rpc_client.send("a"),
            )
            end = time.perf_counter()

            self.assertGreater(end - start, 1.0)

    async def test_send_does_not_pause_transmission_when_requests_per_second_is_not_exceeded(self):
        async with RpcClient("", self._stats_service, 3) as rpc_client:
            start = time.perf_counter()
            await rpc_client.send("a")
            await rpc_client.send("a")
            await rpc_client.send("a")
            end = time.perf_counter()

            self.assertLess(end - start, 1.0)

    async def test_returns_expected_response(self):
        expected = "Expected Result"
        self._ws_response = dict(result=expected[:])
        async with RpcClient("", self._stats_service) as rpc_client:
            actual = await rpc_client.send("method")
            self.assertEqual(expected, actual)

    async def test_warns_on_transport_responses_without_an_id(self):
        async def respond():
            await asyncio.sleep(0)
            return dict()

        self._ws.receive_json.side_effect = respond
        self._ws.send_json.side_effect = None  # Turn off automatic adding of ID1
        self.__uuid1.return_value = "uuid"
        with self.assertLogs(LOGGER_NAME) as cm:
            async with RpcClient("", self._stats_service):
                await asyncio.sleep(0)
                await asyncio.sleep(0)

            self._ws.receive_json.assert_awaited()
            self.assertIn(
                f'ERROR:{LOGGER_NAME}:uuid:Response received without "id" attribute -- {{}}',
                cm.output,
            )

    async def test_warns_on_transport_responses_with_an_unknown_id(self):
        self._ws.send_json.side_effect = None
        self._ws_response = dict(id="9999", result=1)
        self.__uuid1.return_value = "uuid"
        with self.assertLogs(LOGGER_NAME) as cm:
            async with RpcClient("", self._stats_service):
                await asyncio.sleep(0)
            self._ws.receive_json.assert_awaited()
            self.assertIn(
                f"ERROR:{LOGGER_NAME}:uuid:Response received "
                f"for unknown id -- {{'id': '9999', 'result': 1}}",
                cm.output,
            )

    async def test_handles_transport_closed_in_inbound_loop(self):
        closed = False

        async def side_effect(json):
            nonlocal closed
            if not closed:
                closed = True
                self._ws.closed = True
                self._ws.exception.return_value = Exception("Hello")
            await self._ws_feedback_loop(json)

        self._ws.send_json.side_effect = side_effect
        self.__uuid1.return_value = "uuid"

        with self.assertLogs(LOGGER_NAME, DEBUG) as cm:
            async with RpcClient("URI", self._stats_service) as rpc_client:
                await rpc_client.send("hello")
                self.assertEqual(
                    2, self._aiohttp_client_session.ws_connect.await_count, "Expected reconnect"
                )
                self._ws.send_json.assert_has_calls(
                    [
                        call(dict(jsonrpc=ANY, method="hello", params=tuple(), id=ANY)),
                        call(dict(jsonrpc=ANY, method="hello", params=tuple(), id=ANY)),
                    ]
                )
            self.assertIn(
                f"DEBUG:{LOGGER_NAME}:uuid:Reconnecting to URI and replaying 1 requests.", cm.output
            )
            self.assertIn(
                f"DEBUG:{LOGGER_NAME}:uuid:Web Socket exception received: Exception('Hello')",
                cm.output,
            )

    async def test_handles_connection_reset_from_send_json(self):
        errored = False

        async def side_effect(request):
            nonlocal errored
            if not errored:
                errored = True
                raise ConnectionResetError()
            else:
                return await self._ws_feedback_loop(request)

        self._ws.send_json.side_effect = side_effect
        with self.assertLogs(LOGGER_NAME, DEBUG):
            async with RpcClient("URI", self._stats_service) as rpc_client:
                self._ws = new_ws = AsyncMock()
                new_ws.send_json.side_effect = self._ws_feedback_loop
                new_ws.receive_json.side_effect = self._ws_response_json
                await rpc_client.send("hello")
                self.assertEqual(
                    2, self._aiohttp_client_session.ws_connect.await_count, "Expected reconnect"
                )
                new_ws.send_json.assert_called_once_with(
                    dict(jsonrpc=ANY, method="hello", params=tuple(), id=ANY)
                )

    async def test_handles_connection_reset_from_receive_json(self):
        errored = False

        async def side_effect():
            nonlocal errored
            if not errored:
                errored = True
                raise ConnectionResetError()
            else:
                return await self._ws_response_json()

        self._ws.receive_json.side_effect = side_effect
        with self.assertLogs(LOGGER_NAME, DEBUG):
            async with RpcClient("URI", self._stats_service) as rpc_client:
                self._ws = self._get_ws_mock()
                await rpc_client.send("hello")
                self.assertEqual(
                    2, self._aiohttp_client_session.ws_connect.await_count, "Expected reconnect"
                )
                self._ws.send_json.assert_called_once_with(
                    dict(jsonrpc=ANY, method="hello", params=tuple(), id=ANY)
                )

    @ddt.data(
        -32005,  # Infura
        429,  # Alchemy
    )
    async def test_handles_throttling_with_32005_error_and_no_backoff(self, code):
        error_response = {
            "jsonrpc": "2.0",
            "error": {
                "code": code,
                "message": "daily request count exceeded, request rate limited",
            },
        }
        errored = False

        async def side_effect():
            nonlocal errored
            if not errored and "id" in self._ws_response:
                errored = True
                error_response["id"] = self._ws_response["id"]
                return error_response

            return await self._ws_response_json()

        self._ws.receive_json.side_effect = side_effect
        self.__uuid1.return_value = "uuid"

        with self.assertLogs(LOGGER_NAME, DEBUG) as cm:
            async with RpcClient("URI", self._stats_service) as rpc_client:
                await rpc_client.send("hello")
                self._ws.send_json.assert_has_calls(
                    [
                        call(dict(jsonrpc=ANY, method="hello", params=tuple(), id=ANY)),
                        call(dict(jsonrpc=ANY, method="hello", params=tuple(), id=ANY)),
                    ]
                )
                self.assertIn(
                    f"DEBUG:{LOGGER_NAME}:uuid:Received too many request from "
                    f"RPC API URI. Retrying in 1.0 seconds.",
                    cm.output,
                )

    async def test_handles_throttling_with_32005_error_and_backoff(self):
        error_response = {
            "jsonrpc": "2.0",
            "error": {
                "code": -32005,
                "message": "project ID request rate exceeded",
                "data": {
                    "see": "https://infura.io/docs/ethereum/jsonrpc/ratelimits",
                    "current_rps": 13.333,
                    "allowed_rps": 10.0,
                    "backoff_seconds": 0.0,
                },
            },
        }
        errored = False

        async def side_effect():
            await asyncio.sleep(0)  # Allow for processing to happen
            nonlocal errored
            if not errored and "id" in self._ws_response:
                errored = True
                error_response["id"] = self._ws_response["id"]
                return error_response

            return await self._ws_response_json()

        self._ws.receive_json.side_effect = side_effect
        self.__uuid1.return_value = "uuid"

        with self.assertLogs(LOGGER_NAME, DEBUG) as cm:
            async with RpcClient("URI", self._stats_service) as rpc_client:
                await rpc_client.send("hello")
                self._ws.send_json.assert_has_calls(
                    [
                        call(dict(jsonrpc=ANY, method="hello", params=tuple(), id=ANY)),
                        call(dict(jsonrpc=ANY, method="hello", params=tuple(), id=ANY)),
                    ]
                )
            self.assertIn(
                f"DEBUG:{LOGGER_NAME}:uuid:Received too many request from RPC API URI. "
                f"Retrying in 0.0 seconds.",
                cm.output,
            )

    @patch("random.randint")
    async def test_reconnect(self, randint):
        errored = False

        async def send_json_side_effect(request):
            nonlocal errored
            if not errored:
                errored = True
                raise ConnectionResetError()
            else:
                return await self._ws_feedback_loop(request)

        connected = False
        connection_errored = False

        async def ws_connect_side_effect(url, max_msg_size):
            nonlocal connected, connection_errored
            if not connected:
                connected = True
            elif not connection_errored:
                connection_errored = True
                raise ClientError()

            return await self._open(url, max_msg_size)

        self._aiohttp_client_session.ws_connect.side_effect = ws_connect_side_effect
        self._ws.send_json.side_effect = send_json_side_effect
        randint.return_value = 50
        self.__uuid1.return_value = "uuid"
        with self.assertLogs(LOGGER_NAME) as cm:
            async with RpcClient("URI", self._stats_service) as rpc_client:
                await rpc_client.send("hello")
            self.assertIn(
                f"ERROR:{LOGGER_NAME}:uuid:Error connecting:"
                f" ClientError(). Waiting 0.5s to reconnect",
                cm.output,
            )

    async def test_unhandled_exception_in_inbound_loop_raises_rpc_error_on_subsequent_send(self):
        with self.assertLogs():  # We're not checking logs here, just suppressing them from printing
            self._ws.receive_json.side_effect = Exception
            with self.assertRaisesRegex(
                RpcClientError, "No inbound processing loop to react to response for request!"
            ):
                rpc_client = RpcClient("URI", self._stats_service)
                async with rpc_client:
                    await asyncio.sleep(0)
                    await rpc_client.send("hello")
