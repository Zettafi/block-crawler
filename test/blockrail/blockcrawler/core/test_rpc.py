import asyncio
import time
import unittest
from typing import Optional
from unittest.mock import patch, AsyncMock, ANY

from blockrail.blockcrawler.core.rpc import RPCClient


# noinspection PyPep8Naming
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

    async def asyncSetUp(self) -> None:
        self._ws_response = dict(result=None)
        patcher = patch("aiohttp.ClientSession")
        patched = patcher.start()
        patched.return_value = self._aiohttp_client_session = AsyncMock()
        self._ws = self._aiohttp_client_session.ws_connect.return_value
        self._ws.closed = False
        self._ws.send_json.side_effect = self._ws_feedback_loop
        self._ws.receive_json.side_effect = self._ws_response_json
        self.addAsyncCleanup(patcher.stop)  # type: ignore

    async def asyncTearDown(self) -> None:
        self._ws_response = None
        self._ws.closed = True

    async def test_connects_with_expected_uri(self):
        expected = "expected"
        async with RPCClient(expected[:]):
            self._aiohttp_client_session.ws_connect.assert_awaited_once_with(expected)

    async def test_sends_expected_params_with_request_when_no_params(self):
        async with RPCClient("") as rpc_client:
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
        async with RPCClient("") as rpc_client:
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
        async with RPCClient("") as rpc_client:
            await rpc_client.send("ANY")
        self._ws.send_json.assert_awaited_once_with(
            {
                "jsonrpc": "2.0",
                "method": ANY,
                "params": ANY,
                "id": ANY,
            }
        )

    async def test_sends_different_id_with_each_request(self):
        async with RPCClient("") as rpc_client:
            await rpc_client.send("ANY")
            await rpc_client.send("ANY")

        id_1, id_2 = [call.args[0]["id"] for call in self._ws.send_json.call_args_list]
        self.assertNotEqual(id_1, id_2)

    async def test_send_pauses_transmission_when_requests_per_second_is_exceeded(self):
        async with RPCClient("", 1) as rpc_client:
            start = time.perf_counter()
            await asyncio.gather(
                rpc_client.send("a"),
                rpc_client.send("a"),
                rpc_client.send("a"),
            )
            end = time.perf_counter()

            self.assertGreater(end - start, 1.0)

    async def test_send_does_not_pause_transmission_when_requests_per_second_is_not_exceeded(self):
        async with RPCClient("", 3) as rpc_client:
            start = time.perf_counter()
            await rpc_client.send("a")
            await rpc_client.send("a")
            await rpc_client.send("a")
            end = time.perf_counter()

            self.assertLess(end - start, 1.0)
