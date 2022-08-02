from random import randint
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, AsyncMock, ANY, MagicMock

from aiohttp import ClientError

from chainconductor.blockcrawler.data_clients import (
    HttpBatchDataClient,
    ProtocolError,
    IpfsBatchDataClient,
    ArweaveBatchDataClient,
)

from .. import async_context_manager_mock


class TestHttpDataClient(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        patcher = patch("chainconductor.blockcrawler.data_clients.aiohttp", new=AsyncMock())
        self.__aiohttp_patch = patcher.start()
        self.addAsyncCleanup(patcher.stop)
        self.__aiohttp_patch.ClientSession = async_context_manager_mock()
        self.__aiohttp_session = (
            self.__aiohttp_patch.ClientSession.return_value.__aenter__.return_value
        )
        self.__aiohttp_session.get = async_context_manager_mock()
        self.__aiohttp_session_get_response = (
            self.__aiohttp_session.get.return_value.__aenter__.return_value
        )
        self.__aiohttp_session_get_response.raise_for_status = MagicMock()
        self.__client = HttpBatchDataClient()

    async def test_get_passes_uri_to_multiple_sessions(self):
        calls = randint(10, 99)
        uris = {f"uri{call}" for call in range(calls)}
        await self.__client.get(uris)
        self.assertEqual(
            calls,
            self.__aiohttp_session.get.call_count,
            'Expected the number of "get" calls to equal the number of URIs',
        )
        for uri in uris:
            self.assertIn(
                ((uri,),), self.__aiohttp_session.get.call_args_list, "Expected URI call in calls"
            )

    async def test_gathers_all_sessions(self):
        with patch(
            "chainconductor.blockcrawler.data_clients.asyncio", new=AsyncMock()
        ) as asyncio_patch:
            uris = {f"uri{call}" for call in range(2)}
            await self.__client.get(uris)
            asyncio_patch.gather.assert_called_once_with(ANY, ANY)

    async def test_returns_expected_results_with_proper_keys(self):
        self.__aiohttp_session_get_response.text.return_value = "response"
        expected = {"uri 1": "response", "uri 2": "response"}
        actual = await self.__client.get({"uri 1", "uri 2"})
        self.assertEqual(expected, actual)

    async def test_returns_expected_results_when_request_has_error_status(self):
        self.__aiohttp_session_get_response.raise_for_status.side_effect = ClientError("Burn")
        actual = await self.__client.get({"uri 1", "uri 2"})
        self.assertIn("uri 1", actual)
        self.assertIsInstance(actual["uri 1"], ProtocolError)
        self.assertIn("uri 2", actual)
        self.assertIsInstance(actual["uri 2"], ProtocolError)

    async def test_returns_expected_results_when_request_errors(self):
        self.__aiohttp_session.get.return_value.__aenter__.side_effect = ClientError("Burn")
        actual = await self.__client.get({"uri 1", "uri 2"})
        self.assertIn("uri 1", actual)
        self.assertIsInstance(actual["uri 1"], ProtocolError)
        self.assertIn("uri 2", actual)
        self.assertIsInstance(actual["uri 2"], ProtocolError)


class TestIpfsDataClient(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__aiohttp_patcher = patch(
            "chainconductor.blockcrawler.data_clients.aiohttp", new=AsyncMock()
        )
        self.__aiohttp_patch = self.__aiohttp_patcher.start()
        self.addAsyncCleanup(self.__aiohttp_patcher.stop)
        self.__aiohttp_patch.ClientSession = async_context_manager_mock()
        self.__aiohttp_session = (
            self.__aiohttp_patch.ClientSession.return_value.__aenter__.return_value
        )
        self.__aiohttp_session.get = async_context_manager_mock()
        self.__aiohttp_session_get_response = (
            self.__aiohttp_session.get.return_value.__aenter__.return_value
        )
        self.__aiohttp_session_get_response.raise_for_status = MagicMock()
        self.__gateway_uri = "https://gateway.uri"
        self.__client = IpfsBatchDataClient(self.__gateway_uri)

    async def test_get_passes_translated_uri_to_multiple_http_sessions(self):
        calls = randint(10, 99)
        uris = {f"ipfs://hash/{call}" for call in range(calls)}
        await self.__client.get(uris)
        self.assertEqual(
            calls,
            self.__aiohttp_session.get.call_count,
            'Expected the number of "get" calls to equal the number of URIs',
        )
        for uri in uris:
            self.assertIn(
                ((f"{self.__gateway_uri}/ipfs/{uri[7:]}",),),
                self.__aiohttp_session.get.call_args_list,
                "Expected URI call in calls",
            )

    async def test_gathers_all_http_sessions(self):
        with patch(
            "chainconductor.blockcrawler.data_clients.asyncio", new=AsyncMock()
        ) as asyncio_patch:
            uris = {f"ipfs://hash/{call}" for call in range(2)}
            await self.__client.get(uris)
            asyncio_patch.gather.assert_called_once_with(ANY, ANY)

    async def test_returns_expected_results_with_proper_keys(self):
        self.__aiohttp_session_get_response.text.return_value = "response"
        expected = {"ipfs://hash/1": "response", "ipfs://hash/2": "response"}
        actual = await self.__client.get({"ipfs://hash/1", "ipfs://hash/2"})
        self.assertEqual(expected, actual)

    async def test_returns_expected_results_when_request_has_error_status(self):
        self.__aiohttp_session_get_response.raise_for_status.side_effect = ClientError("Burn")
        actual = await self.__client.get({"ipfs://hash/1", "ipfs://hash/2"})
        self.assertIn("ipfs://hash/1", actual)
        self.assertIsInstance(actual["ipfs://hash/1"], ProtocolError)
        self.assertIn("ipfs://hash/2", actual)
        self.assertIsInstance(actual["ipfs://hash/2"], ProtocolError)

    async def test_returns_expected_results_when_request_errors(self):
        self.__aiohttp_session.get.return_value.__aenter__.side_effect = ClientError("Burn")
        actual = await self.__client.get({"ipfs://hash/1", "ipfs://hash/2"})
        self.assertIn("ipfs://hash/1", actual)
        self.assertIsInstance(actual["ipfs://hash/1"], ProtocolError)
        self.assertIn("ipfs://hash/2", actual)
        self.assertIsInstance(actual["ipfs://hash/2"], ProtocolError)

    async def test_raises_value_error_for_non_ipfs_uri(self):
        with self.assertRaises(ValueError):
            await self.__client.get({"invalid IPFS URI"})


class TestArweaveDataClient(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__aiohttp_patcher = patch(
            "chainconductor.blockcrawler.data_clients.aiohttp", new=AsyncMock()
        )
        self.__aiohttp_patch = self.__aiohttp_patcher.start()
        self.addAsyncCleanup(self.__aiohttp_patcher.stop)
        self.__aiohttp_patch.ClientSession = async_context_manager_mock()
        self.__aiohttp_session = (
            self.__aiohttp_patch.ClientSession.return_value.__aenter__.return_value
        )
        self.__aiohttp_session.get = async_context_manager_mock()
        self.__aiohttp_session_get_response = (
            self.__aiohttp_session.get.return_value.__aenter__.return_value
        )
        self.__aiohttp_session_get_response.raise_for_status = MagicMock()
        self.__gateway_uri = "https://gateway.uri"
        self.__client = ArweaveBatchDataClient(self.__gateway_uri)

    async def test_get_passes_translated_uri_to_multiple_http_sessions(self):
        calls = randint(10, 99)
        uris = {f"ar://hash/{call}" for call in range(calls)}
        await self.__client.get(uris)
        self.assertEqual(
            calls,
            self.__aiohttp_session.get.call_count,
            'Expected the number of "get" calls to equal the number of URIs',
        )
        for uri in uris:
            self.assertIn(
                ((f"{self.__gateway_uri}/{uri[5:]}",),),
                self.__aiohttp_session.get.call_args_list,
                "Expected URI call in calls",
            )

    async def test_gathers_all_http_sessions(self):
        with patch(
            "chainconductor.blockcrawler.data_clients.asyncio", new=AsyncMock()
        ) as asyncio_patch:
            uris = {f"ar://hash/{call}" for call in range(2)}
            await self.__client.get(uris)
            asyncio_patch.gather.assert_called_once_with(ANY, ANY)

    async def test_returns_expected_results_with_proper_keys(self):
        self.__aiohttp_session_get_response.text.return_value = "response"
        expected = {"ar://hash/1": "response", "ar://hash/2": "response"}
        actual = await self.__client.get({"ar://hash/1", "ar://hash/2"})
        self.assertEqual(expected, actual)

    async def test_returns_expected_results_when_request_has_error_status(self):
        self.__aiohttp_session_get_response.raise_for_status.side_effect = ClientError("Burn")
        actual = await self.__client.get({"ar://hash/1", "ar://hash/2"})
        self.assertIn("ar://hash/1", actual)
        self.assertIsInstance(actual["ar://hash/1"], ProtocolError)
        self.assertIn("ar://hash/2", actual)
        self.assertIsInstance(actual["ar://hash/2"], ProtocolError)

    async def test_returns_expected_results_when_request_errors(self):
        self.__aiohttp_session.get.return_value.__aenter__.side_effect = ClientError("Burn")
        actual = await self.__client.get({"ar://hash/1", "ar://hash/2"})
        self.assertIn("ar://hash/1", actual)
        self.assertIsInstance(actual["ar://hash/1"], ProtocolError)
        self.assertIn("ar://hash/2", actual)
        self.assertIsInstance(actual["ar://hash/2"], ProtocolError)

    async def test_raises_value_error_for_non_arweave_uri(self):
        with self.assertRaises(ValueError):
            await self.__client.get({"invalid URI"})
