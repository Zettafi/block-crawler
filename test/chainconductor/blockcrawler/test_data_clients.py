import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, AsyncMock, MagicMock

from aiohttp import ClientError

from chainconductor.blockcrawler.data_clients import ProtocolError, ProtocolTimeoutError
from chainconductor.blockcrawler.data_clients import (
    HttpDataClient,
    IpfsDataClient,
    ArweaveDataClient,
)

from .. import async_context_manager_mock


class DataClientBaseTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        patcher = patch("chainconductor.blockcrawler.data_clients.aiohttp", new=AsyncMock())
        self._aiohttp_patch = patcher.start()
        self.addAsyncCleanup(patcher.stop)
        self._aiohttp_patch.ClientTimeout = MagicMock()
        self._aiohttp_patch.ClientSession = async_context_manager_mock()
        self._aiohttp_session = (
            self._aiohttp_patch.ClientSession.return_value.__aenter__.return_value
        )
        self._aiohttp_session.get = async_context_manager_mock()
        self._aiohttp_session_get_response = (
            self._aiohttp_session.get.return_value.__aenter__.return_value
        )
        self._aiohttp_session_get_response.raise_for_status = MagicMock()


class TestHttpDataClient(DataClientBaseTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.__client = HttpDataClient(60)

    async def test_provides_proper_timeout_to_session_timeout(self):
        client = HttpDataClient(1.23)
        await client.get("uri")
        self._aiohttp_patch.ClientTimeout.assert_called_with(total=1.23)

    async def test_provides_session_to_session(self):
        await self.__client.get("uri")
        expected = self._aiohttp_patch.ClientTimeout.return_value
        self._aiohttp_patch.ClientSession.assert_called_with(timeout=expected)

    async def test_get_passes_uri_to_http_client(self):
        expected = "uri"
        await self.__client.get(expected[:])
        self._aiohttp_session.get.assert_called_once_with(expected)

    async def test_returns_expected_results(self):
        expected = "response"
        self._aiohttp_session_get_response.text.return_value = expected[:]
        actual = await self.__client.get("uri")
        self.assertEqual(expected, actual)

    async def test_raises_expected_exception_when_request_has_error_status(self):
        self._aiohttp_session_get_response.raise_for_status.side_effect = ClientError("Burn")
        with self.assertRaisesRegex(ProtocolError, "Burn"):
            await self.__client.get("uri")

    async def test_raises_expected_exception_when_request_errors(self):
        self._aiohttp_session.get.return_value.__aenter__.side_effect = ClientError("Burn")
        with self.assertRaisesRegex(ProtocolError, "Burn"):
            await self.__client.get("uri")

    async def test_raises_expected_exception_when_request_times_out(self):
        self._aiohttp_session.get.return_value.__aenter__.side_effect = asyncio.TimeoutError("Burn")
        with self.assertRaisesRegex(ProtocolTimeoutError, "A timeout occurred for URI"):
            await self.__client.get("uri")


class TestIpfsDataClient(DataClientBaseTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.__gateway_uri = "https://gateway.uri"
        self.__client = IpfsDataClient(self.__gateway_uri, 60)

    async def test_provides_proper_timeout_to_session_timeout(self):
        client = IpfsDataClient(self.__gateway_uri, 1.23)
        await client.get("ipfs://hash/value")
        self._aiohttp_patch.ClientTimeout.assert_called_with(total=1.23)

    async def test_provides_session_to_session(self):
        await self.__client.get("ipfs://hash/value")
        expected = self._aiohttp_patch.ClientTimeout.return_value
        self._aiohttp_patch.ClientSession.assert_called_with(timeout=expected)

    async def test_get_passes_translated_uri_for_valid_ipfs_uri_to_http_session(self):
        uri = "ipfs://hash/value"
        expected = f"{self.__gateway_uri}/ipfs/hash/value"
        await self.__client.get(uri[:])
        self._aiohttp_session.get.assert_called_once_with(expected)

    async def test_get_passes_translated_uri_for_ipfs_uri_with_ipfs_to_http_session(self):
        uri = "ipfs://ipfs/hash/value"
        expected = f"{self.__gateway_uri}/ipfs/hash/value"
        await self.__client.get(uri[:])
        self._aiohttp_session.get.assert_called_once_with(expected)

    async def test_returns_expected_results(self):
        expected = "response"
        self._aiohttp_session_get_response.text.return_value = expected[:]
        actual = await self.__client.get("ipfs://hash/1")
        self.assertEqual(expected, actual)

    async def test_raises_expected_exception_when_request_has_error_status(self):
        self._aiohttp_session_get_response.raise_for_status.side_effect = ClientError("Burn")
        with self.assertRaisesRegex(ProtocolError, "Burn"):
            await self.__client.get("ipfs://hash/1")

    async def test_raises_expected_exception_when_request_errors(self):
        self._aiohttp_session.get.return_value.__aenter__.side_effect = ClientError("Burn")
        with self.assertRaisesRegex(ProtocolError, "Burn"):
            await self.__client.get("ipfs://hash/1")

    async def test_raises_expected_exception_when_request_times_out(self):
        self._aiohttp_session.get.return_value.__aenter__.side_effect = asyncio.TimeoutError("Burn")
        with self.assertRaisesRegex(ProtocolTimeoutError, "A timeout occurred for URI"):
            await self.__client.get("ipfs://hash/1")

    async def test_raises_value_error_for_non_ipfs_uri(self):
        with self.assertRaises(ValueError):
            await self.__client.get("invalid IPFS URI")


class TestArweaveDataClient(DataClientBaseTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.__gateway_uri = "https://gateway.uri"
        self.__client = ArweaveDataClient(self.__gateway_uri, 60)

    async def test_provides_proper_timeout_to_session_timeout(self):
        client = ArweaveDataClient(self.__gateway_uri, 1.23)
        await client.get("ar://hash/value")
        self._aiohttp_patch.ClientTimeout.assert_called_with(total=1.23)

    async def test_provides_session_to_session(self):
        await self.__client.get("ar://hash/value")
        expected = self._aiohttp_patch.ClientTimeout.return_value
        self._aiohttp_patch.ClientSession.assert_called_with(timeout=expected)

    async def test_get_passes_translated_uri_to_http_session(self):
        uri = "ar://hash/1"
        await self.__client.get(uri)
        self._aiohttp_session.get.assert_called_once_with(f"{self.__gateway_uri}/hash/1")

    async def test_returns_expected_result(self):
        expected = "response"
        self._aiohttp_session_get_response.text.return_value = expected[:]
        actual = await self.__client.get("ar://hash/1")
        self.assertEqual(expected, actual)

    async def test_raises_expected_exception_when_request_has_error_status(self):
        self._aiohttp_session_get_response.raise_for_status.side_effect = ClientError("Burn")
        with self.assertRaisesRegex(ProtocolError, "Burn"):
            await self.__client.get("ar://hash/1")

    async def test_raises_expected_exception_when_request_errors(self):
        self._aiohttp_session.get.return_value.__aenter__.side_effect = ClientError("Burn")
        with self.assertRaisesRegex(ProtocolError, "Burn"):
            await self.__client.get("ar://hash/1")

    async def test_raises_expected_exception_when_request_times_out(self):
        self._aiohttp_session.get.return_value.__aenter__.side_effect = asyncio.TimeoutError("Burn")
        with self.assertRaisesRegex(ProtocolTimeoutError, "A timeout occurred for URI"):
            await self.__client.get("ar://hash/1")

    async def test_raises_value_error_for_non_arweave_uri(self):
        with self.assertRaises(ValueError):
            await self.__client.get("invalid URI")
