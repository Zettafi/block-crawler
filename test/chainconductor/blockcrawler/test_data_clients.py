import asyncio
import base64
from unittest import IsolatedAsyncioTestCase
from unittest.mock import patch, AsyncMock, MagicMock

import ddt
from aiohttp import ClientError

from chainconductor.blockcrawler.data_clients import (
    ProtocolError,
    ProtocolTimeoutError,
    DataUriDataClient,
)
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

    async def test_returns_expected_content_type(self):
        expected = "conten/type"
        self._aiohttp_session_get_response.content_type = expected[:]
        actual, _ = await self.__client.get("ar://hash/1")
        self.assertEqual(expected, actual)

    async def test_returns_expected_data(self):
        expected = "response"
        self._aiohttp_session_get_response.text.return_value = expected[:]
        _, actual = await self.__client.get("uri")
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

    async def test_returns_expected_content_type(self):
        expected = "conten/type"
        self._aiohttp_session_get_response.content_type = expected[:]
        actual, _ = await self.__client.get("ipfs://hash/1")
        self.assertEqual(expected, actual)

    async def test_returns_expected_data(self):
        expected = "response"
        self._aiohttp_session_get_response.text.return_value = expected[:]
        _, actual = await self.__client.get("ipfs://hash/1")
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

    async def test_returns_expected_data(self):
        expected = "response"
        self._aiohttp_session_get_response.text.return_value = expected[:]
        _, actual = await self.__client.get("ar://hash/1")
        self.assertEqual(expected, actual)

    async def test_returns_expected_content_type(self):
        expected = "conten/type"
        self._aiohttp_session_get_response.content_type = expected[:]
        actual, _ = await self.__client.get("ar://hash/1")
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


@ddt.ddt
class TestDataUriDataClient(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__data_client = DataUriDataClient()

    @ddt.data(
        "data:missing comma will fail",  # Missing comma to mark data start
        "data:;base65,data",  # Invalid encoding
        "data:test/text;base64,!!!!!",  # Base64 encoding enabled but data not base 64 encoded
    )
    async def test_invalid_uri_raises_protocol_error(self, uri):
        with self.assertRaises(ProtocolError):
            await self.__data_client.get(uri)

    async def test_response_content_type_is_returned_when_present(self):
        actual, _ = await self.__data_client.get("data:content/type,content")
        self.assertEqual("content/type", actual)

    async def test_response_content_type_is_text_text_when_not_present(self):
        actual, _ = await self.__data_client.get("data:,content")
        self.assertEqual("text/plain", actual)

    @ddt.data(
        "data:mime-type;base64," + base64.b64encode(b"Hello, World!").decode("utf8"),
        "data:;base64," + base64.b64encode(b"Hello, World!").decode("utf8"),
    )
    async def test_base64_encoded_data_is_decoded(self, uri):
        _, actual = await self.__data_client.get(uri)
        self.assertEqual("Hello, World!", actual)

    @ddt.data(
        "data:mime-type,Hello, World!",
        "data:,Hello, World!",
    )
    async def test_non_encoded_data_is_returned_as_is(self, uri):
        _, actual = await self.__data_client.get(uri)
        self.assertEqual("Hello, World!", actual)
