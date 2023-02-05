import abc
import asyncio
import base64
import re
from contextlib import asynccontextmanager
from re import Pattern
from typing import AsyncIterator, cast, Optional

import aiohttp
from aiohttp.streams import StreamReader
from multidict import CIMultiDict

from blockcrawler.core.stats import StatsService


class ProtocolError(Exception):
    pass


class UnsupportedProtocolError(ProtocolError):
    pass


class ProtocolTimeoutError(ProtocolError):
    pass


class ResourceNotFoundProtocolError(ProtocolError):
    pass


class InvalidRequestProtocolError(ProtocolError):
    pass


class TooManyRequestsProtocolError(ProtocolError):
    def __init__(self, *args: object, retry_after: int) -> None:
        super().__init__(*args)
        self.__retry_after = retry_after

    @property
    def retry_after(self):
        return self.__retry_after


class DataReader(abc.ABC):
    @abc.abstractmethod
    async def read(self, size: int = -1):
        raise NotImplementedError


class StreamReaderDataReader(DataReader):
    def __init__(self, stream_reader: StreamReader) -> None:
        self.__stream_reader = stream_reader

    async def read(self, size: int = -1):
        return await self.__stream_reader.read(size)


class BytesDataReader(DataReader):
    def __init__(self, bytes_data: bytes) -> None:
        if bytes_data is None:
            raise ValueError("bytes_data must be bytes")
        self.__bytes_data = bytes_data
        self.__current_index = 0

    async def read(self, size: int = -1):
        if size == -1:
            add_current = len(self.__bytes_data) - self.__current_index
            read_length = None
        else:
            add_current = read_length = size
        data = self.__bytes_data[self.__current_index : read_length]  # NOQA: E203
        self.__current_index += add_current
        return data


class DataClient(abc.ABC):
    # noinspection PyUnreachableCode
    @asynccontextmanager
    @abc.abstractmethod
    async def get(self, uri: str) -> AsyncIterator:
        """
        Get the data from the URI and return a tuple
        containing response Mime-Type and data stream
        """
        raise NotImplementedError
        yield None  # Unreachable but a workaround for typing


class HttpDataClient(DataClient):
    STAT_GET = "http_client_get"
    STAT_GET_MS = "http_client_get_ms"

    def __init__(
        self,
        request_timeout: float,
        stats_service: StatsService,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        self.__timeout = aiohttp.ClientTimeout(total=request_timeout)
        self.__stats_service = stats_service
        if username is not None and password is not None:
            self.__auth: Optional[aiohttp.BasicAuth] = aiohttp.BasicAuth(username, password)
        else:
            self.__auth = None

    @asynccontextmanager
    async def get(self, uri: str) -> AsyncIterator:
        with self.__stats_service.ms_counter(self.STAT_GET_MS):
            async with aiohttp.ClientSession(timeout=self.__timeout, auth=self.__auth) as session:
                try:
                    async with session.get(uri) as response:
                        response.raise_for_status()
                        content_stream = response.content
                        content_type = response.content_type
                        data_reader = StreamReaderDataReader(content_stream)
                        yield content_type, data_reader

                except asyncio.TimeoutError:
                    raise ProtocolTimeoutError(
                        f"A timeout occurred for URI {uri} after {self.__timeout} seconds"
                    )
                except aiohttp.ClientError as e:
                    if isinstance(e, aiohttp.ClientResponseError) and e.status == 404:
                        raise ResourceNotFoundProtocolError(e)
                    elif isinstance(e, aiohttp.ClientResponseError) and e.status == 400:
                        raise InvalidRequestProtocolError(e)
                    elif isinstance(e, aiohttp.ClientResponseError) and e.status == 429:
                        try:
                            headers = cast(CIMultiDict, e.headers)
                            retry = int(headers.getone("Retry-After", 0))
                        except (KeyError, TypeError, ValueError):
                            retry = 0
                        raise TooManyRequestsProtocolError(e, retry_after=retry)
                    raise ProtocolError(f"An error occurred getting data for URI {uri}: {e}")
                finally:
                    self.__stats_service.increment(self.STAT_GET)


class UriTranslatingDataClient(HttpDataClient):
    def __init__(
        self,
        base_uri: str,
        regex_pattern: Pattern,
        request_timeout: float,
        stats_service: StatsService,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        super(UriTranslatingDataClient, self).__init__(
            request_timeout=request_timeout,
            stats_service=stats_service,
            username=username,
            password=password,
        )
        self.__base_uri: str = base_uri
        self.__regex_pattern: Pattern = regex_pattern

    @asynccontextmanager
    async def get(self, uri: str) -> AsyncIterator:
        match = self.__regex_pattern.fullmatch(uri)
        if not match:
            raise ValueError(f"URI {uri} is not a valid")
        http_uri = f"{self.__base_uri}{match.group(1)}"
        async with super().get(http_uri) as result:
            yield result


class IpfsDataClient(UriTranslatingDataClient):
    STAT_GET = "ipfs_client_get"
    STAT_GET_MS = "ipfs_client_get_ms"
    URI_REGEX = re.compile(r"^ipfs://(?:ipfs/)?(.+)$")

    def __init__(
        self,
        gateway_uri: str,
        request_timeout: float,
        stats_service: StatsService,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        super(IpfsDataClient, self).__init__(
            base_uri=f"{gateway_uri}/ipfs/",
            regex_pattern=self.URI_REGEX,
            request_timeout=request_timeout,
            stats_service=stats_service,
            username=username,
            password=password,
        )


class ArweaveDataClient(UriTranslatingDataClient):
    STAT_GET = "arweave_client_get"
    STAT_GET_MS = "arweave_client_get_ms"
    URI_REGEX = re.compile(r"^ar://(.+)$")

    def __init__(
        self, gateway_uri: str, request_timeout: float, stats_service: StatsService
    ) -> None:
        super(ArweaveDataClient, self).__init__(
            f"{gateway_uri}/", self.URI_REGEX, request_timeout, stats_service
        )


class DataUriDataClient(DataClient):
    STAT_GET = "data_uri_client_get"
    STAT_GET_MS = "data_uri_client_get_ms"
    URI_REGEX = re.compile(r"^data:(?P<mime_type>[^,;]+)?(?:;(?P<encoding>base64))?,(?P<data>.+)")

    def __init__(self, stats_service: StatsService) -> None:
        self.__stats_service = stats_service

    @asynccontextmanager
    async def get(self, uri: str) -> AsyncIterator:
        match = self.URI_REGEX.match(uri)
        if not match:
            raise ProtocolError(f"Invalid Data URI: {uri}")
        match_dict = match.groupdict()
        content_type = (
            match_dict["mime_type"] if match_dict["mime_type"] is not None else "text/plain"
        )
        encoding = match_dict["encoding"]
        data = match_dict["data"]
        if data and encoding == "base64":
            data = base64.b64decode(data)
            if data == b"":
                raise ProtocolError(f"Data URI data not base64 encoded: {uri}")
        else:
            data = data.encode("utf8")
        data_reader = BytesDataReader(data)
        yield content_type, data_reader
        self.__stats_service.increment(self.STAT_GET)
