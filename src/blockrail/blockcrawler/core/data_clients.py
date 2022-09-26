import asyncio
import base64
import re
from abc import abstractmethod
from contextlib import asynccontextmanager
from re import Pattern
from typing import AsyncIterator

import aiohttp
from aiohttp import ClientError
from aiohttp.streams import StreamReader


class ProtocolError(Exception):
    pass


class UnsupportedProtocolError(ProtocolError):
    pass


class ProtocolTimeoutError(ProtocolError):
    pass


class DataReader:
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
        data = self.__bytes_data[self.__current_index : read_length]
        self.__current_index += add_current
        return data


class DataClient:
    @asynccontextmanager
    @abstractmethod
    async def get(self, uri: str) -> AsyncIterator:
        """
        Get the data from the URI and return a tuple
        containing response Mime-Type and data stream
        """
        raise NotImplementedError
        yield None


class HttpDataClient(DataClient):
    def __init__(self, request_timeout: float) -> None:
        self.__timeout = aiohttp.ClientTimeout(total=request_timeout)

    @asynccontextmanager
    async def get(self, uri: str) -> AsyncIterator:
        async with aiohttp.ClientSession(timeout=self.__timeout) as session:
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
            except ClientError as e:
                raise ProtocolError(f"An error occurred getting data for URI {uri}: {e}")


class UriTranslatingDataClient(HttpDataClient):
    def __init__(self, base_uri: str, regex_pattern: Pattern, request_timeout: float) -> None:
        super(UriTranslatingDataClient, self).__init__(request_timeout)
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
    URI_REGEX = re.compile(r"^ipfs://(?:ipfs/)?(.+)$")

    def __init__(self, gateway_uri: str, request_timeout: float) -> None:
        super(IpfsDataClient, self).__init__(
            f"{gateway_uri}/ipfs/", self.URI_REGEX, request_timeout
        )


class ArweaveDataClient(UriTranslatingDataClient):
    URI_REGEX = re.compile(r"^ar://(.+)$")

    def __init__(self, gateway_uri: str, request_timeout: float) -> None:
        super(ArweaveDataClient, self).__init__(f"{gateway_uri}/", self.URI_REGEX, request_timeout)


class DataUriDataClient(DataClient):
    URI_REGEX = re.compile(r"^data:(?P<mime_type>[^,;]+)?(?:;(?P<encoding>base64))?,(?P<data>.+)")

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
