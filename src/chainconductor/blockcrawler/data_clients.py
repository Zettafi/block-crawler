import asyncio
import re
from re import Pattern

import aiohttp
from aiohttp import ClientError


class ProtocolError(Exception):
    pass


class ProtocolTimeoutError(ProtocolError):
    pass


class DataClient:
    async def get(self, uri: str):
        raise NotImplementedError


class HttpDataClient(DataClient):
    def __init__(self, request_timeout: float) -> None:
        self.__timeout = aiohttp.ClientTimeout(total=request_timeout)

    async def get(self, uri: str) -> str:
        async with aiohttp.ClientSession(timeout=self.__timeout) as session:
            try:
                async with session.get(uri) as response:
                    response.raise_for_status()
                    data = await response.text()
            except asyncio.TimeoutError:
                raise ProtocolTimeoutError(
                    f"A timeout occurred for URI {uri} after {self.__timeout} seconds"
                )
            except ClientError as e:
                raise ProtocolError(f"An error occurred getting data for URI {uri}: {e}")
        return data


class UriTranslatingDataClient(HttpDataClient):
    def __init__(self, base_uri: str, regex_pattern: Pattern, request_timeout: float) -> None:
        super(UriTranslatingDataClient, self).__init__(request_timeout)
        self.__base_uri: str = base_uri
        self.__regex_pattern: Pattern = regex_pattern

    async def get(self, uri: str) -> str:
        match = self.__regex_pattern.fullmatch(uri)
        if not match:
            raise ValueError(f"URI {uri} is not a valid")
        http_uri = f"{self.__base_uri}{match.group(1)}"
        result = await super().get(http_uri)
        return result


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
