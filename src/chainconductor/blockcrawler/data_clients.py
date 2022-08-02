import re
from re import Pattern
from typing import Set, Dict

import aiohttp
import asyncio

from aiohttp import ClientError


class ProtocolError(Exception):
    pass


class BatchDataClient:
    async def get(self, uris: Set[str]):
        raise NotImplementedError


class HttpBatchDataClient(BatchDataClient):
    async def _get(self, uri: str) -> tuple:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(uri) as response:
                    response.raise_for_status()
                    data = await response.text()
            except ClientError as e:
                data = ProtocolError(f"An error occurred getting data for URI {uri}: {e}")
            return uri, data

    async def get(self, uris: Set[str]) -> Dict[str, str]:
        coroutines = list()
        for uri in uris:
            coroutines.append(self._get(uri))
        results = await asyncio.gather(*coroutines)
        result = {k: v for k, v in results}
        return result


class UriTranslatingBatchDataClient(HttpBatchDataClient):
    def __init__(self, base_uri: str, regex_pattern: Pattern) -> None:
        self.__base_uri: str = base_uri
        self.__regex_pattern: Pattern = regex_pattern

    async def get(self, uris: Set[str]) -> Dict[str, str]:
        http_uris = set()
        uri_translate = dict()
        for uri in uris:
            match = self.__regex_pattern.fullmatch(uri)
            if not match:
                raise ValueError(f"URI {uri} is not a valid")
            http_uri = f"{self.__base_uri}{match.group(1)}"
            uri_translate[http_uri] = uri
            http_uris.add(http_uri)
        http_result = await super().get(http_uris)
        result = {uri_translate[k]: v for k, v in http_result.items()}
        return result


class IpfsBatchDataClient(UriTranslatingBatchDataClient):
    URI_REGEX = re.compile(r"^ipfs://(.+)$")

    def __init__(self, gateway_uri: str) -> None:
        super(IpfsBatchDataClient, self).__init__(f"{gateway_uri}/ipfs/", self.URI_REGEX)


class ArweaveBatchDataClient(UriTranslatingBatchDataClient):
    URI_REGEX = re.compile(r"^ar://(.+)$")

    def __init__(self, gateway_uri: str) -> None:
        super(ArweaveBatchDataClient, self).__init__(f"{gateway_uri}/", self.URI_REGEX)
