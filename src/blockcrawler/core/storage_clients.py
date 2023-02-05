import abc
from typing import Optional, AsyncContextManager, Any

import aioboto3

from blockcrawler.core.data_clients import DataReader
from blockcrawler.core.stats import StatsService


class StorageClient(abc.ABC):
    @abc.abstractmethod
    async def __aenter__(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError


class StorageClientContext(abc.ABC):
    @abc.abstractmethod
    async def store(self, path: str, data_reader: DataReader, content_type: str):
        raise NotImplementedError


class S3StorageContext(StorageClientContext):
    def __init__(self, bucket, stats_service: StatsService) -> None:
        self.__bucket = bucket
        self.__stats_service = stats_service

    async def store(self, path: str, data_reader: DataReader, content_type: str):
        with self.__stats_service.ms_counter(S3StorageClient.STAT_STORE_MS):
            await self.__bucket.upload_fileobj(
                data_reader,
                Key=path,
                ExtraArgs={"ContentType": content_type},
            )
            self.__stats_service.increment(S3StorageClient.STAT_STORE)


class S3StorageClient(StorageClient):
    STAT_STORE = "s3_store"
    STAT_STORE_MS = "s3_store_ms"

    def __init__(
        self, bucket: str, stats_service: StatsService, region_name=None, endpoint_url=None
    ) -> None:
        self.__bucket = bucket
        self.__stats_service = stats_service
        self.__region_name = region_name
        self.__endpoint_url = endpoint_url
        self.__s3_bucket: Optional[Any] = None
        self.__session_ctm: Optional[AsyncContextManager] = None

    async def __aenter__(self):
        kwargs = {}
        if self.__endpoint_url is not None:
            kwargs["endpoint_url"] = self.__endpoint_url
        if self.__region_name is not None:
            kwargs["region_name"] = self.__region_name
        resource = self.__session_ctm = (
            await aioboto3.Session().resource("s3", **kwargs).__aenter__()
        )
        bucket = await resource.Bucket(self.__bucket)
        return S3StorageContext(bucket, self.__stats_service)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.__session_ctm.__aexit__(exc_type, exc_val, exc_tb)
