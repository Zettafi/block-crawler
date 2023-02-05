import unittest
from unittest.mock import AsyncMock, ANY, MagicMock, patch

from blockcrawler.core.data_clients import DataReader
from blockcrawler.core.stats import StatsService
from blockcrawler.core.storage_clients import S3StorageClient


class S3StorageClientTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__s3_bucket = AsyncMock()
        patcher = patch("blockcrawler.core.storage_clients.aioboto3")
        self.__boto3 = patcher.start()
        self.addAsyncCleanup(patcher.stop)  # type: ignore
        self.__s3_ctm = (
            self.__boto3.Session.return_value.resource.return_value.__aenter__.return_value
        )
        self.__s3_bucket = self.__s3_ctm.Bucket.return_value
        self.__stats_service = MagicMock(StatsService)
        self.__data_reader = AsyncMock(DataReader)
        self.__storage_client = S3StorageClient("bucket", self.__stats_service)

    async def test_creates_a_proper_s3_bucket_without_endpoint_url_or_region_name(self):
        async with S3StorageClient("bucket", self.__stats_service):
            pass
        self.__boto3.Session.assert_called_once()
        self.__boto3.Session.return_value.resource.assert_called_once_with("s3")
        self.__s3_ctm.Bucket.assert_awaited_once_with("bucket")

    async def test_creates_a_proper_s3_bucket_with_endpoint_url_and_region_name(self):
        async with S3StorageClient(
            "bucket", self.__stats_service, region_name="region", endpoint_url="url"
        ):
            pass
        self.__boto3.Session.assert_called_once()
        self.__boto3.Session.return_value.resource.assert_called_once_with(
            "s3", region_name="region", endpoint_url="url"
        )
        self.__s3_ctm.Bucket.assert_awaited_once_with("bucket")

    async def test_sends_data_reader_to_s3_bucket(self):
        async with self.__storage_client as storage_client:
            await storage_client.store("", self.__data_reader, "")
        self.__s3_bucket.upload_fileobj.assert_awaited_once_with(
            self.__data_reader,
            Key=ANY,
            ExtraArgs=ANY,
        )

    async def test_sends_content_type_to_s3(self):
        async with self.__storage_client as storage_client:
            await storage_client.store("", self.__data_reader, "expected content type")
        self.__s3_bucket.upload_fileobj.assert_awaited_once_with(
            ANY,
            Key=ANY,
            ExtraArgs=dict(
                ContentType="expected content type",
            ),
        )

    async def test_sends_metadata_with_correct_key_to_s3_bucket(self):
        expected = "blockchain/0x1/0x2/0x3"
        async with self.__storage_client as storage_client:
            await storage_client.store(expected[:], self.__data_reader, "")
        self.__s3_bucket.upload_fileobj.assert_awaited_once_with(
            ANY,
            Key=expected,
            ExtraArgs=ANY,
        )
