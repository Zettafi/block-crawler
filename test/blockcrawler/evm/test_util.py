import io
import pathlib
from unittest import TestCase
from unittest.mock import Mock, patch, MagicMock, call

from blockcrawler.evm.util import BlockTimeCacheManager


class BlockTimeCacheManagerTestCase(TestCase):
    def setUp(self) -> None:
        self.__writer = MagicMock()
        self.__cache_file = Mock(pathlib.Path)
        self.__cache_manager = BlockTimeCacheManager(self.__cache_file)
        patcher = patch("builtins.open")
        self.__open_patch = patcher.start()
        self.__open_patch.return_value.__enter__.return_value = MagicMock(io.StringIO())
        self.addCleanup(patcher.stop)

    def test_get_block_times_from_cache_opens_file_in_read_mode(self):
        self.__cache_manager.get_block_times_from_cache()
        self.__open_patch.assert_called_once_with(self.__cache_file, "r")

    def test_get_block_times_gets_csv_data_and_creates_list_of_int_int(self):
        self.__open_patch.return_value.__enter__.return_value = ["1,101\n", "2,102"]
        expected = [(1, 101), (2, 102)]
        actual = self.__cache_manager.get_block_times_from_cache()
        self.assertEqual(expected, actual)

    def test_write_block_times_to_cache_opens_file_in_write_plus_mode(self):
        self.__cache_manager.write_block_times_to_cache(list())
        self.__open_patch.assert_called_once_with(self.__cache_file, "w+")

    def test_write_block_times_writes_csv_to_cache_file(self):
        self.__cache_manager.write_block_times_to_cache([(1, 101), (2, 102)])
        self.__open_patch.return_value.__enter__.return_value.write.assert_has_calls(
            [call("1,101\r\n"), call("2,102\r\n")]
        )
