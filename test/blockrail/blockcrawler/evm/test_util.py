from unittest import TestCase

from blockrail.blockcrawler.evm.types import Erc165InterfaceID


class TestERC165InterfaceID(TestCase):
    def test_get_by_value_is_enum_member_when_valid_value_provided(self):
        expected = Erc165InterfaceID.ERC721
        actual = Erc165InterfaceID.from_value(Erc165InterfaceID.ERC721.value)
        self.assertEqual(expected, actual)

    def test_get_by_value_is_none_when_invalid_value_provided(self):
        actual = Erc165InterfaceID.from_value("NOT A VALID MEMBER VALUE")
        self.assertIsNone(actual)

    def test_bytes_returns_expected_value(self):
        expected = bytes([0x80, 0xAC, 0x58, 0xCD])
        actual = Erc165InterfaceID.ERC721.bytes
        self.assertEqual(expected, actual)
