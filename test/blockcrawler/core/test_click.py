import unittest
from unittest.mock import MagicMock

import ddt
from click import BadParameter, Parameter
from hexbytes import HexBytes

from blockcrawler.core.click import (
    BlockChainParamType,
    HexIntParamType,
    AddressParamType,
    HexBytesParamType,
    EthereumCollectionTypeParamType,
)
from blockcrawler.core.entities import BlockChain
from blockcrawler.core.types import Address, HexInt
from blockcrawler.nft.entities import EthereumCollectionType, CollectionType


class BlockChainParamTypeTestCase(unittest.TestCase):
    def test_converts_valid_blockchain_into_object(self):
        param_type = BlockChainParamType()
        actual = param_type(BlockChain.ETHEREUM_MAINNET.value)
        expected = BlockChain.ETHEREUM_MAINNET
        self.assertEqual(expected, actual)

    def test_convert_raises_error_with_invalid_blockchain(self):
        with self.assertRaises(BadParameter):
            BlockChainParamType()("bad")

    def test_implements_name(self):
        self.assertIsNotNone(BlockChainParamType().name)


class HexIntParamTypeTestCase(unittest.TestCase):
    def test_converts_valid_hex_into_HexInt(self):
        param_type = HexIntParamType()
        actual = param_type("0x1")
        expected = HexInt(0x1)
        self.assertEqual(expected, actual)

    def test_converts_valid_int_string_into_HexInt(self):
        param_type = HexIntParamType()
        actual = param_type("1")
        expected = HexInt(0x1)
        self.assertEqual(expected, actual)

    def test_converts_valid_int_into_HexInt(self):
        param_type = HexIntParamType()
        actual = param_type(1)
        expected = HexInt(0x1)
        self.assertEqual(expected, actual)

    def test_convert_raises_error_with_float(self):
        with self.assertRaises(BadParameter):
            HexIntParamType()("1.234")

    def test_convert_raises_error_with_invalid_hex_string(self):
        with self.assertRaises(BadParameter):
            HexIntParamType()("0xg")

    def test_convert_raises_error_with_non_hex_string(self):
        with self.assertRaises(BadParameter):
            HexIntParamType()("a12")

    def test_implements_name(self):
        self.assertIsNotNone(HexIntParamType().name)


class AddressParamTypeTestCase(unittest.TestCase):
    def test_converts_valid_address_into_address(self):
        param_type = AddressParamType()
        input_ = "0x0123456789012345678901234567890123456789"
        expected = Address(input_)
        actual = param_type(input_[:])
        self.assertEqual(expected, actual)

    def test_converts_valid_address_letters_into_lowercase(self):
        param_type = AddressParamType()
        input_ = "0xabcdefABCDEFabcdefABCDEFabcdefABCDEFabcd"
        expected = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        actual = param_type(input_)
        self.assertEqual(expected, actual)

    def test_convert_raises_error_with_invalid_hex(self):
        with self.assertRaises(BadParameter):
            AddressParamType()("0x012345678901234567890123456789012345678g")

    def test_convert_raises_error_with_long_hex(self):
        with self.assertRaises(BadParameter):
            AddressParamType()("0x01234567890123456789012345678901234567890")

    def test_convert_raises_error_with_short_hex(self):
        with self.assertRaises(BadParameter):
            AddressParamType()("0x012345678901234567890123456789012345678")

    def test_convert_raises_error_with_non_hex(self):
        with self.assertRaises(BadParameter):
            AddressParamType()("012345678901234567890123456789012345678901")

    def test_convert_raises_error_with_nonstr(self):
        with self.assertRaises(BadParameter):
            AddressParamType()(b"0x0123456789012345678901234567890123456789")

    def test_implements_name(self):
        self.assertIsNotNone(AddressParamType().name)


class HexBytesParamTypeTestCase(unittest.TestCase):
    def test_converts_valid_hex_into_hex_bytes(self):
        param_type = HexBytesParamType()
        input_ = "0x0123456789012345678901234567890123456789"
        expected = HexBytes(input_)
        actual = param_type(input_[:])
        self.assertEqual(expected, actual)

    def test_convert_raises_error_with_invalid_hex(self):
        with self.assertRaises(BadParameter):
            HexBytesParamType()("0x012345678901234567890123456789012345678g")

    def test_convert_raises_error_with_non_hex(self):
        with self.assertRaises(BadParameter):
            HexBytesParamType()("0")

    def test_convert_raises_error_with_nonstr(self):
        with self.assertRaises(BadParameter):
            AddressParamType()(b"0x0123456789012345678901234567890123456789")

    def test_implements_name(self):
        self.assertIsNotNone(AddressParamType().name)


@ddt.ddt
class EthereumCollectionTypeParamTypeTestCase(unittest.TestCase):
    @ddt.data(EthereumCollectionType.ERC721, EthereumCollectionType.ERC1155)
    def test_converts_valid_value_into_collection_type(self, value):
        param_type = EthereumCollectionTypeParamType()
        input_ = value
        expected = CollectionType(value)
        actual = param_type(input_[:])
        self.assertEqual(expected, actual)

    def test_convert_raises_error_with_invalid_value(self):
        with self.assertRaises(BadParameter):
            EthereumCollectionTypeParamType()("NOT VALID")

    def test_allows_default_even_if_not_known(self):
        param_type = EthereumCollectionTypeParamType()
        param = MagicMock(Parameter)
        param.default = "OTHER"
        expected = CollectionType("OTHER")
        actual = param_type("OTHER", param=param)
        self.assertEqual(expected, actual)

    def test_implements_name(self):
        self.assertIsNotNone(EthereumCollectionTypeParamType().name)
