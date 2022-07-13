import unittest

from chainconductor.web3.types import HexInt


class HexIntTestCase(unittest.TestCase):
    def test_equal_hex_str_is_equal(self):
        self.assertEqual(HexInt("0x1"), HexInt("0x1"))

    def test_unequal_hex_str_is_not_equal(self):
        self.assertNotEqual(HexInt("0x1"), HexInt("0x0"))

    def test_hex_value_is_original(self):
        self.assertEqual("0x1", HexInt("0x1").hex_value)

    def test_int_value_is_correct(self):
        self.assertEqual(1, HexInt("0x1").int_value)

    def test_str_is_original(self):
        self.assertEqual("0x1", str(HexInt("0x1")))

    def test_hash_is_int(self):
        self.assertEqual(1, HexInt("0x1").__hash__())

    def test_equal_hex_str_is_equal_hash(self):
        self.assertEqual(HexInt("0x1").__hash__(), HexInt("0x1").__hash__())

    def test_unequal_hex_str_is_not_equal_hash(self):
        self.assertNotEqual(HexInt("0x1").__hash__(), HexInt("0x0").__hash__())

    def test_assert_hash_works_in_dict(self):
        self.assertEquals("1", {HexInt("0x0"): "1"}[HexInt("0x0")])
