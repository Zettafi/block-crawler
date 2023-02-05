from unittest import TestCase

import ddt

from blockcrawler.core.types import HexInt


@ddt.ddt()
class HexIntTestCase(TestCase):
    def test_equal_hex_str_is_equal(self):
        self.assertEqual(HexInt("0x1"), HexInt("0x1"))

    def test_equivalent_hex_str_is_equal(self):
        self.assertEqual(HexInt("0x01"), HexInt("0x1"))

    def test_equal_int_is_equal(self):
        self.assertEqual(HexInt("0x1"), 1)

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
        self.assertEqual("1", {HexInt("0x0"): "1"}[HexInt("0x0")])

    def test_hex_input_maintains_leading_zeros(self):
        self.assertEqual("0x00000000", HexInt("0x00000000").hex_value)

    @ddt.data(
        (4, "0x1", "0x0001"),
        (4, "0x0001", "0x0001"),
        (4, "0x000001", "0x000001"),
        (4, "0x0", "0x0000"),
    )
    @ddt.unpack
    def test_padded_hex_pads_correctly(self, length, before, after):
        self.assertEqual(after, HexInt(before).padded_hex(length))

    def test_lt(self):
        self.assertLess(HexInt(1), HexInt(2))

    def test_lte(self):
        self.assertLessEqual(HexInt(1), HexInt(2))
        self.assertLessEqual(HexInt(1), HexInt(1))

    def test_gt(self):
        self.assertGreater(HexInt(2), HexInt(1))

    def test_gte(self):
        self.assertGreaterEqual(HexInt(2), HexInt(1))
        self.assertGreaterEqual(HexInt(1), HexInt(1))

    def test_add_hex_int(self):
        self.assertEqual(HexInt(2), HexInt(1) + HexInt(1))

    def test_add_int_is_hex_int(self):
        self.assertIsInstance(HexInt(1) + 1, HexInt)

    def test_add_int(self):
        self.assertEqual(HexInt(2), HexInt(1) + 1)

    def test_add_hex_int_is_hex_int(self):
        self.assertIsInstance(HexInt(1) + 1, HexInt)

    def test_radd_int(self):
        self.assertEqual(2, 1 + HexInt(1))

    def test_radd_int_is_int(self):
        self.assertIsInstance(1 + HexInt(1), int)

    def test_sub_hex_int(self):
        self.assertEqual(HexInt(1), HexInt(2) - HexInt(1))

    def test_sub_hex_int_is_hex_int(self):
        self.assertIsInstance(HexInt(2) - HexInt(1), HexInt)

    def test_sub_int(self):
        self.assertEqual(HexInt(1), HexInt(2) - 1)

    def test_sub_int_is_hex_int(self):
        self.assertIsInstance(HexInt(2) - 1, HexInt)

    def test_rsub_int(self):
        self.assertEqual(1, 2 - HexInt(1))

    def test_rsub_int_is_int(self):
        self.assertIsInstance(2 - HexInt(1), int)
