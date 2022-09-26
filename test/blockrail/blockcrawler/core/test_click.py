import unittest

from click import BadParameter

from blockrail.blockcrawler.core.entities import BlockChain
from blockrail.blockcrawler.core.click import BlockChainParamType


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
