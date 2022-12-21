from unittest import TestCase
from unittest.mock import Mock

import ddt
from hexbytes import HexBytes

from blockrail.blockcrawler.core.entities import HexInt
from blockrail.blockcrawler.evm.types import EvmLog
from blockrail.blockcrawler.evm.util import Erc721Events, Erc1155Events
from blockrail.blockcrawler.nft.entities import TokenTransactionType
from blockrail.blockcrawler.nft.evm.oracles import TokenTransactionTypeOracle, LogVersionOracle


@ddt.ddt
class TransactionTypeOracleTestCase(TestCase):

    LOG_DATA = (
        # 0 address to non-contract address is mint
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000001"),
            TokenTransactionType.MINT,
        ),
        # Contract address to non-contract address is mint
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000001"),
            TokenTransactionType.MINT,
        ),
        # Contract address to 0 address is burn
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            TokenTransactionType.BURN,
        ),
        # 0 address to 0 address is burn
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            TokenTransactionType.BURN,
        ),
        # Con-contract address to 0 address is burn
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000001"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            TokenTransactionType.BURN,
        ),
        # Contract address to contract address is transfer
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            TokenTransactionType.TRANSFER,
        ),
        # 0 address to contract address is transfer
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            TokenTransactionType.TRANSFER,
        ),
        # Non-contract address to Non-contract address is transfer
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000001"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000002"),
            TokenTransactionType.TRANSFER,
        ),
        # Non-contract address to contract address is transfer
        (
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000001"),
            HexBytes("0x0000000000000000000000000000000000000000000000000000000000000099"),
            TokenTransactionType.TRANSFER,
        ),
    )

    @ddt.data(*LOG_DATA)
    @ddt.unpack
    def test_derive_type_derives_correct_type_for_erc721(self, from_, to_, expected_type):
        log = Mock(EvmLog)
        log.address = "0x0000000000000000000000000000000000000099"
        log.topics = [Erc721Events.TRANSFER.event_signature_hash, from_, to_]
        actual = TokenTransactionTypeOracle().type_from_log(log)
        self.assertEqual(expected_type, actual)

    @ddt.data(*LOG_DATA)
    @ddt.unpack
    def test_derive_type_derives_correct_type_for_erc1155_single(self, from_, to_, expected_type):
        log = Mock(EvmLog)
        log.address = "0x0000000000000000000000000000000000000099"
        log.topics = [Erc1155Events.TRANSFER_SINGLE.event_signature_hash, "operator", from_, to_]
        actual = TokenTransactionTypeOracle().type_from_log(log)
        self.assertEqual(expected_type, actual)

    @ddt.data(*LOG_DATA)
    @ddt.unpack
    def test_derive_type_derives_correct_type_for_erc1155_batch(self, from_, to_, expected_type):
        log = Mock(EvmLog)
        log.address = "0x0000000000000000000000000000000000000099"
        log.topics = [Erc1155Events.TRANSFER_BATCH.event_signature_hash, "operator", from_, to_]
        actual = TokenTransactionTypeOracle().type_from_log(log)
        self.assertEqual(expected_type, actual)


class LogVersionOracleTestCase(TestCase):
    def test_returns_correct_version(self):
        log = Mock(EvmLog)
        log.block_number = HexInt(99_888_777)
        log.transaction_index = HexInt(234)
        log.log_index = HexInt(12)
        # noinspection SpellCheckingInspection
        expected = HexInt("0x0000000000000000000000000162e0503e45ceac")
        actual = LogVersionOracle().version_from_log(log)
        self.assertEqual(expected, actual)
