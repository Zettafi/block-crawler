from unittest import TestCase
from unittest.mock import Mock

from hexbytes import HexBytes

from blockcrawler.core.entities import HexInt
from blockcrawler.evm.types import (
    EvmLog,
    EvmTransactionReceipt,
    EvmBlock,
    EvmTransaction,
    Address,
)


class EvmLogTestCase(TestCase):
    def test_equal_values_are_equal(self):
        left = EvmLog(
            removed=True,
            log_index=HexInt("0x01"),
            transaction_index=HexInt("0x02"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x03"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        right = EvmLog(
            removed=True,
            log_index=HexInt("0x01"),
            transaction_index=HexInt("0x02"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x03"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        self.assertEqual(left, right, "Expected objects to be equal")

    def test_different_values_are_unequal(self):
        left = EvmLog(
            removed=True,
            log_index=HexInt("0x01"),
            transaction_index=HexInt("0x02"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x03"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        right = EvmLog(
            removed=False,
            log_index=HexInt("0x01"),
            transaction_index=HexInt("0x02"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x03"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        self.assertNotEqual(left, right, "Expected objects not to be equal")

    def test_same_block_number_and_transaction_index_and_log_index_have_same_hash(self):
        left = EvmLog(
            removed=True,
            log_index=HexInt("0x01"),
            transaction_index=HexInt("0x02"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x03"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        right = EvmLog(
            removed=True,
            log_index=HexInt("0x01"),
            transaction_index=HexInt("0x02"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x03"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        self.assertEqual(left.__hash__(), right.__hash__(), "Expected hashes to be equal")

    def test_different_block_number_has_different_hash(self):
        left = EvmLog(
            removed=True,
            log_index=HexInt("0x01"),
            transaction_index=HexInt("0x02"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x03"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        right = EvmLog(
            removed=True,
            log_index=HexInt("0x01"),
            transaction_index=HexInt("0x02"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x04"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        self.assertNotEqual(left.__hash__(), right.__hash__(), "Expected hashes not to be equal")

    def test_different_transaction_index_has_different_hash(self):
        left = EvmLog(
            removed=True,
            log_index=HexInt("0x01"),
            transaction_index=HexInt("0x02"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x03"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        right = EvmLog(
            removed=True,
            log_index=HexInt("0x01"),
            transaction_index=HexInt("0x03"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x03"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        self.assertNotEqual(left.__hash__(), right.__hash__(), "Expected hashes not to be equal")

    def test_different_log_index_has_different_hash(self):
        left = EvmLog(
            removed=True,
            log_index=HexInt("0x01"),
            transaction_index=HexInt("0x02"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x03"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        right = EvmLog(
            removed=True,
            log_index=HexInt("0x02"),
            transaction_index=HexInt("0x02"),
            transaction_hash=HexBytes("0x1111"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x03"),
            address=Address("the address"),
            data=HexBytes(b"the data"),
            topics=[HexBytes(b"topic 1"), HexBytes(b"topic 2")],
        )
        self.assertNotEqual(left.__hash__(), right.__hash__(), "Expected hashes not to be equal")


class EvmTransactionReceiptTestCase(TestCase):
    def test_equal_values_are_equal(self):
        left = EvmTransactionReceipt(
            transaction_hash=HexBytes("0x1111"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            cumulative_gas_used=HexInt("0x03"),
            gas_used=HexInt("0x04"),
            contract_address=Address("the contract address"),
            logs=[
                EvmLog(
                    removed=True,
                    log_index=HexInt("0x01"),
                    transaction_index=HexInt("0x02"),
                    transaction_hash=HexBytes(b"0x1111"),
                    block_hash=HexBytes(b"block hash 1"),
                    block_number=HexInt("0x03"),
                    address=Address("the address 1"),
                    data=HexBytes(b"the data 1"),
                    topics=[HexBytes(b"topic 1.1"), HexBytes(b"topic 1.2")],
                ),
                EvmLog(
                    removed=False,
                    log_index=HexInt("0x11"),
                    transaction_index=HexInt("0x12"),
                    transaction_hash=HexBytes("0x1112"),
                    block_hash=HexBytes(b"block hash 2"),
                    block_number=HexInt("0x13"),
                    address=Address("the address 2"),
                    data=HexBytes(b"the data 2"),
                    topics=[HexBytes(b"topic 2.1"), HexBytes(b"topic 2.2")],
                ),
            ],
            logs_bloom=HexInt("0x987654"),
            root=HexBytes(b"the root"),
            status=HexInt("0x0"),
        )
        right = EvmTransactionReceipt(
            transaction_hash=HexBytes("0x1111"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            cumulative_gas_used=HexInt("0x03"),
            gas_used=HexInt("0x04"),
            contract_address=Address("the contract address"),
            logs=[
                EvmLog(
                    removed=True,
                    log_index=HexInt("0x01"),
                    transaction_index=HexInt("0x02"),
                    transaction_hash=HexBytes(b"0x1111"),
                    block_hash=HexBytes(b"block hash 1"),
                    block_number=HexInt("0x03"),
                    address=Address("the address 1"),
                    data=HexBytes(b"the data 1"),
                    topics=[HexBytes(b"topic 1.1"), HexBytes(b"topic 1.2")],
                ),
                EvmLog(
                    removed=False,
                    log_index=HexInt("0x11"),
                    transaction_index=HexInt("0x12"),
                    transaction_hash=HexBytes("0x1112"),
                    block_hash=HexBytes(b"block hash 2"),
                    block_number=HexInt("0x13"),
                    address=Address("the address 2"),
                    data=HexBytes(b"the data 2"),
                    topics=[HexBytes(b"topic 2.1"), HexBytes(b"topic 2.2")],
                ),
            ],
            logs_bloom=HexInt("0x987654"),
            root=HexBytes(b"the root"),
            status=HexInt("0x0"),
        )
        self.assertEqual(left, right, "Expected objects to be equal")

    def test_different_value_is_not_equal(self):
        left = EvmTransactionReceipt(
            transaction_hash=HexBytes(b"other"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            cumulative_gas_used=HexInt("0x03"),
            gas_used=HexInt("0x04"),
            contract_address=Address("the contract address"),
            logs=[
                EvmLog(
                    removed=True,
                    log_index=HexInt("0x01"),
                    transaction_index=HexInt("0x02"),
                    transaction_hash=HexBytes(b"0x1111"),
                    block_hash=HexBytes(b"block hash 1"),
                    block_number=HexInt("0x03"),
                    address=Address("the address 1"),
                    data=HexBytes(b"the data 1"),
                    topics=[HexBytes(b"topic 1.1"), HexBytes(b"topic 1.2")],
                ),
                EvmLog(
                    removed=False,
                    log_index=HexInt("0x11"),
                    transaction_index=HexInt("0x12"),
                    transaction_hash=HexBytes("0x1112"),
                    block_hash=HexBytes(b"block hash 2"),
                    block_number=HexInt("0x13"),
                    address=Address("the address 2"),
                    data=HexBytes(b"the data 2"),
                    topics=[HexBytes(b"topic 2.1"), HexBytes(b"topic 2.2")],
                ),
            ],
            logs_bloom=HexInt("0x987654"),
            root=HexBytes(b"the root"),
            status=HexInt("0x0"),
        )
        right = EvmTransactionReceipt(
            transaction_hash=HexBytes("0x1111"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            cumulative_gas_used=HexInt("0x03"),
            gas_used=HexInt("0x04"),
            contract_address=Address("the contract address"),
            logs=[
                EvmLog(
                    removed=True,
                    log_index=HexInt("0x01"),
                    transaction_index=HexInt("0x02"),
                    transaction_hash=HexBytes(b"0x1111"),
                    block_hash=HexBytes(b"block hash 1"),
                    block_number=HexInt("0x03"),
                    address=Address("the address 1"),
                    data=HexBytes(b"the data 1"),
                    topics=[HexBytes(b"topic 1.1"), HexBytes(b"topic 1.2")],
                ),
                EvmLog(
                    removed=False,
                    log_index=HexInt("0x11"),
                    transaction_index=HexInt("0x12"),
                    transaction_hash=HexBytes("0x1112"),
                    block_hash=HexBytes(b"block hash 2"),
                    block_number=HexInt("0x13"),
                    address=Address("the address 2"),
                    data=HexBytes(b"the data 2"),
                    topics=[HexBytes(b"topic 2.1"), HexBytes(b"topic 2.2")],
                ),
            ],
            logs_bloom=HexInt("0x987654"),
            root=HexBytes(b"the root"),
            status=HexInt("0x0"),
        )

        self.assertNotEqual(left, right)

    def test_equal_values_hashes_are_equal(self):
        left = EvmTransactionReceipt(
            transaction_hash=HexBytes("0x1111"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            cumulative_gas_used=HexInt("0x03"),
            gas_used=HexInt("0x04"),
            contract_address=Address("the contract address"),
            logs=[
                EvmLog(
                    removed=True,
                    log_index=HexInt("0x01"),
                    transaction_index=HexInt("0x02"),
                    transaction_hash=HexBytes(b"0x1111"),
                    block_hash=HexBytes(b"block hash 1"),
                    block_number=HexInt("0x03"),
                    address=Address("the address 1"),
                    data=HexBytes(b"the data 1"),
                    topics=[HexBytes(b"topic 1.1"), HexBytes(b"topic 1.2")],
                ),
                EvmLog(
                    removed=False,
                    log_index=HexInt("0x11"),
                    transaction_index=HexInt("0x12"),
                    transaction_hash=HexBytes("0x1112"),
                    block_hash=HexBytes(b"block hash 2"),
                    block_number=HexInt("0x13"),
                    address=Address("the address 2"),
                    data=HexBytes(b"the data 2"),
                    topics=[HexBytes(b"topic 2.1"), HexBytes(b"topic 2.2")],
                ),
            ],
            logs_bloom=HexInt("0x987654"),
            root=HexBytes(b"the root"),
            status=HexInt("0x0"),
        )
        right = EvmTransactionReceipt(
            transaction_hash=HexBytes("0x1111"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            cumulative_gas_used=HexInt("0x03"),
            gas_used=HexInt("0x04"),
            contract_address=Address("the contract address"),
            logs=[
                EvmLog(
                    removed=True,
                    log_index=HexInt("0x01"),
                    transaction_index=HexInt("0x02"),
                    transaction_hash=HexBytes(b"0x1111"),
                    block_hash=HexBytes(b"block hash 1"),
                    block_number=HexInt("0x03"),
                    address=Address("the address 1"),
                    data=HexBytes(b"the data 1"),
                    topics=[HexBytes(b"topic 1.1"), HexBytes(b"topic 1.2")],
                ),
                EvmLog(
                    removed=False,
                    log_index=HexInt("0x11"),
                    transaction_index=HexInt("0x12"),
                    transaction_hash=HexBytes("0x1112"),
                    block_hash=HexBytes(b"block hash 2"),
                    block_number=HexInt("0x13"),
                    address=Address("the address 2"),
                    data=HexBytes(b"the data 2"),
                    topics=[HexBytes(b"topic 2.1"), HexBytes(b"topic 2.2")],
                ),
            ],
            logs_bloom=HexInt("0x987654"),
            root=HexBytes(b"the root"),
            status=HexInt("0x0"),
        )
        self.assertEqual(left, right, "Expected objects to be equal")

    def test_different_transaction_hashes_hash_is_not_equal(self):
        left = EvmTransactionReceipt(
            transaction_hash=HexBytes(b"other"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            cumulative_gas_used=HexInt("0x03"),
            gas_used=HexInt("0x04"),
            contract_address=Address("the contract address"),
            logs=[
                EvmLog(
                    removed=True,
                    log_index=HexInt("0x01"),
                    transaction_index=HexInt("0x02"),
                    transaction_hash=HexBytes(b"0x1111"),
                    block_hash=HexBytes(b"block hash 1"),
                    block_number=HexInt("0x03"),
                    address=Address("the address 1"),
                    data=HexBytes(b"the data 1"),
                    topics=[HexBytes(b"topic 1.1"), HexBytes(b"topic 1.2")],
                ),
                EvmLog(
                    removed=False,
                    log_index=HexInt("0x11"),
                    transaction_index=HexInt("0x12"),
                    transaction_hash=HexBytes("0x1112"),
                    block_hash=HexBytes(b"block hash 2"),
                    block_number=HexInt("0x13"),
                    address=Address("the address 2"),
                    data=HexBytes(b"the data 2"),
                    topics=[HexBytes(b"topic 2.1"), HexBytes(b"topic 2.2")],
                ),
            ],
            logs_bloom=HexInt("0x987654"),
            root=HexBytes(b"the root"),
            status=HexInt("0x0"),
        )
        right = EvmTransactionReceipt(
            transaction_hash=HexBytes("0x1111"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            cumulative_gas_used=HexInt("0x03"),
            gas_used=HexInt("0x04"),
            contract_address=Address("the contract address"),
            logs=[
                EvmLog(
                    removed=True,
                    log_index=HexInt("0x01"),
                    transaction_index=HexInt("0x02"),
                    transaction_hash=HexBytes(b"0x1111"),
                    block_hash=HexBytes(b"block hash 1"),
                    block_number=HexInt("0x03"),
                    address=Address("the address 1"),
                    data=HexBytes(b"the data 1"),
                    topics=[HexBytes(b"topic 1.1"), HexBytes(b"topic 1.2")],
                ),
                EvmLog(
                    removed=False,
                    log_index=HexInt("0x11"),
                    transaction_index=HexInt("0x12"),
                    transaction_hash=HexBytes("0x1112"),
                    block_hash=HexBytes(b"block hash 2"),
                    block_number=HexInt("0x13"),
                    address=Address("the address 2"),
                    data=HexBytes(b"the data 2"),
                    topics=[HexBytes(b"topic 2.1"), HexBytes(b"topic 2.2")],
                ),
            ],
            logs_bloom=HexInt("0x987654"),
            root=HexBytes(b"the root"),
            status=HexInt("0x0"),
        )

        self.assertNotEqual(left, right)


class EvmBlockTestCase(TestCase):
    def setUp(self) -> None:
        self._transaction1 = Mock(EvmTransaction, name="tx1")
        self._transaction2 = Mock(EvmTransaction, name="tx2")

    def test_equal_attrs_is_equal(self):
        left = EvmBlock(
            difficulty=HexInt("0x1"),
            extra_data=HexBytes(b"extra"),
            gas_limit=HexInt("0x2"),
            gas_used=HexInt("0x3"),
            hash=HexBytes(b"hash"),
            logs_bloom=HexInt("0x4"),
            miner=Address("miner"),
            mix_hash=HexBytes(b"mix hash"),
            nonce=HexBytes(b"nonce"),
            number=HexInt("0x5"),
            parent_hash=HexBytes(b"parent hash"),
            receipts_root=HexBytes(b"receipts root"),
            sha3_uncles=HexBytes(b"sha3 uncles"),
            size=HexInt("0x6"),
            state_root=HexBytes(b"state root"),
            timestamp=HexInt("0x7"),
            total_difficulty=HexInt("0x8"),
            transaction_hashes=[HexBytes("0x1111"), HexBytes("0x1112")],
            transactions=[self._transaction1, self._transaction2],
            transactions_root=HexBytes(b"transactions root"),
            uncles=[HexBytes(b"uncle hash 1"), HexBytes(b"uncle hash 2")],
        )
        right = EvmBlock(
            difficulty=HexInt("0x1"),
            extra_data=HexBytes(b"extra"),
            gas_limit=HexInt("0x2"),
            gas_used=HexInt("0x3"),
            hash=HexBytes(b"hash"),
            logs_bloom=HexInt("0x4"),
            miner=Address("miner"),
            mix_hash=HexBytes(b"mix hash"),
            nonce=HexBytes(b"nonce"),
            number=HexInt("0x5"),
            parent_hash=HexBytes(b"parent hash"),
            receipts_root=HexBytes(b"receipts root"),
            sha3_uncles=HexBytes(b"sha3 uncles"),
            size=HexInt("0x6"),
            state_root=HexBytes(b"state root"),
            timestamp=HexInt("0x7"),
            total_difficulty=HexInt("0x8"),
            transaction_hashes=[HexBytes("0x1111"), HexBytes("0x1112")],
            transactions=[self._transaction1, self._transaction2],
            transactions_root=HexBytes(b"transactions root"),
            uncles=[HexBytes(b"uncle hash 1"), HexBytes(b"uncle hash 2")],
        )
        self.assertEqual(left, right)

    def test_different_value_is_not_equal(self):
        left = EvmBlock(
            difficulty=HexInt("0x1"),
            extra_data=HexBytes(b"extra"),
            gas_limit=HexInt("0x2"),
            gas_used=HexInt("0x3"),
            hash=HexBytes(b"hash"),
            logs_bloom=HexInt("0x4"),
            miner=Address("miner"),
            mix_hash=HexBytes(b"mix hash"),
            nonce=HexBytes(b"nonce"),
            number=HexInt("0x5"),
            parent_hash=HexBytes(b"parent hash"),
            receipts_root=HexBytes(b"receipts root"),
            sha3_uncles=HexBytes(b"sha3 uncles"),
            size=HexInt("0x6"),
            state_root=HexBytes(b"state root"),
            timestamp=HexInt("0x7"),
            total_difficulty=HexInt("0x8"),
            transaction_hashes=[HexBytes("0x1111"), HexBytes("0x1112")],
            transactions=[self._transaction1, self._transaction2],
            transactions_root=HexBytes(b"transactions root"),
            uncles=[HexBytes(b"uncle hash 1"), HexBytes(b"uncle hash 2")],
        )
        right = EvmBlock(
            difficulty=HexInt("0x2"),
            extra_data=HexBytes(b"extra"),
            gas_limit=HexInt("0x2"),
            gas_used=HexInt("0x3"),
            hash=HexBytes(b"hash"),
            logs_bloom=HexInt("0x4"),
            miner=Address("miner"),
            mix_hash=HexBytes(b"mix hash"),
            nonce=HexBytes(b"nonce"),
            number=HexInt("0x5"),
            parent_hash=HexBytes(b"parent hash"),
            receipts_root=HexBytes(b"receipts root"),
            sha3_uncles=HexBytes(b"sha3 uncles"),
            size=HexInt("0x6"),
            state_root=HexBytes(b"state root"),
            timestamp=HexInt("0x7"),
            total_difficulty=HexInt("0x8"),
            transaction_hashes=[HexBytes("0x1111"), HexBytes("0x1112")],
            transactions=[self._transaction1, self._transaction2],
            transactions_root=HexBytes(b"transactions root"),
            uncles=[HexBytes(b"uncle hash 1"), HexBytes(b"uncle hash 2")],
        )
        self.assertNotEqual(left, right)

    def test_equal_attrs_is_same_hash(self):
        left = EvmBlock(
            difficulty=HexInt("0x1"),
            extra_data=HexBytes(b"extra"),
            gas_limit=HexInt("0x2"),
            gas_used=HexInt("0x3"),
            hash=HexBytes(b"hash"),
            logs_bloom=HexInt("0x4"),
            miner=Address("miner"),
            mix_hash=HexBytes(b"mix hash"),
            nonce=HexBytes(b"nonce"),
            number=HexInt("0x5"),
            parent_hash=HexBytes(b"parent hash"),
            receipts_root=HexBytes(b"receipts root"),
            sha3_uncles=HexBytes(b"sha3 uncles"),
            size=HexInt("0x6"),
            state_root=HexBytes(b"state root"),
            timestamp=HexInt("0x7"),
            total_difficulty=HexInt("0x8"),
            transaction_hashes=[HexBytes("0x1111"), HexBytes("0x1112")],
            transactions=[self._transaction1, self._transaction2],
            transactions_root=HexBytes(b"transactions root"),
            uncles=[HexBytes(b"uncle hash 1"), HexBytes(b"uncle hash 2")],
        )
        right = EvmBlock(
            difficulty=HexInt("0x1"),
            extra_data=HexBytes(b"extra"),
            gas_limit=HexInt("0x2"),
            gas_used=HexInt("0x3"),
            hash=HexBytes(b"hash"),
            logs_bloom=HexInt("0x4"),
            miner=Address("miner"),
            mix_hash=HexBytes(b"mix hash"),
            nonce=HexBytes(b"nonce"),
            number=HexInt("0x5"),
            parent_hash=HexBytes(b"parent hash"),
            receipts_root=HexBytes(b"receipts root"),
            sha3_uncles=HexBytes(b"sha3 uncles"),
            size=HexInt("0x6"),
            state_root=HexBytes(b"state root"),
            timestamp=HexInt("0x7"),
            total_difficulty=HexInt("0x8"),
            transaction_hashes=[HexBytes("0x1111"), HexBytes("0x1112")],
            transactions=[self._transaction1, self._transaction2],
            transactions_root=HexBytes(b"transactions root"),
            uncles=[HexBytes(b"uncle hash 1"), HexBytes(b"uncle hash 2")],
        )
        self.assertEqual(left.__hash__(), right.__hash__())

    def test_different_block_hash_is_not_same_hash(self):
        left = EvmBlock(
            difficulty=HexInt("0x1"),
            extra_data=HexBytes(b"extra"),
            gas_limit=HexInt("0x2"),
            gas_used=HexInt("0x3"),
            hash=HexBytes(b"hash"),
            logs_bloom=HexInt("0x4"),
            miner=Address("miner"),
            mix_hash=HexBytes(b"mix hash"),
            nonce=HexBytes(b"nonce"),
            number=HexInt("0x5"),
            parent_hash=HexBytes(b"parent hash"),
            receipts_root=HexBytes(b"receipts root"),
            sha3_uncles=HexBytes(b"sha3 uncles"),
            size=HexInt("0x6"),
            state_root=HexBytes(b"state root"),
            timestamp=HexInt("0x7"),
            total_difficulty=HexInt("0x8"),
            transaction_hashes=[HexBytes("0x1111"), HexBytes("0x1112")],
            transactions=[self._transaction1, self._transaction2],
            transactions_root=HexBytes(b"transactions root"),
            uncles=[HexBytes(b"uncle hash 1"), HexBytes(b"uncle hash 2")],
        )
        right = EvmBlock(
            difficulty=HexInt("0x1"),
            extra_data=HexBytes(b"extra"),
            gas_limit=HexInt("0x2"),
            gas_used=HexInt("0x3"),
            hash=HexBytes(b"other hash"),
            logs_bloom=HexInt("0x4"),
            miner=Address("miner"),
            mix_hash=HexBytes(b"mix hash"),
            nonce=HexBytes(b"nonce"),
            number=HexInt("0x5"),
            parent_hash=HexBytes(b"parent hash"),
            receipts_root=HexBytes(b"receipts root"),
            sha3_uncles=HexBytes(b"sha3 uncles"),
            size=HexInt("0x6"),
            state_root=HexBytes(b"state root"),
            timestamp=HexInt("0x7"),
            total_difficulty=HexInt("0x8"),
            transaction_hashes=[HexBytes("0x1111"), HexBytes("0x1112")],
            transactions=[self._transaction1, self._transaction2],
            transactions_root=HexBytes(b"transactions root"),
            uncles=[HexBytes(b"uncle hash 1"), HexBytes(b"uncle hash 2")],
        )
        self.assertNotEqual(left.__hash__(), right.__hash__())


class EvmTransactionTestCase(TestCase):
    def test_equal_values_are_equal(self):
        left = EvmTransaction(
            hash=HexBytes("0x1111"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            gas=HexInt("0x03"),
            gas_price=HexInt("0x04"),
            input=HexBytes(b"input"),
            nonce=HexInt("0x4"),
            v=HexInt("0x987654"),
            r=HexBytes(b"r"),
            s=HexBytes(b"s"),
            value=HexInt("0x0"),
        )
        right = EvmTransaction(
            hash=HexBytes("0x1111"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            gas=HexInt("0x03"),
            gas_price=HexInt("0x04"),
            input=HexBytes(b"input"),
            nonce=HexInt("0x4"),
            v=HexInt("0x987654"),
            r=HexBytes(b"r"),
            s=HexBytes(b"s"),
            value=HexInt("0x0"),
        )
        self.assertEqual(left, right, "Expected objects to be equal")

    def test_different_value_is_not_equal(self):
        left = EvmTransaction(
            hash=HexBytes(b"hash"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            gas=HexInt("0x03"),
            gas_price=HexInt("0x04"),
            input=HexBytes(b"input"),
            nonce=HexInt("0x4"),
            v=HexInt("0x987654"),
            r=HexBytes(b"r"),
            s=HexBytes(b"s"),
            value=HexInt("0x0"),
        )
        right = EvmTransaction(
            hash=HexBytes(b"other"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            gas=HexInt("0x03"),
            gas_price=HexInt("0x04"),
            input=HexBytes(b"input"),
            nonce=HexInt("0x4"),
            v=HexInt("0x987654"),
            r=HexBytes(b"r"),
            s=HexBytes(b"s"),
            value=HexInt("0x0"),
        )

        self.assertNotEqual(left, right)

    def test_equal_values_hashes_are_equal(self):
        left = EvmTransaction(
            hash=HexBytes(b"hash"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            gas=HexInt("0x03"),
            gas_price=HexInt("0x04"),
            input=HexBytes(b"input"),
            nonce=HexInt("0x4"),
            v=HexInt("0x987654"),
            r=HexBytes(b"r"),
            s=HexBytes(b"s"),
            value=HexInt("0x0"),
        )
        right = EvmTransaction(
            hash=HexBytes(b"hash"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            gas=HexInt("0x03"),
            gas_price=HexInt("0x04"),
            input=HexBytes(b"input"),
            nonce=HexInt("0x4"),
            v=HexInt("0x987654"),
            r=HexBytes(b"r"),
            s=HexBytes(b"s"),
            value=HexInt("0x0"),
        )
        self.assertEqual(left, right, "Expected objects to be equal")

    def test_different_transaction_hashes_hash_is_not_equal(self):
        left = EvmTransaction(
            hash=HexBytes(b"hash"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            gas=HexInt("0x03"),
            gas_price=HexInt("0x04"),
            input=HexBytes(b"input"),
            nonce=HexInt("0x4"),
            v=HexInt("0x987654"),
            r=HexBytes(b"r"),
            s=HexBytes(b"s"),
            value=HexInt("0x0"),
        )
        right = EvmTransaction(
            hash=HexBytes(b"different hash"),
            transaction_index=HexInt("0x01"),
            block_hash=HexBytes(b"block hash"),
            block_number=HexInt("0x02"),
            from_=Address("from address"),
            to_=Address("to address"),
            gas=HexInt("0x03"),
            gas_price=HexInt("0x04"),
            input=HexBytes(b"input"),
            nonce=HexInt("0x4"),
            v=HexInt("0x987654"),
            r=HexBytes(b"r"),
            s=HexBytes(b"s"),
            value=HexInt("0x0"),
        )

        self.assertNotEqual(left, right)
