from unittest import TestCase
from unittest.mock import MagicMock

from chainconductor.blockcrawler.processors import (
    ContractTransportObject,
    Token,
)
from chainconductor.web3.types import (
    Block,
    TransactionReceipt,
    Contract,
    ERC165InterfaceID,
    HexInt,
    Log,
)


def assert_timer_run(mocked_stats_service: MagicMock, timer):
    mocked_stats_service.timer.assert_called_once_with(timer)
    # enter starts the timer
    mocked_stats_service.timer.return_value.__enter__.assert_called_once()
    # exit ends the timer and records
    mocked_stats_service.timer.return_value.__exit__.assert_called_once()


class TokenTestCase(TestCase):
    def test_equal_attributes_is_equal(self):
        left = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        right = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        self.assertEqual(left, right)

    def test_different_collection_ids_is_not_equal(self):
        left = Token(
            collection_id="c1",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        right = Token(
            collection_id="c2",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        self.assertNotEqual(left, right)

    def test_different_original_owners_is_not_equal(self):
        left = Token(
            collection_id="o1",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        right = Token(
            collection_id="str",
            original_owner="02",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        self.assertNotEqual(left, right)

    def test_different_token_ids_is_not_equal(self):
        left = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        right = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x13"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        self.assertNotEqual(left, right)

    def test_different_timestamps_is_not_equal(self):
        left = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        right = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x01"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        self.assertNotEqual(left, right)

    def test_different_metadata_uris_is_not_equal(self):
        left = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri1",
            metadata="metadata",
        )
        right = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri2",
            metadata="metadata",
        )
        self.assertNotEqual(left, right)

    def test_different_metadata_is_not_equal(self):
        left = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata1",
        )
        right = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x12"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata2",
        )
        self.assertNotEqual(left, right)


class ContractTransportObjectTestCase(TestCase):
    def test_equal_attributes_are_equal(self):
        block = Block(
            number="0x01",
            hash="block_hash",
            parent_hash="parent_hash",
            nonce="nonce",
            sha3_uncles="sha3_uncles",
            logs_bloom="logs_bloom",
            transactions_root="transactions_root",
            state_root="state_root",
            receipts_root="receipts_root",
            miner="miner",
            mix_hash="",
            difficulty="difficulty",
            total_difficulty="total_difficulty",
            extra_data="extra_data",
            size="size",
            gas_limit="gas_limit",
            gas_used="gas_used",
            timestamp="timestamp",
            transactions=["transaction"],
            uncles=["uncle1"],
        )
        transaction_receipt = TransactionReceipt(
            transaction_hash="hash",
            transaction_index="0x00",
            block_hash="block_hash",
            block_number="0x01",
            from_="from_",
            to_="to_",
            cumulative_gas_used="cumulative_gas_used",
            gas_used="gas_used",
            contract_address="contract_address",
            logs=[
                Log(
                    removed=False,
                    log_index="0x00",
                    transaction_index="transaction_index",
                    transaction_hash="transaction_hash",
                    block_hash="block_hash",
                    block_number="block_number",
                    address="address",
                    data="data",
                    topics=["topic"],
                )
            ],
            logs_bloom="logs_bloom",
            root="root",
            status="0x01",
        )
        contract = Contract(
            address="0x00",
            name="name",
            symbol="symbol",
            creator="creator",
            total_supply=1,
            interfaces=[ERC165InterfaceID.ERC721],
        )
        left = ContractTransportObject(
            block=block,
            transaction_receipt=transaction_receipt,
            contract=contract,
        )
        right = ContractTransportObject(
            block=block,
            transaction_receipt=transaction_receipt,
            contract=contract,
        )
        self.assertEqual(left, right)

    def test_different_blocks_are_not_equal(self):
        transaction_receipt = TransactionReceipt(
            transaction_hash="hash",
            transaction_index="0x00",
            block_hash="block_hash",
            block_number="0x01",
            from_="from_",
            to_="to_",
            cumulative_gas_used="cumulative_gas_used",
            gas_used="gas_used",
            contract_address="contract_address",
            logs=[
                Log(
                    removed=False,
                    log_index="0x00",
                    transaction_index="transaction_index",
                    transaction_hash="transaction_hash",
                    block_hash="block_hash",
                    block_number="block_number",
                    address="address",
                    data="data",
                    topics=["topic"],
                )
            ],
            logs_bloom="logs_bloom",
            root="root",
            status="0x01",
        )
        contract = Contract(
            address="0x00",
            name="name",
            symbol="symbol",
            creator="creator",
            total_supply=1,
            interfaces=[ERC165InterfaceID.ERC721],
        )
        left = ContractTransportObject(
            block=Block(
                number="0x01",
                hash="block_hash",
                parent_hash="parent_hash",
                nonce="nonce",
                sha3_uncles="sha3_uncles",
                logs_bloom="logs_bloom",
                transactions_root="transactions_root",
                state_root="state_root",
                receipts_root="receipts_root",
                miner="miner",
                mix_hash="",
                difficulty="difficulty",
                total_difficulty="total_difficulty",
                extra_data="extra_data",
                size="size",
                gas_limit="gas_limit",
                gas_used="gas_used",
                timestamp="timestamp",
                transactions=["transaction"],
                uncles=["uncle1"],
            ),
            transaction_receipt=transaction_receipt,
            contract=contract,
        )
        right = ContractTransportObject(
            block=Block(
                number="0x02",
                hash="block_hash",
                parent_hash="parent_hash",
                nonce="nonce",
                sha3_uncles="sha3_uncles",
                logs_bloom="logs_bloom",
                transactions_root="transactions_root",
                state_root="state_root",
                receipts_root="receipts_root",
                miner="miner",
                mix_hash="",
                difficulty="difficulty",
                total_difficulty="total_difficulty",
                extra_data="extra_data",
                size="size",
                gas_limit="gas_limit",
                gas_used="gas_used",
                timestamp="timestamp",
                transactions=["transaction"],
                uncles=["uncle1"],
            ),
            transaction_receipt=transaction_receipt,
            contract=contract,
        )
        self.assertNotEqual(left, right)

    def test_different_transaction_receipts_are_not_equal(self):
        block = Block(
            number="0x01",
            hash="block_hash",
            parent_hash="parent_hash",
            nonce="nonce",
            sha3_uncles="sha3_uncles",
            logs_bloom="logs_bloom",
            transactions_root="transactions_root",
            state_root="state_root",
            receipts_root="receipts_root",
            miner="miner",
            mix_hash="",
            difficulty="difficulty",
            total_difficulty="total_difficulty",
            extra_data="extra_data",
            size="size",
            gas_limit="gas_limit",
            gas_used="gas_used",
            timestamp="timestamp",
            transactions=["transaction"],
            uncles=["uncle1"],
        )
        contract = Contract(
            address="0x00",
            name="name",
            symbol="symbol",
            creator="creator",
            total_supply=1,
            interfaces=[ERC165InterfaceID.ERC721],
        )
        left = ContractTransportObject(
            block=block,
            transaction_receipt=TransactionReceipt(
                transaction_hash="hash",
                transaction_index="0x00",
                block_hash="block_hash",
                block_number="0x01",
                from_="from_",
                to_="to_",
                cumulative_gas_used="cumulative_gas_used",
                gas_used="gas_used",
                contract_address="contract_address",
                logs=[
                    Log(
                        removed=False,
                        log_index="0x00",
                        transaction_index="transaction_index",
                        transaction_hash="transaction_hash",
                        block_hash="block_hash",
                        block_number="block_number",
                        address="address",
                        data="data",
                        topics=["topic"],
                    )
                ],
                logs_bloom="logs_bloom",
                root="root",
                status="0x01",
            ),
            contract=contract,
        )
        right = ContractTransportObject(
            block=block,
            transaction_receipt=TransactionReceipt(
                transaction_hash="hash",
                transaction_index="0x01",
                block_hash="block_hash",
                block_number="0x01",
                from_="from_",
                to_="to_",
                cumulative_gas_used="cumulative_gas_used",
                gas_used="gas_used",
                contract_address="contract_address",
                logs=[
                    Log(
                        removed=False,
                        log_index="0x00",
                        transaction_index="transaction_index",
                        transaction_hash="transaction_hash",
                        block_hash="block_hash",
                        block_number="block_number",
                        address="address",
                        data="data",
                        topics=["topic"],
                    )
                ],
                logs_bloom="logs_bloom",
                root="root",
                status="0x01",
            ),
            contract=contract,
        )
        self.assertNotEqual(left, right)

    def test_different_contracts_are_not_equal(self):
        block = Block(
            number="0x01",
            hash="block_hash",
            parent_hash="parent_hash",
            nonce="nonce",
            sha3_uncles="sha3_uncles",
            logs_bloom="logs_bloom",
            transactions_root="transactions_root",
            state_root="state_root",
            receipts_root="receipts_root",
            miner="miner",
            mix_hash="",
            difficulty="difficulty",
            total_difficulty="total_difficulty",
            extra_data="extra_data",
            size="size",
            gas_limit="gas_limit",
            gas_used="gas_used",
            timestamp="timestamp",
            transactions=["transaction"],
            uncles=["uncle1"],
        )
        transaction_receipt = TransactionReceipt(
            transaction_hash="hash",
            transaction_index="0x00",
            block_hash="block_hash",
            block_number="0x01",
            from_="from_",
            to_="to_",
            cumulative_gas_used="cumulative_gas_used",
            gas_used="gas_used",
            contract_address="contract_address",
            logs=[
                Log(
                    removed=False,
                    log_index="0x00",
                    transaction_index="transaction_index",
                    transaction_hash="transaction_hash",
                    block_hash="block_hash",
                    block_number="block_number",
                    address="address",
                    data="data",
                    topics=["topic"],
                )
            ],
            logs_bloom="logs_bloom",
            root="root",
            status="0x01",
        )
        left = ContractTransportObject(
            block=block,
            transaction_receipt=transaction_receipt,
            contract=Contract(
                address="0x00",
                name="name",
                symbol="symbol",
                creator="creator",
                total_supply=1,
                interfaces=[ERC165InterfaceID.ERC721],
            ),
        )
        right = ContractTransportObject(
            block=block,
            transaction_receipt=transaction_receipt,
            contract=Contract(
                address="0x01",
                name="name",
                symbol="symbol",
                creator="creator",
                total_supply=1,
                interfaces=[ERC165InterfaceID.ERC721],
            ),
        )
        self.assertNotEqual(left, right)
