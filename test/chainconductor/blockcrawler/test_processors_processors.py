import random
import unittest
from logging import Logger
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch, call, ANY

import ddt
from botocore.exceptions import ClientError
from eth_abi import encode
from eth_hash.auto import keccak
from eth_utils import decode_hex

from chainconductor.blockcrawler.data_clients import ProtocolError
from chainconductor.blockcrawler.processors import (
    ContractTransportObject,
    TokenTransportObject,
    TokenTransferPersistenceBatchProcessor,
    CollectionPersistenceBatchProcessor,
    TokenMetadataUriBatchProcessor,
    Token,
    TokenPersistenceBatchProcessor,
    TokenMetadataRetrievalBatchProcessor,
    BlockBatchProcessor,
    TransactionBatchProcessor,
    ContractBatchProcessor,
    RPCErrorRetryDecoratingBatchProcessor,
)
from chainconductor.blockcrawler.stats import StatsService
from chainconductor.web3.rpc import EthCall, RPCServerError, RPCClient
from chainconductor.web3.types import (
    Block,
    TransactionReceipt,
    Contract,
    ERC165InterfaceID,
    HexInt,
    Log,
)
from chainconductor.web3.util import (
    ERC721Events,
    ERC721MetadataFunctions,
    ERC1155MetadataURIFunctions,
    ERC165Functions,
    ERC721EnumerableFunctions,
)
from chainconductor.blockcrawler.processors.queued_batch import QueuedBatchProcessor
from .. import async_context_manager_mock


def assert_timer_run(mocked_stats_service: MagicMock, timer):
    mocked_stats_service.timer.assert_called_once_with(timer)
    # enter starts the timer
    mocked_stats_service.timer.return_value.__enter__.assert_called_once()
    # exit ends the timer and records
    mocked_stats_service.timer.return_value.__exit__.assert_called_once()


class BatchedQueueProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__acquisition_strategy = AsyncMock()
        self.__disposition_strategy = AsyncMock()
        self.__batch_processor = AsyncMock()
        self.__batch_processor.side_effect = lambda batch: batch
        self.__logger = AsyncMock(Logger)
        self.__processor = QueuedBatchProcessor(
            acquisition_strategy=self.__acquisition_strategy,
            disposition_strategy=self.__disposition_strategy,
            batch_processor=self.__batch_processor,
            logger=self.__logger,
            max_processors=1,
        )

    async def test_passes_acquired_items_to_batch_processor(self):
        self.__acquisition_strategy.return_value = [1, "two", 3.0, None]
        await self.__processor.run()
        self.__batch_processor.assert_awaited_with([1, "two", 3.0])

    async def test_passes_none_marker_to_disposition_strategy_as_separate_final_call(self):
        self.__acquisition_strategy.return_value = [1, 2, 3, None]
        await self.__processor.run()
        self.__disposition_strategy.assert_awaited_with(None)

    async def test_passes_batch_results_to_disposition_strategy(self):
        self.__acquisition_strategy.return_value = ["str", 2.3, 3, {}, [], None]
        await self.__processor.run()
        self.__disposition_strategy.assert_has_awaits([call(["str", 2.3, 3, {}, []])])

    async def test_block_processor_exceptions_are_logged_and_disposition_strategy_not_called(self):
        self.__acquisition_strategy.return_value = ["value", None]
        expected = Exception()
        self.__batch_processor.side_effect = expected
        await self.__processor.run()
        self.__logger.exception.assert_called_once()

    async def test_does_not_send_empty_batch_to_processor(self):
        self.__acquisition_strategy.return_value = [None]
        await self.__processor.run()
        self.__batch_processor.assert_not_called()

    async def test_batch_error_drains_inbound_and_alerts_outbound_without_processing(self):
        self.__batch_processor.side_effect = Exception
        self.__acquisition_strategy.side_effect = [["a"], ["b", None]]
        await self.__processor.run()
        self.__acquisition_strategy.assert_has_awaits([call(), call()])
        self.__disposition_strategy.assert_awaited_once_with(None)
        self.__batch_processor.assert_awaited_once_with(["a"])


class BlockBatchProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock()
        self.__rpc_client.get_blocks.return_value = []
        self.__stats_service = MagicMock()
        self.__processor = BlockBatchProcessor(
            rpc_client=self.__rpc_client,
            stats_service=self.__stats_service,
        )

    async def test_records_rpc_call_timer(self):
        await self.__processor([0])
        assert_timer_run(self.__stats_service, "rpc_get_blocks")

    async def test_increments_stats_for_each_block(self):
        block_id_count = random.randint(1, 99)
        block_ids = list()
        get_blocks_return_value = list()
        for block_id in range(block_id_count):
            block_ids.append(block_id)
            get_blocks_return_value.append(
                Block(
                    number=hex(block_id),
                    hash="",
                    parent_hash="",
                    nonce="",
                    sha3_uncles="",
                    logs_bloom="",
                    transactions_root="",
                    state_root="",
                    receipts_root="",
                    miner="",
                    mix_hash="",
                    difficulty="",
                    total_difficulty="",
                    extra_data="",
                    size="",
                    gas_limit="",
                    gas_used="",
                    timestamp="",
                    transactions=list(),
                    uncles=list(),
                )
            )
        self.__rpc_client.get_blocks.return_value = get_blocks_return_value
        await self.__processor(block_ids)
        self.assertEqual(
            block_id_count,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were blocks",
        )
        self.__stats_service.increment.assert_called_with("blocks_processed")

    async def test_sends_block_ids_to_rpc_client_get_blocks(self):
        await self.__processor([0, 1])
        self.__rpc_client.get_blocks.assert_awaited_with({0, 1}, full_transactions=ANY)

    async def test_sends_full_transactions_false_to_rpc_client_get_blocks(self):
        await self.__processor([0, 1])
        self.__rpc_client.get_blocks.assert_awaited_with(ANY, full_transactions=False)

    async def test_returns_items_if_there_are_transactions(
        self,
    ):
        block = Block(
            number="0x00",
            hash="",
            parent_hash="",
            nonce="",
            sha3_uncles="",
            logs_bloom="",
            transactions_root="",
            state_root="",
            receipts_root="",
            miner="",
            mix_hash="",
            difficulty="",
            total_difficulty="",
            extra_data="",
            size="",
            gas_limit="",
            gas_used="",
            timestamp="",
            transactions=["1", "2"],
            uncles=list(),
        )
        self.__rpc_client.get_blocks.return_value = [block]
        expected = [("1", block), ("2", block)]
        actual = await self.__processor([0])
        self.assertEqual(expected, actual)

    async def test_does_not_return_items_if_there_are_no_transactions(
        self,
    ):
        self.__rpc_client.get_blocks.return_value = [
            Block(
                number="0x00",
                hash="",
                parent_hash="",
                nonce="",
                sha3_uncles="",
                logs_bloom="",
                transactions_root="",
                state_root="",
                receipts_root="",
                miner="",
                mix_hash="",
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="",
                transactions=list(),
                uncles=list(),
            )
        ]
        actual = await self.__processor([0])
        self.assertEqual([], actual)


@ddt.ddt
class TransactionBatchProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock()
        self.__rpc_client.get_transaction_receipts.return_value = [
            TransactionReceipt(
                transaction_hash="tx hash",
                transaction_index="",
                block_hash="",
                block_number="",
                from_="",
                to_="",
                cumulative_gas_used="",
                gas_used="",
                contract_address="",
                logs=list(),
                logs_bloom="0x00",
                root="",
                status="",
            )
        ]
        self.__default_batch = [
            (
                "tx hash",
                Block(
                    number="0x00",
                    hash="",
                    parent_hash="",
                    nonce="",
                    sha3_uncles="",
                    logs_bloom="0x00",
                    transactions_root="",
                    state_root="",
                    receipts_root="",
                    miner="",
                    mix_hash="",
                    difficulty="",
                    total_difficulty="",
                    extra_data="",
                    size="",
                    gas_limit="",
                    gas_used="",
                    timestamp="",
                    transactions=list("tx hash"),
                    uncles=list(),
                ),
            ),
        ]
        self.__stats_service = MagicMock()
        self.__processor = TransactionBatchProcessor(
            rpc_client=self.__rpc_client,
            stats_service=self.__stats_service,
        )

    async def test_records_rpc_call_timer(self):
        await self.__processor(self.__default_batch)
        assert_timer_run(self.__stats_service, "rpc_get_transaction_receipts")

    async def test_increments_stats_for_each_block(self):
        transactions = random.randint(1, 99)
        get_transaction_receipts_return_value = list()
        transaction_hashes = list()
        for i in range(transactions):
            transaction_hash = f"tx {i}"
            transaction_hashes.append(transaction_hash)
            get_transaction_receipts_return_value.append(
                TransactionReceipt(
                    transaction_hash=transaction_hash,
                    transaction_index="",
                    block_hash="",
                    block_number="",
                    from_="",
                    to_="",
                    cumulative_gas_used="",
                    gas_used="",
                    contract_address="",
                    logs=list(),
                    logs_bloom="0x2",
                    root="",
                    status="",
                )
            )
        self.__rpc_client.get_transaction_receipts.return_value = (
            get_transaction_receipts_return_value
        )
        batch = [
            (
                tx_hash,
                Block(
                    number="0x00",
                    hash="",
                    parent_hash="",
                    nonce="",
                    sha3_uncles="",
                    logs_bloom="0x00",
                    transactions_root="",
                    state_root="",
                    receipts_root="",
                    miner="",
                    mix_hash="",
                    difficulty="",
                    total_difficulty="",
                    extra_data="",
                    size="",
                    gas_limit="",
                    gas_used="",
                    timestamp="",
                    transactions=transaction_hashes,
                    uncles=list(),
                ),
            )
            for tx_hash in transaction_hashes
        ]
        await self.__processor(batch)
        self.assertEqual(
            transactions,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were transactions",
        )
        self.__stats_service.increment.assert_called_with("transactions_processed")

    async def test_returns_contracts_as_contract_with_transaction_receipt_attached(self):
        block = Block(
            number="0x00",
            hash="",
            parent_hash="",
            nonce="",
            sha3_uncles="",
            logs_bloom="",
            transactions_root="",
            state_root="",
            receipts_root="",
            miner="",
            mix_hash="",
            difficulty="",
            total_difficulty="",
            extra_data="",
            size="",
            gas_limit="",
            gas_used="",
            timestamp="",
            transactions=list(),
            uncles=list(),
        )
        cto = ("tx hash", block)
        transaction_receipt = TransactionReceipt(
            transaction_hash="tx hash",
            transaction_index="",
            block_hash="",
            block_number="",
            from_="",
            to_=None,
            cumulative_gas_used="",
            gas_used="",
            contract_address="",
            logs=list(),
            logs_bloom="0x00",
            root="",
            status="",
        )
        self.__rpc_client.get_transaction_receipts.return_value = [transaction_receipt]
        actual = await self.__processor([cto])
        expected = [
            ContractTransportObject(
                block=block,
                transaction_receipt=transaction_receipt,
            )
        ]
        self.assertEqual(expected, actual)

    @patch("chainconductor.blockcrawler.processors.BloomFilter")
    async def test_returns_tokens_with_receipt_if_transfer_event_in_logs_bloom(self, bloom_patch):
        block = Block(
            number="0x00",
            hash="",
            parent_hash="",
            nonce="",
            sha3_uncles="",
            logs_bloom="0x00",
            transactions_root="",
            state_root="",
            receipts_root="",
            miner="",
            mix_hash="",
            difficulty="",
            total_difficulty="",
            extra_data="",
            size="",
            gas_limit="",
            gas_used="",
            timestamp="",
            transactions=list("tx hash"),
            uncles=list(),
        )
        tto = ("tx hash", block)
        transaction_receipt = TransactionReceipt(
            transaction_hash="tx hash",
            transaction_index="",
            block_hash="",
            block_number="",
            from_="",
            to_="",
            cumulative_gas_used="",
            gas_used="",
            contract_address="",
            logs=list(),
            logs_bloom="0x00",
            root="",
            status="",
        )
        self.__rpc_client.get_transaction_receipts.return_value = [transaction_receipt]
        bloom_patch.return_value.__contains__.return_value = True
        actual = await self.__processor([tto])
        expected = [
            TokenTransportObject(
                block=block,
                transaction_receipt=transaction_receipt,
            )
        ]
        self.assertEqual(expected, actual)

    @patch("chainconductor.blockcrawler.processors.BloomFilter")
    async def test_constructs_bloom_filter_with_transaction_logs_bloom(self, bloom_patch):
        self.__rpc_client.get_transaction_receipts.return_value = [
            TransactionReceipt(
                transaction_hash="tx",
                transaction_index="",
                block_hash="",
                block_number="",
                from_="",
                to_="",
                cumulative_gas_used="",
                gas_used="",
                contract_address="",
                logs=list(),
                logs_bloom="0x998877665544332211",
                root="",
                status="",
            )
        ]
        tto = (
            "tx",
            Block(
                number="0x00",
                hash="",
                parent_hash="",
                nonce="",
                sha3_uncles="",
                logs_bloom="0x998877665544332211",
                transactions_root="",
                state_root="",
                receipts_root="",
                miner="",
                mix_hash="",
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="",
                transactions=list("tx"),
                uncles=list(),
            ),
        )
        await self.__processor([tto])
        bloom_patch.assert_called_once_with(int("0x998877665544332211", 16))

    @patch("chainconductor.blockcrawler.processors.BloomFilter")
    async def test_checks_bloom_filter_against_transfer_event_hash(self, bloom_patch):
        block = Block(
            number="0x00",
            hash="",
            parent_hash="",
            nonce="",
            sha3_uncles="",
            logs_bloom="",
            transactions_root="",
            state_root="",
            receipts_root="",
            miner="",
            mix_hash="",
            difficulty="",
            total_difficulty="",
            extra_data="",
            size="",
            gas_limit="",
            gas_used="",
            timestamp="",
            transactions=list("tx hash"),
            uncles=list(),
        )
        await self.__processor([("tx hash", block)])
        expected = decode_hex("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
        bloom_patch.return_value.__contains__.assert_called_once_with(expected)


@ddt.ddt
class TokenTransferPersistenceBatchProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__default_batch = [
            TokenTransportObject(
                transaction_receipt=TransactionReceipt(
                    transaction_hash="",
                    transaction_index="",
                    block_hash="",
                    block_number="",
                    from_="0x50",
                    to_="",
                    cumulative_gas_used="",
                    gas_used="",
                    contract_address="0x99",
                    logs=list(),
                    logs_bloom="",
                    root="",
                    status="",
                ),
                block=Block(
                    number="0x00",
                    hash="",
                    parent_hash="",
                    nonce="",
                    sha3_uncles="",
                    logs_bloom="",
                    transactions_root="",
                    state_root="",
                    receipts_root="",
                    miner="",
                    mix_hash="",
                    difficulty="",
                    total_difficulty="",
                    extra_data="",
                    size="",
                    gas_limit="",
                    gas_used="",
                    timestamp="0x00",
                    transactions=list(),
                    uncles=list(),
                ),
            ),
        ]

        self.__dynamodb = AsyncMock()
        self.__dynamodb.Table.return_value.batch_writer = async_context_manager_mock()
        self.__stats_service = MagicMock()
        self.__logger = MagicMock()
        self.__blockchain = "expected blockchain"
        self.__processor = TokenTransferPersistenceBatchProcessor(
            dynamodb=self.__dynamodb,
            stats_service=self.__stats_service,
            logger=self.__logger,
            blockchain=self.__blockchain[:],
        )

    async def test_increments_stats_for_each_token(self):
        tokens = random.randint(1, 99)
        batch_items = list()

        log_topics = [
            ERC721Events.TRANSFER.event_hash,
            encode(["address"], ["0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"]).hex(),
            encode(["address"], ["0xcd5db1feb8758b9bc8d3172d3f229b29ba47024f"]).hex(),
            encode(["uint256"], [1]).hex(),
        ]

        for _ in range(tokens):
            batch_items.append(
                TokenTransportObject(
                    transaction_receipt=TransactionReceipt(
                        transaction_hash="",
                        transaction_index="",
                        block_hash="",
                        block_number="",
                        from_="",
                        to_="",
                        cumulative_gas_used="",
                        gas_used="",
                        contract_address="",
                        logs=[
                            Log(
                                removed=False,
                                log_index="0x00",
                                transaction_index="0x10",
                                transaction_hash="",
                                block_number="0x20",
                                block_hash="",
                                address="0x30",
                                data="0x00",
                                topics=log_topics,
                            )
                        ],
                        logs_bloom="",
                        root="",
                        status="",
                    ),
                    block=Block(
                        number="0x00",
                        hash="",
                        parent_hash="",
                        nonce="",
                        sha3_uncles="",
                        logs_bloom="",
                        transactions_root="",
                        state_root="",
                        receipts_root="",
                        miner="",
                        mix_hash="",
                        difficulty="",
                        total_difficulty="",
                        extra_data="",
                        size="",
                        gas_limit="",
                        gas_used="",
                        timestamp="0x00",
                        transactions=list(),
                        uncles=list(),
                    ),
                )
            )
        await self.__processor(batch_items)
        self.assertEqual(
            tokens,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were transactions",
        )
        self.__stats_service.increment.assert_called_with("token_transfers_persisted")

    async def test_records_dynamodb_timer_in_stats(self):
        # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor(self.__default_batch)
        assert_timer_run(self.__stats_service, "dynamodb_write_token_transfers")

    async def test_stores_correct_data(self):
        log_topics = [
            ERC721Events.TRANSFER.event_hash,
            encode(["address"], ["0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"]).hex(),
            encode(["address"], ["0x759a401a287ffad0fa2deae15fe3b6169506d657"]).hex(),
            encode(["uint256"], [1]).hex(),
        ]
        tto = TokenTransportObject(
            transaction_receipt=TransactionReceipt(
                transaction_hash="transaction hash",
                transaction_index="",
                block_hash="",
                block_number="",
                from_="0x50",
                to_="to address",
                cumulative_gas_used="",
                gas_used="",
                contract_address="expected contract",
                logs=[
                    Log(
                        removed=False,
                        log_index="0x30",
                        transaction_index="0x10",
                        transaction_hash="transaction hash",
                        block_number="0x20",
                        block_hash="",
                        address="to address",
                        data="0x00",
                        topics=log_topics,
                    )
                ],
                logs_bloom="",
                root="",
                status="",
            ),
            block=Block(
                number="0x20",
                hash="",
                parent_hash="",
                nonce="",
                sha3_uncles="",
                logs_bloom="",
                transactions_root="",
                state_root="",
                receipts_root="",
                miner="",
                mix_hash="",
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="0x0f",
                transactions=list(),
                uncles=list(),
            ),
        )

        await self.__processor([tto])
        context_manager = self.__dynamodb.Table.return_value.batch_writer.return_value.__aenter__
        context_manager.return_value.put_item.assert_awaited_once_with(
            Item={
                "blockchain": self.__blockchain,
                "transaction_log_index_hash": keccak("0x200x100x30".encode("utf8")).hex(),
                "collection_id": "to address",
                "token_id": "1",
                "from": "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d",
                "to": "0x759a401a287ffad0fa2deae15fe3b6169506d657",
                "block": 32,
                "transaction_index": 16,
                "log_index": 48,
                "timestamp": 15,
            }
        )

    async def test_does_not_process_erc20_transfer_events(self):
        log_topics = [
            ERC721Events.TRANSFER.event_hash,
            encode(["address"], ["0x0000000000000000000000000000000000000000"]).hex(),
            encode(["address"], ["0x759a401a287ffad0fa2deae15fe3b6169506d657"]).hex(),
        ]
        log_data = encode(["uint256"], [1]).hex()

        tto = TokenTransportObject(
            transaction_receipt=TransactionReceipt(
                transaction_hash="transaction hash",
                transaction_index="",
                block_hash="",
                block_number="",
                from_="0x50",
                to_="to address",
                cumulative_gas_used="",
                gas_used="",
                contract_address="expected contract",
                logs=[
                    Log(
                        removed=False,
                        log_index="0x00",
                        transaction_index="0x10",
                        transaction_hash="",
                        block_number="0x20",
                        block_hash="",
                        address="to address",
                        data=log_data,
                        topics=log_topics,
                    )
                ],
                logs_bloom="",
                root="",
                status="",
            ),
            block=Block(
                number="0x20",
                hash="",
                parent_hash="",
                nonce="",
                sha3_uncles="",
                logs_bloom="",
                transactions_root="",
                state_root="",
                receipts_root="",
                miner="",
                mix_hash="",
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="0x0f",
                transactions=list(),
                uncles=list(),
            ),
        )

        actual = await self.__processor([tto])
        context_manager = self.__dynamodb.Table.return_value.batch_writer.return_value.__aenter__
        context_manager.return_value.put_item.assert_not_called()
        self.assertEqual([], actual)

    @ddt.data(
        # Contract to 0
        (
            "0x759a401a287ffad0fa2deae15fe3b6169506d657",
            "0x0000000000000000000000000000000000000000",
        ),
        # 0 to Contract
        (
            "0x0000000000000000000000000000000000000000",
            "0x759a401a287ffad0fa2deae15fe3b6169506d657",
        ),
    )
    @ddt.unpack
    async def test_does_not_add_internal_transfer_events_to_result(self, from_address, to_address):
        log_topics = [
            ERC721Events.TRANSFER.event_hash,
            encode(["address"], [from_address]).hex(),
            encode(["address"], [to_address]).hex(),
            encode(["uint256"], [1]).hex(),
        ]

        tto = TokenTransportObject(
            transaction_receipt=TransactionReceipt(
                transaction_hash="transaction hash",
                transaction_index="",
                block_hash="",
                block_number="",
                from_="wrong from",
                to_="wrong to",
                cumulative_gas_used="",
                gas_used="",
                contract_address="expected contract",
                logs=[
                    Log(
                        removed=False,
                        log_index="0x00",
                        transaction_index="0x10",
                        transaction_hash="",
                        block_number="0x20",
                        block_hash="",
                        address="0x759a401a287ffad0fa2deae15fe3b6169506d657",
                        data="0x00",
                        topics=log_topics,
                    ),
                ],
                logs_bloom="",
                root="",
                status="",
            ),
            block=Block(
                number="0x20",
                hash="",
                parent_hash="",
                nonce="",
                sha3_uncles="",
                logs_bloom="",
                transactions_root="",
                state_root="",
                receipts_root="",
                miner="",
                mix_hash="",
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="0x0f",
                transactions=list(),
                uncles=list(),
            ),
        )

        actual = await self.__processor([tto])
        self.assertEqual([], actual)

    async def test_does_not_process_non_transfer_events(self):
        log_topics = [
            "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b",
            encode(["address"], ["0x0000000000000000000000000000000000000000"]).hex(),
            encode(["address"], ["0x759a401a287ffad0fa2deae15fe3b6169506d657"]).hex(),
            encode(["uint256"], [1]).hex(),
        ]

        tto = TokenTransportObject(
            transaction_receipt=TransactionReceipt(
                transaction_hash="transaction hash",
                transaction_index="",
                block_hash="",
                block_number="",
                from_="0x50",
                to_="to address",
                cumulative_gas_used="",
                gas_used="",
                contract_address="expected contract",
                logs=[
                    Log(
                        removed=False,
                        log_index="0x00",
                        transaction_index="0x10",
                        transaction_hash="",
                        block_number="0x20",
                        block_hash="",
                        address="to address",
                        data="0x00",
                        topics=log_topics,
                    )
                ],
                logs_bloom="",
                root="",
                status="",
            ),
            block=Block(
                number="0x20",
                hash="",
                parent_hash="",
                nonce="",
                sha3_uncles="",
                logs_bloom="",
                transactions_root="",
                state_root="",
                receipts_root="",
                miner="",
                mix_hash="",
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="0x0f",
                transactions=list(),
                uncles=list(),
            ),
        )

        await self.__processor([tto])
        context_manager = self.__dynamodb.Table.return_value.batch_writer.return_value.__aenter__
        context_manager.return_value.put_item.assert_not_called()
        actual = await self.__processor([tto])
        self.assertEqual([], actual)

    @ddt.data(
        "0x759a401a287ffad0fa2deae15fe3b6169506d657", "0x0000000000000000000000000000000000000000"
    )
    async def test_token_transaction_from_minting_address_added_to_result(self, from_address):
        log_topics = [
            ERC721Events.TRANSFER.event_hash,
            encode(["address"], [from_address]).hex(),
            encode(["address"], ["0xcd5db1feb8758b9bc8d3172d3f229b29ba47024f"]).hex(),
            encode(["uint256"], [11247]).hex(),
        ]

        tto = TokenTransportObject(
            transaction_receipt=TransactionReceipt(
                transaction_hash="transaction hash",
                transaction_index="",
                block_hash="",
                block_number="",
                from_=from_address,
                to_="to address",
                cumulative_gas_used="",
                gas_used="",
                contract_address="0x90",
                logs=[
                    Log(
                        removed=False,
                        log_index="0x00",
                        transaction_index="0x10",
                        transaction_hash="",
                        block_number="0x20",
                        block_hash="",
                        address="0x759a401a287ffad0fa2deae15fe3b6169506d657",
                        data="0x00",
                        topics=log_topics,
                    )
                ],
                logs_bloom="",
                root="",
                status="",
            ),
            block=Block(
                number="0x00",
                hash="",
                parent_hash="",
                nonce="",
                sha3_uncles="",
                logs_bloom="",
                transactions_root="",
                state_root="",
                receipts_root="",
                miner="",
                mix_hash="",
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="0x0",
                transactions=list(),
                uncles=list(),
            ),
        )
        actual = await self.__processor([tto])
        expected = [
            TokenTransportObject(
                block=tto.block,
                transaction_receipt=tto.transaction_receipt,
                token=Token(
                    collection_id="0x759a401a287ffad0fa2deae15fe3b6169506d657",
                    original_owner="0xcd5db1feb8758b9bc8d3172d3f229b29ba47024f",
                    token_id=HexInt("0x2bef"),
                    timestamp=HexInt("0x0"),
                ),
            )
        ]
        self.assertEqual(expected, actual)


class ContractBatchProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock(RPCClient)
        self.__rpc_client.calls.return_value = dict()
        self.__stats_service = AsyncMock(StatsService)
        self.__processor = ContractBatchProcessor(self.__rpc_client, self.__stats_service)
        transaction_receipt = TransactionReceipt(
            transaction_hash="",
            transaction_index="",
            block_number="",
            block_hash="",
            from_="0x99",
            to_="",
            cumulative_gas_used="",
            gas_used="",
            contract_address="contract address",
            logs=list(),
            logs_bloom="",
            root="",
            status="",
        )
        self.__default_batch = [ContractTransportObject(transaction_receipt=transaction_receipt)]

    async def test_records_time_for_rpc_call(self):
        await self.__processor(self.__default_batch)
        assert_timer_run(self.__stats_service, ContractBatchProcessor.RPC_TIMER_CALL_CONTRACT_INFO)

    async def test_gets_contract_data(self):
        await self.__processor(self.__default_batch)
        contract_address = self.__default_batch[0].transaction_receipt.contract_address
        self.__rpc_client.calls.assert_any_await(
            [
                EthCall(
                    ERC165InterfaceID.ERC721.value,
                    None,
                    contract_address,
                    ERC165Functions.SUPPORTS_INTERFACE,
                    [ERC165InterfaceID.ERC721.bytes],
                ),
                EthCall(
                    ERC165InterfaceID.ERC721_METADATA.value,
                    None,
                    contract_address,
                    ERC165Functions.SUPPORTS_INTERFACE,
                    [ERC165InterfaceID.ERC721_METADATA.bytes],
                ),
                EthCall(
                    ERC165InterfaceID.ERC721_ENUMERABLE.value,
                    None,
                    contract_address,
                    ERC165Functions.SUPPORTS_INTERFACE,
                    [ERC165InterfaceID.ERC721_ENUMERABLE.bytes],
                ),
                EthCall(
                    ERC165InterfaceID.ERC1155.value,
                    None,
                    contract_address,
                    ERC165Functions.SUPPORTS_INTERFACE,
                    [ERC165InterfaceID.ERC1155.bytes],
                ),
                EthCall(
                    ERC165InterfaceID.ERC1155_METADATA_URI.value,
                    None,
                    contract_address,
                    ERC165Functions.SUPPORTS_INTERFACE,
                    [ERC165InterfaceID.ERC1155_METADATA_URI.bytes],
                ),
                EthCall(
                    "symbol",
                    None,
                    self.__default_batch[0].transaction_receipt.contract_address,
                    ERC721MetadataFunctions.SYMBOL,
                ),
                EthCall(
                    "name",
                    None,
                    self.__default_batch[0].transaction_receipt.contract_address,
                    ERC721MetadataFunctions.NAME,
                ),
                EthCall(
                    "total_supply",
                    None,
                    self.__default_batch[0].transaction_receipt.contract_address,
                    ERC721EnumerableFunctions.TOTAL_SUPPLY,
                ),
            ]
        )

    async def test_adds_all_interfaces_returning_true_to_contract(self):
        self.__rpc_client.calls.return_value = {
            ERC165InterfaceID.ERC721.value: (True,),
            ERC165InterfaceID.ERC721_METADATA.value: (False,),
            ERC165InterfaceID.ERC721_ENUMERABLE.value: (True,),
            ERC165InterfaceID.ERC1155.value: (False,),
            ERC165InterfaceID.ERC1155_METADATA_URI.value: (False,),
        }
        actual = await self.__processor(self.__default_batch)
        expected = [ERC165InterfaceID.ERC721, ERC165InterfaceID.ERC721_ENUMERABLE]
        self.assertEqual(expected, actual[0].contract.interfaces)

    async def test_adds_no_interfaces_returning_error(self):
        self.__rpc_client.calls.return_value = {
            ERC165InterfaceID.ERC721.value: (True,),
            ERC165InterfaceID.ERC721_METADATA.value: RPCServerError("", "", "", ""),
            ERC165InterfaceID.ERC721_ENUMERABLE.value: (True,),
            ERC165InterfaceID.ERC1155.value: RPCServerError("", "", "", ""),
            ERC165InterfaceID.ERC1155_METADATA_URI.value: RPCServerError("", "", "", ""),
        }
        actual = await self.__processor(self.__default_batch)
        expected = [ERC165InterfaceID.ERC721, ERC165InterfaceID.ERC721_ENUMERABLE]
        self.assertEqual(expected, actual[0].contract.interfaces)

    async def test_sets_name_when_not_error(self):
        expected = "Expected Name"
        self.__rpc_client.calls.return_value = {
            "name": (expected[:],),
            ERC165InterfaceID.ERC721.value: (True,),
        }
        actual = await self.__processor(self.__default_batch)
        self.assertEqual(expected, actual[0].contract.name)

    async def test_sets_error_for_name_when_error(self):
        self.__rpc_client.calls.return_value = {
            "name": RPCServerError("", "", "", ""),
            ERC165InterfaceID.ERC721.value: (True,),
        }
        actual = await self.__processor(self.__default_batch)
        self.assertEqual("#ERROR", actual[0].contract.name)

    async def test_sets_symbol_when_not_error(self):
        expected = "Expected Symbol"
        self.__rpc_client.calls.return_value = {
            "symbol": (expected[:],),
            ERC165InterfaceID.ERC721.value: (True,),
        }
        actual = await self.__processor(self.__default_batch)
        self.assertEqual(expected, actual[0].contract.symbol)

    async def test_sets_error_for_symbol_when_error(self):
        self.__rpc_client.calls.return_value = {
            "symbol": RPCServerError("", "", "", ""),
            ERC165InterfaceID.ERC721.value: (True,),
        }
        actual = await self.__processor(self.__default_batch)
        self.assertEqual("#ERROR", actual[0].contract.symbol)

    async def test_sets_total_supply_when_not_error(self):
        expected = "Expected Supply"
        self.__rpc_client.calls.return_value = {
            "total_supply": (expected[:],),
            ERC165InterfaceID.ERC721.value: (True,),
        }
        actual = await self.__processor(self.__default_batch)
        self.assertEqual(expected, actual[0].contract.total_supply)

    async def test_sets_error_for_total_supply_when_error(self):
        self.__rpc_client.calls.return_value = {
            "total_supply": RPCServerError("", "", "", ""),
            ERC165InterfaceID.ERC721.value: (True,),
        }
        actual = await self.__processor(self.__default_batch)
        self.assertEqual("#ERROR", actual[0].contract.total_supply)

    async def test_does_not_return_contract_when_not_721(self):
        actual = await self.__processor(self.__default_batch)
        self.assertEqual([], actual)


class CollectionPersistenceBatchProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__default_batch = [
            ContractTransportObject(
                transaction_receipt=TransactionReceipt(
                    transaction_hash="",
                    transaction_index="0x02",
                    block_hash="",
                    block_number="0x01",
                    from_="",
                    to_="",
                    cumulative_gas_used="",
                    gas_used="",
                    contract_address="",
                    logs=list(),
                    logs_bloom="",
                    root="",
                    status="",
                ),
                block=Block(
                    number="0x00",
                    hash="",
                    parent_hash="",
                    nonce="",
                    sha3_uncles="",
                    logs_bloom="",
                    transactions_root="",
                    state_root="",
                    receipts_root="",
                    miner="",
                    mix_hash="",
                    difficulty="",
                    total_difficulty="",
                    extra_data="",
                    size="",
                    gas_limit="",
                    gas_used="",
                    timestamp="0x00",
                    transactions=list(),
                    uncles=list(),
                ),
                contract=Contract(
                    address="",
                    creator="",
                    interfaces=list(),
                    name="",
                    symbol="",
                    total_supply=0,
                ),
            ),
        ]

        self.__dynamodb = AsyncMock()
        self.__dynamodb.Table.return_value.batch_writer = async_context_manager_mock()
        self.__stats_service = MagicMock()
        self.__blockchain = "expected blockchain"
        self.__processor = CollectionPersistenceBatchProcessor(
            dynamodb=self.__dynamodb,
            stats_service=self.__stats_service,
            blockchain=self.__blockchain[:],
        )

    async def test_increments_stats_for_each_token(self):
        tokens = random.randint(1, 99)
        batch_items = list()
        for _ in range(tokens):
            batch_items.append(
                ContractTransportObject(
                    transaction_receipt=TransactionReceipt(
                        transaction_hash="",
                        transaction_index="0x02",
                        block_hash="",
                        block_number="0x01",
                        from_="",
                        to_="",
                        cumulative_gas_used="",
                        gas_used="",
                        contract_address="",
                        logs=list(),
                        logs_bloom="",
                        root="",
                        status="",
                    ),
                    block=Block(
                        number="0x00",
                        hash="",
                        parent_hash="",
                        nonce="",
                        sha3_uncles="",
                        logs_bloom="",
                        transactions_root="",
                        state_root="",
                        receipts_root="",
                        miner="",
                        mix_hash="",
                        difficulty="",
                        total_difficulty="",
                        extra_data="",
                        size="",
                        gas_limit="",
                        gas_used="",
                        timestamp="0x00",
                        transactions=list(),
                        uncles=list(),
                    ),
                    contract=Contract(
                        address="",
                        creator="",
                        interfaces=list(),
                        name="",
                        symbol="",
                        total_supply=0,
                    ),
                )
            )
        await self.__processor(batch_items)
        self.assertEqual(
            tokens,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were transactions",
        )
        self.__stats_service.increment.assert_called_with("collection_persisted")

    async def test_records_dynamodb_timer_in_stats(self):
        await self.__processor(self.__default_batch)
        assert_timer_run(self.__stats_service, "dynamodb_write_collections")

    async def test_stores_correct_data(self):
        batch = [
            ContractTransportObject(
                transaction_receipt=TransactionReceipt(
                    transaction_hash="transaction hash",
                    transaction_index="0x02",
                    block_hash="",
                    block_number="0x01",
                    from_="from address",
                    to_="",
                    cumulative_gas_used="",
                    gas_used="",
                    contract_address="",
                    logs=list(),
                    logs_bloom="",
                    root="",
                    status="",
                ),
                block=Block(
                    number="0x00",
                    hash="",
                    parent_hash="",
                    nonce="",
                    sha3_uncles="",
                    logs_bloom="",
                    transactions_root="",
                    state_root="",
                    receipts_root="",
                    miner="",
                    mix_hash="",
                    difficulty="",
                    total_difficulty="",
                    extra_data="",
                    size="",
                    gas_limit="",
                    gas_used="",
                    timestamp="0x0F",
                    transactions=list(),
                    uncles=list(),
                ),
                contract=Contract(
                    address="Expected Contract Address",
                    creator="from address",
                    interfaces=[ERC165InterfaceID.ERC721, ERC165InterfaceID.ERC1155],
                    name="name",
                    symbol="symbol",
                    total_supply=99,
                ),
            ),
        ]

        await self.__processor(batch)
        context_manager = self.__dynamodb.Table.return_value.batch_writer.return_value.__aenter__
        context_manager.return_value.put_item.assert_awaited_once_with(
            Item={
                "blockchain": self.__blockchain,
                "collection_id": "Expected Contract Address",
                "block_number": 1,
                "transaction_index": 2,
                "transaction_hash": "transaction hash",
                "creator": "from address",
                "timestamp": 15,
                "name": "name",
                "symbol": "symbol",
                "total_supply": 99,
                "interfaces": ["0x80ac58cd", "0xd9b67a26"],
            }
        )


class TokenMetadataUriBatchProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__default_batch = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                )
            ),
        ]
        self.__rpc_client = AsyncMock()
        self.__rpc_client.calls.return_value = {"0x10-0x30-721": (None,), "0x10-0x30-1155": (None,)}
        self.__stats_service = MagicMock()
        self.__processor = TokenMetadataUriBatchProcessor(
            stats_service=self.__stats_service,
            rpc_client=self.__rpc_client,
        )

    async def test_increments_stats_for_each_token(self):
        tokens = random.randint(1, 99)
        batch_items = list()
        for _ in range(tokens):
            batch_items.append(
                TokenTransportObject(
                    token=Token(
                        collection_id="0x10",
                        original_owner="0x20",
                        token_id=HexInt("0x30"),
                        timestamp=HexInt("0x40"),
                    )
                )
            )
        await self.__processor(batch_items)
        self.assertEqual(
            tokens,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were tokens",
        )
        self.__stats_service.increment.assert_called_with("token_metadata_uris_processed")

    async def test_calls_721_and_1155_methods_with_correct_data_for_all_tokens(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x03",
                    original_owner="",
                    token_id=HexInt("0x01"),
                    timestamp=HexInt("0x00"),
                )
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="0x03",
                    original_owner="",
                    token_id=HexInt("0x02"),
                    timestamp=HexInt("0x00"),
                )
            ),
        ]
        self.__rpc_client.calls.return_value = {
            "0x03-0x01-721": (None,),
            "0x03-0x01-1155": (None,),
            "0x03-0x02-721": (None,),
            "0x03-0x02-1155": (None,),
        }
        await self.__processor(batch_items)
        self.__rpc_client.calls.assert_awaited_once_with(
            [
                EthCall("0x03-0x01-721", None, "0x03", ERC721MetadataFunctions.TOKEN_URI, [1]),
                EthCall("0x03-0x01-1155", None, "0x03", ERC1155MetadataURIFunctions.URI, [1]),
                EthCall("0x03-0x02-721", None, "0x03", ERC721MetadataFunctions.TOKEN_URI, [2]),
                EthCall("0x03-0x02-1155", None, "0x03", ERC1155MetadataURIFunctions.URI, [2]),
            ]
        )

    async def test_uses_721_URI_when_1155_URI_not_present(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x03",
                    original_owner="",
                    token_id=HexInt("0x01"),
                    timestamp=HexInt("0x00"),
                )
            ),
        ]
        self.__rpc_client.calls.return_value = {
            "0x03-0x01-721": ("721 URI",),
            "0x03-0x01-1155": RPCServerError(
                rpc_version="", error_code="", error_message="", request_id="0x01-1155"
            ),
        }
        actual = await self.__processor(batch_items)
        self.assertEqual("721 URI", actual[0].token.metadata_uri)

    async def test_uses_1155_URI_when_present(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x02",
                    original_owner="",
                    token_id=HexInt("0x01"),
                    timestamp=HexInt("0x00"),
                )
            ),
        ]
        self.__rpc_client.calls.return_value = {
            "0x02-0x01-721": ("721 URI",),
            "0x02-0x01-1155": ("1155 URI",),
        }
        actual = await self.__processor(batch_items)
        self.assertEqual("1155 URI", actual[0].token.metadata_uri)

    async def test_sets_none_when_no_uri_from_either(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x02",
                    original_owner="",
                    token_id=HexInt("0x01"),
                    timestamp=HexInt("0x00"),
                )
            ),
        ]
        self.__rpc_client.calls.return_value = {
            "0x02-0x01-721": RPCServerError(
                rpc_version="", error_code="", error_message="", request_id="0x01-721"
            ),
            "0x02-0x01-1155": RPCServerError(
                rpc_version="", error_code="", error_message="", request_id="0x01-1155"
            ),
        }
        self.__rpc_client.calls.return_value = {
            "0x02-0x01-721": (None,),
            "0x02-0x01-1155": (None,),
        }
        actual = await self.__processor(batch_items)
        self.assertEqual(None, actual[0].token.metadata_uri)


class TokenMetadataRetrievalBatchProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock()
        self.__stats_service = MagicMock()
        self.__http_data_client = AsyncMock()
        self.__ipfs_data_client = AsyncMock()
        self.__arweave_data_client = AsyncMock()

        self.__processor = TokenMetadataRetrievalBatchProcessor(
            stats_service=self.__stats_service,
            http_data_client=self.__http_data_client,
            ipfs_data_client=self.__ipfs_data_client,
            arweave_data_client=self.__arweave_data_client,
        )

    async def test_increments_stats_for_each_token(self):
        tokens = random.randint(1, 99)
        batch_items = list()
        for _ in range(tokens):
            batch_items.append(
                TokenTransportObject(
                    token=Token(
                        collection_id="0x10",
                        original_owner="0x20",
                        token_id=HexInt("0x30"),
                        timestamp=HexInt("0x40"),
                        metadata_uri="metadata URI",
                    )
                )
            )
        await self.__processor(batch_items)
        self.assertEqual(
            tokens,
            len(
                [
                    a
                    for a in self.__stats_service.increment.call_args_list
                    if a == (("token_metadata_retrieves",),)
                ]
            ),
            "Expected as many increment calls as there were tokens",
        )
        self.__stats_service.increment.assert_called_with("token_metadata_retrieves")

    async def test_adds_encoding_error_stat_when_decode_error_raised(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="http://metadata.uri",
                ),
            ),
        ]
        self.__http_data_client.get.side_effect = UnicodeDecodeError("", b"", 1, 1, "")
        await self.__processor(batch_items)
        self.__stats_service.increment.assert_any_call("metadata_retrieval_encoding_error")

    async def test_adds_unsupported_stat_when_unsupported_protocol_in_uri(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="unsupported://metadata.uri",
                ),
            ),
        ]
        await self.__processor(batch_items)
        self.__stats_service.increment.assert_any_call("metadata_retrieval_unsupported_protocol")

    async def test_properly_handles_multiple_tokens_with_the_same_metadata_uri(self):
        tto1 = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x01"),
                timestamp=HexInt("0x40"),
                metadata_uri="http://metadata.uri",
            ),
        )
        tto2 = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x02"),
                timestamp=HexInt("0x40"),
                metadata_uri="http://metadata.uri",
            ),
        )
        batch_items = [tto1, tto2]
        tto1 = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x01"),
                timestamp=HexInt("0x40"),
                metadata_uri="http://metadata.uri",
                metadata="expected metadata",
            ),
        )
        tto2 = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x02"),
                timestamp=HexInt("0x40"),
                metadata_uri="http://metadata.uri",
                metadata="expected metadata",
            ),
        )
        expected = [tto1, tto2]
        self.__http_data_client.get.return_value = "expected metadata"
        actual = await self.__processor(batch_items)
        self.assertEqual(expected, actual)

    async def test_uses_http_client_when_uri_is_http_s(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="http://metadata.uri/1",
                ),
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x50"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="https://metadata.uri/2",
                ),
            ),
        ]
        await self.__processor(batch_items)
        self.__http_data_client.get.assert_has_awaits(
            (call("http://metadata.uri/1"), call("https://metadata.uri/2"))
        )

    async def test_adds_http_client_timing_when_uri_is_http_s(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="http://metadata.uri",
                ),
            ),
        ]
        await self.__processor(batch_items)
        self.__stats_service.timer.assert_called_once_with("metadata_retrieval_http")

    async def test_adds_error_stats_when_http_errors(self):
        self.__http_data_client.get.side_effect = ProtocolError()
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="http://meadata.uri",
                )
            ),
        ]
        await self.__processor(batch_items)
        self.__stats_service.increment.assert_any_call("metadata_retrieval_http_error")

    async def test_no_change_to_token_when_http_errors(self):
        self.__http_data_client.get.side_effect = ProtocolError()
        tto = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x30"),
                timestamp=HexInt("0x40"),
                metadata_uri="http://meadata.uri",
            )
        )
        actual = await self.__processor([tto])
        self.assertEqual([tto], actual)

    async def test_places_http_response_in_result_token(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="http://meadata.uri/0x30",
                )
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x31"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="http://meadata.uri/0x31",
                )
            ),
        ]
        self.__http_data_client.get.side_effect = ["0x30 metadata", "0x31 metadata"]

        actual = await self.__processor(batch_items)
        expected = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="http://meadata.uri/0x30",
                    metadata="0x30 metadata",
                )
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x31"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="http://meadata.uri/0x31",
                    metadata="0x31 metadata",
                )
            ),
        ]
        self.assertEqual(expected, actual)

    async def test_uses_ipfs_client_when_uri_is_ipfs(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ipfs://metadata.uri/1",
                ),
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x50"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ipfs://metadata.uri/2",
                ),
            ),
        ]
        await self.__processor(batch_items)
        self.__ipfs_data_client.get.assert_has_awaits(
            [call("ipfs://metadata.uri/1"), call("ipfs://metadata.uri/2")]
        )

    async def test_adds_ipfs_client_timing_when_uri_is_ipfs(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ipfs://metadata.uri",
                ),
            ),
        ]
        await self.__processor(batch_items)
        self.__stats_service.timer.assert_called_once_with("metadata_retrieval_ipfs")

    async def test_adds_error_stats_when_ipfs_errors(self):
        self.__ipfs_data_client.get.side_effect = ProtocolError()
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ipfs://meadata.uri",
                )
            ),
        ]
        await self.__processor(batch_items)
        self.__stats_service.increment.assert_any_call("metadata_retrieval_ipfs_error")

    async def test_no_change_to_token_when_ipfs_errors(self):
        self.__ipfs_data_client.get.side_effect = ProtocolError()
        tto = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x30"),
                timestamp=HexInt("0x40"),
                metadata_uri="ipfs://meadata.uri",
            )
        )
        actual = await self.__processor([tto])
        self.assertEqual([tto], actual)

    async def test_places_ipfs_response_in_response(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ipfs://meadata.uri/0x30",
                )
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x31"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ipfs://meadata.uri/0x31",
                )
            ),
        ]
        self.__ipfs_data_client.get.side_effect = ["0x30 metadata", "0x31 metadata"]

        actual = await self.__processor(batch_items)
        expected = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ipfs://meadata.uri/0x30",
                    metadata="0x30 metadata",
                )
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x31"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ipfs://meadata.uri/0x31",
                    metadata="0x31 metadata",
                )
            ),
        ]
        self.assertEqual(expected, actual)

    async def test_uses_arweave_client_when_uri_is_arweave(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ar://metadata.uri/1",
                ),
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x50"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ar://metadata.uri/2",
                ),
            ),
        ]
        await self.__processor(batch_items)
        self.__arweave_data_client.get.assert_has_awaits(
            (call("ar://metadata.uri/1"), call("ar://metadata.uri/2"))
        )

    async def test_adds_arweave_client_timing_when_uri_is_arweave(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ar://metadata.uri",
                ),
            ),
        ]
        await self.__processor(batch_items)
        self.__stats_service.timer.assert_called_once_with("metadata_retrieval_arweave")

    async def test_adds_error_stats_when_arweave_errors(self):
        self.__arweave_data_client.get.side_effect = ProtocolError()
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ar://meadata.uri",
                )
            )
        ]
        await self.__processor(batch_items)
        self.__stats_service.increment.assert_any_call("metadata_retrieval_arweave_error")

    async def test_no_change_to_token_when_arweave_errors(self):
        self.__arweave_data_client.get.side_effect = ProtocolError()
        tto = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x30"),
                timestamp=HexInt("0x40"),
                metadata_uri="ar://meadata.uri",
            )
        )
        actual = await self.__processor([tto])
        self.assertEqual([tto], actual)

    async def test_places_arweave_response_in_result_token(self):
        batch_items = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ar://meadata.uri/0x30",
                )
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x31"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ar://meadata.uri/0x31",
                )
            ),
        ]
        self.__arweave_data_client.get.side_effect = ["0x30 metadata", "0x31 metadata"]

        actual = await self.__processor(batch_items)
        expected = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ar://meadata.uri/0x30",
                    metadata="0x30 metadata",
                )
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x31"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ar://meadata.uri/0x31",
                    metadata="0x31 metadata",
                )
            ),
        ]
        self.assertEqual(expected, actual)

    async def test_token_transaction_with_invalid_uri_from_input_to_output(self):
        tto = TokenTransportObject(
            block=Block(
                number="0x00",
                hash="",
                parent_hash="",
                nonce="",
                sha3_uncles="",
                logs_bloom="",
                transactions_root="",
                state_root="",
                receipts_root="",
                miner="",
                mix_hash="",
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="0x0",
                transactions=list(),
                uncles=list(),
            ),
            token=Token(
                collection_id="collection ID",
                original_owner="original owner",
                token_id=HexInt("0x10"),
                timestamp=HexInt("0x11"),
                metadata_uri="metadata uri",
            ),
        )
        actual = await self.__processor([tto])
        self.assertEqual([tto], actual)


class TokenPersistenceBatchProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__default_batch = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="Metadata URI",
                    metadata="the metadata",
                )
            ),
        ]

        self.__dynamodb = AsyncMock()
        self.__dynamodb.Table.return_value.batch_writer = async_context_manager_mock()
        self.__dynamodb_batch_writer_context = (
            self.__dynamodb.Table.return_value.batch_writer.return_value.__aenter__.return_value
        )
        self.__stats_service = MagicMock()
        self.__logger = MagicMock()
        self.__blockchain = "expected blockchain"
        self.__processor = TokenPersistenceBatchProcessor(
            dynamodb=self.__dynamodb,
            stats_service=self.__stats_service,
            logger=self.__logger,
            blockchain=self.__blockchain[:],
        )

    async def test_increments_stats_for_each_token_mint(self):
        tokens = random.randint(1, 99)
        batch_items = list()
        for _ in range(tokens):
            batch_items.append(
                TokenTransportObject(
                    token=Token(
                        collection_id="0x10",
                        original_owner="0x20",
                        token_id=HexInt("0x30"),
                        timestamp=HexInt("0x40"),
                    )
                )
            )
        await self.__processor(batch_items)
        self.assertEqual(
            tokens,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were transactions",
        )
        self.__stats_service.increment.assert_called_with("tokens_persisted")

    async def test_records_dynamodb_timer_in_stats(self):
        await self.__processor(self.__default_batch)
        assert_timer_run(self.__stats_service, "dynamodb_write_tokens")

    @patch("chainconductor.blockcrawler.processors.keccak", return_value=b"keccak hash")
    async def test_uses_keccak_of_blockchain_and_collection_id_for_blockchain_collection_id(
        self, keccak_patch
    ):
        tto = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x30"),
                timestamp=HexInt("0x40"),
            )
        )
        await self.__processor([tto])
        keccak_patch.assert_called_once_with(self.__blockchain.encode("utf8") + b"0x10")
        self.__dynamodb_batch_writer_context.put_item.assert_awaited_once()
        actual = self.__dynamodb_batch_writer_context.put_item.call_args.kwargs["Item"][
            "blockchain_collection_id"
        ]
        self.assertEqual(b"keccak hash".hex(), actual)

    @patch("chainconductor.blockcrawler.processors.keccak")
    async def test_writes_appropriate_data_when_no_metadata_to_dynamo_db(self, keccak_patch):
        keccak_patch.return_value = b"keccak_hash"
        tto = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x30"),
                timestamp=HexInt("0x40"),
            )
        )
        await self.__processor([tto])
        self.__dynamodb_batch_writer_context.put_item.assert_awaited_once_with(
            Item={
                "blockchain_collection_id": b"keccak_hash".hex(),
                "token_id": "48",
                "blockchain": self.__blockchain,
                "collection_id": "0x10",
                "original_owner": "0x20",
                "mint_timestamp": 64,
                "metadata_uri": None,
                "metadata": None,
            }
        )

    @patch("chainconductor.blockcrawler.processors.keccak")
    async def test_writes_appropriate_data_when_metadata_to_dynamo_db(self, keccak_patch):
        keccak_patch.return_value = b"keccak_hash"
        tto = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x30"),
                timestamp=HexInt("0x40"),
                metadata_uri="Metadata URI",
                metadata="the metadata",
            )
        )
        await self.__processor([tto])
        self.__dynamodb_batch_writer_context.put_item.assert_awaited_once_with(
            Item={
                "blockchain_collection_id": b"keccak_hash".hex(),
                "token_id": "48",
                "blockchain": self.__blockchain,
                "collection_id": "0x10",
                "original_owner": "0x20",
                "mint_timestamp": 64,
                "metadata_uri": "Metadata URI",
                "metadata": "the metadata",
            }
        )

    async def test_logs_exception_for_error_writing_batch(self):
        batch_writer = self.__dynamodb.Table.return_value.batch_writer.return_value
        batch_writer.__aexit__.side_effect = ClientError({}, "name")
        await self.__processor([])
        self.__logger.exception.assert_called_once()


class TokenTransportTestCase(TestCase):
    def test_same_attributes_is_equal(self):
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
        token = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x11"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        left = TokenTransportObject(
            block=block,
            transaction_receipt=transaction_receipt,
            token=token,
        )
        right = TokenTransportObject(
            block=block,
            transaction_receipt=transaction_receipt,
            token=token,
        )
        self.assertEqual(left, right)

    def test_different_blocks_is_not_equal(self):
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
        token = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x11"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        left = TokenTransportObject(
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
            token=token,
        )
        right = TokenTransportObject(
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
            token=token,
        )
        self.assertNotEqual(left, right)

    def test_different_transaction_receipts_is_not_equal(self):
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
        token = Token(
            collection_id="str",
            original_owner="str",
            token_id=HexInt("0x11"),
            timestamp=HexInt("0x00"),
            metadata_uri="metadata_uri",
            metadata="metadata",
        )
        left = TokenTransportObject(
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
            token=token,
        )
        right = TokenTransportObject(
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
                status="0x00",
            ),
            token=token,
        )
        self.assertNotEqual(left, right)

    def test_different_tokens_is_not_equal(self):
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
        left = TokenTransportObject(
            block=block,
            transaction_receipt=transaction_receipt,
            token=Token(
                collection_id="str",
                original_owner="str",
                token_id=HexInt("0x11"),
                timestamp=HexInt("0x00"),
                metadata_uri="metadata_uri",
                metadata="metadata",
            ),
        )
        right = TokenTransportObject(
            block=block,
            transaction_receipt=transaction_receipt,
            token=Token(
                collection_id="str",
                original_owner="str",
                token_id=HexInt("0x12"),
                timestamp=HexInt("0x00"),
                metadata_uri="metadata_uri",
                metadata="metadata",
            ),
        )
        self.assertNotEqual(left, right)


class RPCErrorRetryDecoratingBatchProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__decorated_processor = AsyncMock()
        self.__processor = RPCErrorRetryDecoratingBatchProcessor(self.__decorated_processor)

    async def test_passes_input_to_decorated_processor(self):
        expected = ["expected input"]
        await self.__processor(expected.copy())
        self.__decorated_processor.assert_awaited_once_with(expected)

    async def test_returns_results_from_decorated_processor(self):
        expected = "expected result"
        self.__decorated_processor.return_value = expected[:]
        actual = await self.__processor(list())
        self.assertEqual(expected, actual)

    async def test_does_not_retry_with_non_RPCError(self):
        retries = random.randint(1, 20)
        processor = RPCErrorRetryDecoratingBatchProcessor(
            self.__decorated_processor, max_retries=retries
        )
        self.__decorated_processor.side_effect = Exception("Argh")
        with self.assertRaisesRegex(Exception, "Argh"):
            await processor([])
        self.__decorated_processor.assert_called_once()

    async def test_retries_max_retries_amount_with_RPCError_and_reraises(self):
        retries = random.randint(1, 20)
        processor = RPCErrorRetryDecoratingBatchProcessor(
            self.__decorated_processor, max_retries=retries
        )
        self.__decorated_processor.side_effect = RPCServerError("Argh", "", "", "")
        with self.assertRaisesRegex(RPCServerError, "Argh"):
            await processor([])
        self.__decorated_processor.assert_has_awaits([call([])] * retries)
