import random
import unittest
from asyncio import QueueEmpty
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch, call

import ddt
from eth_abi import encode
from eth_hash.auto import keccak
from eth_utils import decode_hex

from chainconductor.blockcrawler.data_clients import ProtocolError
from chainconductor.blockcrawler.processors import (
    BlockProcessor,
    BlockIDTransportObject,
    ContractTransportObject,
    TokenTransportObject,
    TransactionProcessor,
    TokenTransferProcessor,
    CollectionPersistenceProcessor,
    TokenMetadataProcessor,
    Token,
    TokenPersistenceProcessor,
    TokenMetadataGatheringProcessor,
)
from chainconductor.web3.rpc import EthCall, RPCError
from chainconductor.web3.types import (
    Block,
    Transaction,
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
)

from .. import async_context_manager_mock


def assert_timer_run(mocked_stats_service: MagicMock, timer):
    mocked_stats_service.timer.assert_called_once_with(timer)
    # enter starts the timer
    mocked_stats_service.timer.return_value.__enter__.assert_called_once()
    # exit ends the timer and records
    mocked_stats_service.timer.return_value.__exit__.assert_called_once()


@ddt.ddt
class BlockProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock()
        self.__rpc_client.get_blocks.return_value = []
        self.__inbound_queue = (
            MagicMock()
        )  # Queue is asynchronous, but we don't use any async methods
        self.__inbound_queue.get_nowait.side_effect = [
            BlockIDTransportObject(0),
            QueueEmpty(),
        ]
        self.__outbound_queue = AsyncMock()
        self.__stats_service = MagicMock()
        self.__event_bus = AsyncMock()
        self.__processor = BlockProcessor(
            rpc_client=self.__rpc_client,
            inbound_queue=self.__inbound_queue,
            transaction_queue=self.__outbound_queue,
            event_bus=self.__event_bus,
            stats_service=self.__stats_service,
            rpc_batch_size=0,
            max_batch_wait=0,
        )

    async def test_triggers_stopped_event_in_event_bus(self):
        await self.__run_processor()
        self.__event_bus.trigger.assert_called_once_with("block_processor_stopped")

    async def __run_processor(self):
        # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.stop()
        await self.__processor.start()

    async def test_records_rpc_call_timer(self):
        await self.__run_processor()
        assert_timer_run(self.__stats_service, "rpc_get_blocks")

    async def test_increments_stats_for_each_block(self):
        block_ids = random.randint(1, 99)
        get_blocks_return_value = list()
        for block_id in range(block_ids):
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
        await self.__run_processor()
        self.assertEqual(
            block_ids,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were blocks",
        )
        self.__stats_service.increment.assert_called_with("blocks_processed")

    async def test_does_not_add_items_to_outbound_queue_if_there_are_no_transactions(
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
        await self.__run_processor()
        self.__outbound_queue.put.assert_not_called()

    async def test_does_not_add_items_to_outbound_queue_if_bloom_filter_has_no_transfers(
        self,
    ):
        self.__rpc_client.get_blocks.return_value = [
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
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="",
                transactions=[
                    Transaction(
                        block_hash="",
                        block_number="",
                        from_="",
                        gas="",
                        gas_price="",
                        hash="",
                        input="",
                        nonce="",
                        to_="someone",
                        transaction_index="",
                        value="",
                        v="",
                        r="",
                        s="",
                    ),
                ],
                uncles=list(),
            )
        ]
        await self.__run_processor()
        self.__outbound_queue.put.assert_not_called()

    async def test_adds_items_as_contract_dtos_to_outbound_queue_if_transaction_has_no_to(
        self,
    ):
        self.__rpc_client.get_blocks.return_value = [
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
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="",
                transactions=[
                    Transaction(
                        block_hash="",
                        block_number="",
                        from_="",
                        gas="",
                        gas_price="",
                        hash="",
                        input="",
                        nonce="",
                        to_=None,
                        transaction_index="",
                        value="",
                        v="",
                        r="",
                        s="",
                    ),
                ],
                uncles=list(),
            )
        ]
        await self.__run_processor()
        self.__outbound_queue.put.assert_called_once()
        self.assertIsInstance(self.__outbound_queue.put.call_args.args[0], ContractTransportObject)

    @patch("chainconductor.blockcrawler.processors.BloomFilter")
    async def test_checks_block_logs_bloom_for_transfer_event(self, bloom_filter_patch):
        self.__rpc_client.get_blocks.return_value = [
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
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="",
                transactions=[
                    Transaction(
                        block_hash="",
                        block_number="",
                        from_="",
                        gas="",
                        gas_price="",
                        hash="",
                        input="",
                        nonce="",
                        to_="someone",
                        transaction_index="",
                        value="",
                        v="",
                        r="",
                        s="",
                    ),
                ],
                uncles=list(),
            )
        ]
        await self.__run_processor()

        expected = decode_hex("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925")
        bloom_filter_patch.return_value.__contains__.assert_called_once_with(expected)

    @patch("chainconductor.blockcrawler.processors.BloomFilter")
    async def test_inits_block_logs_bloom_with_int_of_logs_bloom(self, bloom_filter_patch):
        self.__rpc_client.get_blocks.return_value = [
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
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="",
                transactions=[
                    Transaction(
                        block_hash="",
                        block_number="",
                        from_="",
                        gas="",
                        gas_price="",
                        hash="",
                        input="",
                        nonce="",
                        to_="someone",
                        transaction_index="",
                        value="",
                        v="",
                        r="",
                        s="",
                    ),
                ],
                uncles=list(),
            )
        ]
        await self.__run_processor()

        expected = int("0x998877665544332211", 16)
        bloom_filter_patch.assert_called_once_with(expected)

    @patch("chainconductor.blockcrawler.processors.BloomFilter")
    async def test_adds_items_as_token_dtos_to_outbound_queue_transfer_event_in_bloom_filter(
        self, bloom_filter_patch
    ):
        self.__rpc_client.get_blocks.return_value = [
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
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="",
                transactions=[
                    Transaction(
                        block_hash="",
                        block_number="",
                        from_="",
                        gas="",
                        gas_price="",
                        hash="",
                        input="",
                        nonce="",
                        to_="someone",
                        transaction_index="",
                        value="",
                        v="",
                        r="",
                        s="",
                    ),
                ],
                uncles=list(),
            )
        ]
        bloom_filter_patch.return_value.__contains__.return_value = True
        await self.__run_processor()
        self.__outbound_queue.put.assert_called_once()
        self.assertIsInstance(self.__outbound_queue.put.call_args.args[0], TokenTransportObject)


@ddt.ddt
class TransactionProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock()
        self.__rpc_client.get_transaction_receipts.return_value = [
            TransactionReceipt(
                transaction_hash="",
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
        self.__inbound_queue = MagicMock()
        self.__inbound_queue.get_nowait.side_effect = [
            ContractTransportObject(
                transaction=Transaction(
                    block_hash="",
                    block_number="",
                    from_="",
                    gas="",
                    gas_price="",
                    hash="",
                    input="",
                    nonce="",
                    to_="someone",
                    transaction_index="",
                    value="",
                    v="",
                    r="",
                    s="",
                ),
                block=Block(
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
                    difficulty="",
                    total_difficulty="",
                    extra_data="",
                    size="",
                    gas_limit="",
                    gas_used="",
                    timestamp="",
                    transactions=list(),
                    uncles=list(),
                ),
            ),
            QueueEmpty(),
        ]
        self.__outbound_contract_queue = AsyncMock()
        self.__outbound_token_queue = AsyncMock()
        self.__stats_service = MagicMock()
        self.__event_bus = AsyncMock()
        self.__processor = TransactionProcessor(
            rpc_client=self.__rpc_client,
            inbound_queue=self.__inbound_queue,
            outbound_contract_queue=self.__outbound_contract_queue,
            outbound_token_queue=self.__outbound_token_queue,
            event_bus=self.__event_bus,
            stats_service=self.__stats_service,
            rpc_batch_size=0,
            max_batch_wait=0,
        )

    async def __run_processor(self):
        await self.__processor.stop()
        await self.__processor.start()

    async def test_triggers_stopped_event_in_event_bus(self):
        # If we don't tell the processor to stop before it starts, it will run forever
        await self.__run_processor()
        self.__event_bus.trigger.assert_called_once_with("transaction_processor_stopped")

    async def test_records_rpc_call_timer(self):
        await self.__run_processor()
        assert_timer_run(self.__stats_service, "rpc_get_transaction_receipts")

    async def test_increments_stats_for_each_block(self):
        transactions = random.randint(1, 99)
        get_transaction_receipts_return_value = list()
        for _ in range(transactions):
            get_transaction_receipts_return_value.append(
                TransactionReceipt(
                    transaction_hash="",
                    transaction_index="",
                    block_hash="",
                    block_number="",
                    from_="",
                    to_="",
                    cumulative_gas_used="",
                    gas_used="",
                    contract_address="",
                    logs=list(),
                    logs_bloom="",
                    root="",
                    status="",
                )
            )
        self.__rpc_client.get_transaction_receipts.return_value = (
            get_transaction_receipts_return_value
        )
        await self.__run_processor()
        self.assertEqual(
            transactions,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were transactions",
        )
        self.__stats_service.increment.assert_called_with("transactions_processed")

    async def test_puts_contracts_on_contract_queue(self):
        cto = ContractTransportObject(
            transaction=Transaction(
                block_hash="",
                block_number="",
                from_="",
                gas="",
                gas_price="",
                hash="",
                input="",
                nonce="",
                to_="someone",
                transaction_index="",
                value="",
                v="",
                r="",
                s="",
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
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="",
                transactions=list(),
                uncles=list(),
            ),
        )
        self.__inbound_queue.get_nowait.side_effect = [cto, QueueEmpty()]
        await self.__run_processor()
        self.__outbound_contract_queue.put.assert_called_once()

    @patch("chainconductor.blockcrawler.processors.BloomFilter")
    async def test_puts_tokens_on_token_queue_if_transfer_event_in_logs_bloom(self, bloom_patch):
        tto = TokenTransportObject(
            transaction=Transaction(
                block_hash="",
                block_number="",
                from_="",
                gas="",
                gas_price="",
                hash="",
                input="",
                nonce="",
                to_="someone",
                transaction_index="",
                value="",
                v="",
                r="",
                s="",
            ),
            block=Block(
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
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="",
                transactions=list(),
                uncles=list(),
            ),
        )
        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty()]
        bloom_patch.return_value.__contains__.return_value = True
        await self.__run_processor()
        self.__outbound_token_queue.put.assert_called_once()

    @patch("chainconductor.blockcrawler.processors.BloomFilter")
    async def test_constructs_bloom_filter_with_transaction_logs_bloom(self, bloom_patch):
        self.__rpc_client.get_transaction_receipts.return_value = [
            TransactionReceipt(
                transaction_hash="",
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
        tto = TokenTransportObject(
            transaction=Transaction(
                block_hash="",
                block_number="",
                from_="",
                gas="",
                gas_price="",
                hash="",
                input="",
                nonce="",
                to_="someone",
                transaction_index="",
                value="",
                v="",
                r="",
                s="",
            ),
            block=Block(
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
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="",
                transactions=list(),
                uncles=list(),
            ),
        )
        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty()]
        await self.__run_processor()
        bloom_patch.assert_called_once_with(int("0x998877665544332211", 16))

    @patch("chainconductor.blockcrawler.processors.BloomFilter")
    async def test_checks_bloom_filter_against_transfer_event_hash(self, bloom_patch):
        tto = TokenTransportObject(
            transaction=Transaction(
                block_hash="",
                block_number="",
                from_="",
                gas="",
                gas_price="",
                hash="",
                input="",
                nonce="",
                to_="someone",
                transaction_index="",
                value="",
                v="",
                r="",
                s="",
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
                difficulty="",
                total_difficulty="",
                extra_data="",
                size="",
                gas_limit="",
                gas_used="",
                timestamp="",
                transactions=list(),
                uncles=list(),
            ),
        )
        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty()]
        await self.__run_processor()
        expected = decode_hex("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925")
        bloom_patch.return_value.__contains__.assert_called_once_with(expected)


@ddt.ddt
class TokenTransferProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__inbound_queue = MagicMock()
        self.__outbound_queue = AsyncMock()

        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                transaction=Transaction(
                    block_hash="",
                    block_number="",
                    from_="",
                    gas="",
                    gas_price="",
                    hash="",
                    input="",
                    nonce="",
                    to_="someone",
                    transaction_index="",
                    value="",
                    v="",
                    r="",
                    s="",
                ),
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
            QueueEmpty(),
        ]

        self.__dynamodb = AsyncMock()
        self.__dynamodb.Table.return_value.batch_writer = async_context_manager_mock()
        self.__stats_service = MagicMock()
        self.__event_bus = AsyncMock()
        self.__processor = TokenTransferProcessor(
            dynamodb=self.__dynamodb,
            stats_service=self.__stats_service,
            event_bus=self.__event_bus,
            inbound_queue=self.__inbound_queue,
            outbound_queue=self.__outbound_queue,
            dynamodb_batch_size=0,
            max_batch_wait=0,
        )

    async def __run_processor(self):
        await self.__processor.stop()
        await self.__processor.start()

    async def test_increments_stats_for_each_token(self):
        tokens = random.randint(1, 99)
        inbound_queue_gets = list()

        log_topics = [
            ERC721Events.TRANSFER.event_hash,
            encode(["address"], ["0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"]).hex(),
            encode(["address"], ["0xcd5db1feb8758b9bc8d3172d3f229b29ba47024f"]).hex(),
            encode(["uint256"], [1]).hex(),
        ]

        for _ in range(tokens):
            inbound_queue_gets.append(
                TokenTransportObject(
                    transaction=Transaction(
                        block_hash="",
                        block_number="",
                        from_="",
                        gas="",
                        gas_price="",
                        hash="",
                        input="",
                        nonce="",
                        to_="someone",
                        transaction_index="",
                        value="",
                        v="",
                        r="",
                        s="",
                    ),
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
        inbound_queue_gets.append(QueueEmpty())
        self.__inbound_queue.get_nowait.side_effect = inbound_queue_gets
        # If we don't tell the processor to stop before it starts, it will run forever
        await self.__run_processor()
        self.assertEqual(
            tokens,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were transactions",
        )
        self.__stats_service.increment.assert_called_with("token_transfers_processed")

    async def test_triggers_stopped_event_in_event_bus(self):
        # If we don't tell the processor to stop before it starts, it will run forever
        await self.__run_processor()
        self.__event_bus.trigger.assert_called_once_with("token_transfer_processor_stopped")

    async def test_records_dynamodb_timer_in_stats(self):
        # If we don't tell the processor to stop before it starts, it will run forever
        await self.__run_processor()
        assert_timer_run(self.__stats_service, "dynamodb_write_token_transfers")

    async def test_stores_correct_data(self):
        log_topics = [
            ERC721Events.TRANSFER.event_hash,
            encode(["address"], ["0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"]).hex(),
            encode(["address"], ["0x759a401a287ffad0fa2deae15fe3b6169506d657"]).hex(),
            encode(["uint256"], [1]).hex(),
        ]
        tto = TokenTransportObject(
            transaction=Transaction(
                block_hash="",
                block_number="",
                from_="wrong from",
                gas="",
                gas_price="",
                hash="",
                input="",
                nonce="",
                to_="wrong to",
                transaction_index="",
                value="",
                v="",
                r="",
                s="",
            ),
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

        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty]
        await self.__run_processor()
        context_manager = self.__dynamodb.Table.return_value.batch_writer.return_value.__aenter__
        context_manager.return_value.put_item.assert_called_once_with(
            Item={
                "blockchain": "Ethereum Mainnet",
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
            transaction=Transaction(
                block_hash="",
                block_number="",
                from_="wrong from",
                gas="",
                gas_price="",
                hash="",
                input="",
                nonce="",
                to_="wrong to",
                transaction_index="",
                value="",
                v="",
                r="",
                s="",
            ),
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

        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty]
        await self.__run_processor()
        context_manager = self.__dynamodb.Table.return_value.batch_writer.return_value.__aenter__
        context_manager.return_value.put_item.assert_not_called()
        self.__outbound_queue.put.assert_not_called()

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
    async def test_does_not_place_internal_transfer_events_onm_outbound_queue(
        self, from_address, to_address
    ):
        log_topics = [
            ERC721Events.TRANSFER.event_hash,
            encode(["address"], [from_address]).hex(),
            encode(["address"], [to_address]).hex(),
            encode(["uint256"], [1]).hex(),
        ]

        tto = TokenTransportObject(
            transaction=Transaction(
                block_hash="",
                block_number="",
                from_="wrong from",
                gas="",
                gas_price="",
                hash="",
                input="",
                nonce="",
                to_="wrong to",
                transaction_index="",
                value="",
                v="",
                r="",
                s="",
            ),
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

        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty]
        await self.__run_processor()
        self.__outbound_queue.put.assert_not_called()

    async def test_does_not_process_non_transfer_events(self):
        log_topics = [
            "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b",
            encode(["address"], ["0x0000000000000000000000000000000000000000"]).hex(),
            encode(["address"], ["0x759a401a287ffad0fa2deae15fe3b6169506d657"]).hex(),
            encode(["uint256"], [1]).hex(),
        ]

        tto = TokenTransportObject(
            transaction=Transaction(
                block_hash="",
                block_number="",
                from_="wrong from",
                gas="",
                gas_price="",
                hash="",
                input="",
                nonce="",
                to_="wrong to",
                transaction_index="",
                value="",
                v="",
                r="",
                s="",
            ),
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

        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty]
        await self.__run_processor()
        context_manager = self.__dynamodb.Table.return_value.batch_writer.return_value.__aenter__
        context_manager.return_value.put_item.assert_not_called()
        self.__outbound_queue.put.assert_not_called()

    @ddt.data(
        "0x759a401a287ffad0fa2deae15fe3b6169506d657", "0x0000000000000000000000000000000000000000"
    )
    async def test_token_transaction_from_minting_address_to_outbound_queue(self, from_address):
        log_topics = [
            ERC721Events.TRANSFER.event_hash,
            encode(["address"], [from_address]).hex(),
            encode(["address"], ["0xcd5db1feb8758b9bc8d3172d3f229b29ba47024f"]).hex(),
            encode(["uint256"], [1]).hex(),
        ]

        tto = TokenTransportObject(
            transaction=Transaction(
                block_hash="",
                block_number="",
                from_=from_address,
                gas="",
                gas_price="",
                hash="",
                input="",
                nonce="",
                to_="someone",
                transaction_index="",
                value="",
                v="",
                r="",
                s="",
            ),
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
        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty()]
        await self.__run_processor()
        self.__outbound_queue.put.assert_called_once()


class ContractPersistenceProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__inbound_queue = MagicMock()

        self.__inbound_queue.get_nowait.side_effect = [
            ContractTransportObject(
                transaction=Transaction(
                    block_hash="",
                    block_number="0x01",
                    from_="",
                    gas="",
                    gas_price="",
                    hash="",
                    input="",
                    nonce="",
                    to_="someone",
                    transaction_index="0x02",
                    value="",
                    v="",
                    r="",
                    s="",
                ),
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
            QueueEmpty(),
        ]

        self.__dynamodb = AsyncMock()
        self.__dynamodb.Table.return_value.batch_writer = async_context_manager_mock()
        self.__stats_service = MagicMock()
        self.__event_bus = AsyncMock()
        self.__processor = CollectionPersistenceProcessor(
            dynamodb=self.__dynamodb,
            stats_service=self.__stats_service,
            event_bus=self.__event_bus,
            inbound_queue=self.__inbound_queue,
            dynamodb_batch_size=0,
            max_batch_wait=0,
        )

    async def __run_processor(self):
        await self.__processor.stop()
        await self.__processor.start()

    async def test_increments_stats_for_each_token(self):
        tokens = random.randint(1, 99)
        inbound_queue_gets = list()
        for _ in range(tokens):
            inbound_queue_gets.append(
                ContractTransportObject(
                    transaction=Transaction(
                        block_hash="",
                        block_number="0x01",
                        from_="",
                        gas="",
                        gas_price="",
                        hash="",
                        input="",
                        nonce="",
                        to_="",
                        transaction_index="0x02",
                        value="",
                        v="",
                        r="",
                        s="",
                    ),
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
        inbound_queue_gets.append(QueueEmpty())
        self.__inbound_queue.get_nowait.side_effect = inbound_queue_gets
        # If we don't tell the processor to stop before it starts, it will run forever
        await self.__run_processor()
        self.assertEqual(
            tokens,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were transactions",
        )
        self.__stats_service.increment.assert_called_with("collection_persisted")

    async def test_triggers_stopped_event_in_event_bus(self):
        await self.__run_processor()
        self.__event_bus.trigger.assert_called_once_with("collection_persistence_stopped")

    async def test_records_dynamodb_timer_in_stats(self):
        await self.__run_processor()
        assert_timer_run(self.__stats_service, "dynamodb_write_collections")

    async def test_stores_correct_data(self):
        self.__inbound_queue.get_nowait.side_effect = [
            ContractTransportObject(
                transaction=Transaction(
                    block_hash="",
                    block_number="0x01",
                    from_="",
                    gas="",
                    gas_price="",
                    hash="transaction hash",
                    input="",
                    nonce="",
                    to_="someone",
                    transaction_index="0x02",
                    value="",
                    v="",
                    r="",
                    s="",
                ),
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
            QueueEmpty(),
        ]

        await self.__run_processor()
        context_manager = self.__dynamodb.Table.return_value.batch_writer.return_value.__aenter__
        context_manager.return_value.put_item.assert_called_once_with(
            Item={
                "blockchain": "Ethereum Mainnet",
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


class TokenMetadataProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__inbound_queue = MagicMock()
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                )
            ),
            QueueEmpty(),
        ]
        self.__outbound_gathering_queue = AsyncMock()
        self.__outbound_persistence_queue = AsyncMock()
        self.__rpc_client = AsyncMock()
        self.__rpc_client.calls.return_value = {"0x30-721": (None,), "0x30-1155": (None,)}
        self.__stats_service = MagicMock()
        self.__event_bus = AsyncMock()
        self.__processor = TokenMetadataProcessor(
            stats_service=self.__stats_service,
            event_bus=self.__event_bus,
            inbound_queue=self.__inbound_queue,
            outbound_token_gathering_queue=self.__outbound_gathering_queue,
            outbound_token_persistence_queue=self.__outbound_persistence_queue,
            rpc_client=self.__rpc_client,
            rpc_batch_size=99,
            max_batch_wait=99,
        )

    async def __run_processor(self):
        await self.__processor.stop()
        await self.__processor.start()

    async def test_increments_stats_for_each_token(self):
        tokens = random.randint(1, 99)
        inbound_queue_gets = list()
        for _ in range(tokens):
            inbound_queue_gets.append(
                TokenTransportObject(
                    token=Token(
                        collection_id="0x10",
                        original_owner="0x20",
                        token_id=HexInt("0x30"),
                        timestamp=HexInt("0x40"),
                    )
                )
            )
        inbound_queue_gets.append(QueueEmpty())
        self.__inbound_queue.get_nowait.side_effect = inbound_queue_gets
        await self.__run_processor()
        self.assertEqual(
            tokens,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were tokens",
        )
        self.__stats_service.increment.assert_called_with("token_metadata_processed")

    async def test_triggers_stopped_event_in_event_bus(self):
        await self.__run_processor()
        self.__event_bus.trigger.assert_called_once_with("token_metadata_processor_stopped")

    async def test_calls_721_and_1155_methods_with_correct_data_for_all_tokens(self):
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="collection id 1",
                    original_owner="",
                    token_id=HexInt("0x01"),
                    timestamp=HexInt("0x00"),
                )
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="collection id 2",
                    original_owner="",
                    token_id=HexInt("0x02"),
                    timestamp=HexInt("0x00"),
                )
            ),
            QueueEmpty(),
        ]
        self.__rpc_client.calls.return_value = {
            "0x01-721": (None,),
            "0x01-1155": (None,),
            "0x02-721": (None,),
            "0x02-1155": (None,),
        }
        await self.__run_processor()
        self.__rpc_client.calls.assert_called_once_with(
            [
                EthCall(
                    "0x01-721", None, "collection id 1", ERC721MetadataFunctions.TOKEN_URI, [1]
                ),
                EthCall("0x01-1155", None, "collection id 1", ERC1155MetadataURIFunctions.URI, [1]),
                EthCall(
                    "0x02-721", None, "collection id 2", ERC721MetadataFunctions.TOKEN_URI, [2]
                ),
                EthCall("0x02-1155", None, "collection id 2", ERC1155MetadataURIFunctions.URI, [2]),
            ]
        )

    async def test_uses_721_URI_when_1155_URI_not_present(self):
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="collection id 1",
                    original_owner="",
                    token_id=HexInt("0x01"),
                    timestamp=HexInt("0x00"),
                )
            ),
            QueueEmpty(),
        ]
        self.__rpc_client.calls.return_value = {
            "0x01-721": ("721 URI",),
            "0x01-1155": RPCError(
                rpc_version="", error_code="", error_message="", request_id="0x01-1155"
            ),
        }
        await self.__run_processor()
        self.__outbound_gathering_queue.put.assert_called_once()
        actual = self.__outbound_gathering_queue.put.call_args.args[0].token.metadata_uri
        self.assertEqual("721 URI", actual)

    async def test_uses_1155_URI_when_present(self):
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="collection id 1",
                    original_owner="",
                    token_id=HexInt("0x01"),
                    timestamp=HexInt("0x00"),
                )
            ),
            QueueEmpty(),
        ]
        self.__rpc_client.calls.return_value = {
            "0x01-721": ("721 URI",),
            "0x01-1155": ("1155 URI",),
        }
        await self.__run_processor()
        self.__outbound_gathering_queue.put.assert_called_once()
        actual = self.__outbound_gathering_queue.put.call_args.args[0].token.metadata_uri
        self.assertEqual("1155 URI", actual)

    async def test_sets_none_and_puts_in_persistence_queue_when_no_uri_from_either(self):
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="collection id 1",
                    original_owner="",
                    token_id=HexInt("0x01"),
                    timestamp=HexInt("0x00"),
                )
            ),
            QueueEmpty(),
        ]
        self.__rpc_client.calls.return_value = {
            "0x01-721": RPCError(
                rpc_version="", error_code="", error_message="", request_id="0x01-721"
            ),
            "0x01-1155": RPCError(
                rpc_version="", error_code="", error_message="", request_id="0x01-1155"
            ),
        }
        await self.__run_processor()
        self.__rpc_client.calls.return_value = {
            "0x01-721": (None,),
            "0x01-1155": (None,),
        }
        self.__outbound_persistence_queue.put.assert_called_once()
        actual = self.__outbound_persistence_queue.put.call_args.args[0].token.metadata_uri
        self.assertEqual(None, actual)

    async def test_token_transaction_direct_to_persistence_queue_for_invalid_token_id(self):
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
                token_id=HexInt(
                    "0x10000000000000000000000000000000000000000000000000000000000000000"
                ),
                timestamp=HexInt("0x11"),
            ),
        )
        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty()]
        await self.__run_processor()
        self.__outbound_persistence_queue.put.assert_called_once_with(tto)


class TokenMetadataGatheringProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__inbound_queue = MagicMock()
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="metadata URI",
                )
            ),
            QueueEmpty(),
        ]
        self.__outbound_queue = AsyncMock()
        self.__rpc_client = AsyncMock()
        self.__stats_service = MagicMock()
        self.__event_bus = AsyncMock()
        self.__http_batch_client = AsyncMock()
        self.__ipfs_batch_client = AsyncMock()
        self.__arweave_batch_client = AsyncMock()

        self.__processor = TokenMetadataGatheringProcessor(
            stats_service=self.__stats_service,
            event_bus=self.__event_bus,
            inbound_queue=self.__inbound_queue,
            outbound_queue=self.__outbound_queue,
            http_batch_client=self.__http_batch_client,
            ipfs_batch_client=self.__ipfs_batch_client,
            arweave_batch_client=self.__arweave_batch_client,
            http_batch_size=99,
            max_batch_wait=99,
        )

    async def __run_processor(self):
        await self.__processor.stop()
        await self.__processor.start()

    async def test_increments_stats_for_each_token(self):
        tokens = random.randint(1, 99)
        inbound_queue_gets = list()
        for _ in range(tokens):
            inbound_queue_gets.append(
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
        inbound_queue_gets.append(QueueEmpty())
        self.__inbound_queue.get_nowait.side_effect = inbound_queue_gets
        await self.__run_processor()
        self.assertEqual(
            tokens,
            len(
                [
                    a
                    for a in self.__stats_service.increment.call_args_list
                    if a == (("token_metadata_gathering_processed",),)
                ]
            ),
            "Expected as many increment calls as there were tokens",
        )
        self.__stats_service.increment.assert_called_with("token_metadata_gathering_processed")

    async def test_triggers_stopped_event_in_event_bus(self):
        await self.__run_processor()
        self.__event_bus.trigger.assert_called_once_with(
            "token_metadata_gathering_processor_stopped"
        )

    async def test_adds_unsupported_stat_when_unsupported_protocol_in_uri(self):
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="unsupported://metadata.uri",
                ),
            ),
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__stats_service.increment.assert_any_call("metadata_retrieval_unsupported_protocol")

    async def test_uses_http_client_when_uri_is_http_s(self):
        self.__inbound_queue.get_nowait.side_effect = [
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
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__http_batch_client.get.assert_called_once_with(
            {"http://metadata.uri/1", "https://metadata.uri/2"}
        )

    async def test_adds_http_client_timing_when_uri_is_http_s(self):
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="http://metadata.uri",
                ),
            ),
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x50"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="https://metadata.uri",
                ),
            ),
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__stats_service.timer.assert_called_once_with("metadata_retrieval_http")

    async def test_adds_error_stats_when_http_errors(self):
        self.__http_batch_client.get.return_value = {"http://meadata.uri": ProtocolError()}
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="http://meadata.uri",
                )
            ),
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__stats_service.increment.assert_any_call("metadata_retrieval_http_error")

    async def test_no_change_to_token_when_http_errors(self):
        self.__http_batch_client.get.return_value = {"http://meadata.uri": ProtocolError()}
        tto = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x30"),
                timestamp=HexInt("0x40"),
                metadata_uri="http://meadata.uri",
            )
        )
        self.__inbound_queue.get_nowait.side_effect = [
            tto,
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__outbound_queue.put.assert_called_once_with(tto)

    async def test_places_http_response_in_outbound_token(self):
        self.__inbound_queue.get_nowait.side_effect = [
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
            QueueEmpty(),
        ]
        self.__http_batch_client.get.return_value = {
            "http://meadata.uri/0x31": "0x31 metadata",
            "http://meadata.uri/0x30": "0x30 metadata",
        }

        await self.__run_processor()
        self.assertEqual(
            2, self.__outbound_queue.put.call_count, "Expected two outbound queue puts"
        )
        self.__outbound_queue.put.assert_has_calls(
            [
                call(
                    TokenTransportObject(
                        token=Token(
                            collection_id="0x10",
                            original_owner="0x20",
                            token_id=HexInt("0x30"),
                            timestamp=HexInt("0x40"),
                            metadata_uri="http://meadata.uri/0x30",
                            metadata="0x30 metadata",
                        )
                    )
                ),
                call(
                    TokenTransportObject(
                        token=Token(
                            collection_id="0x10",
                            original_owner="0x20",
                            token_id=HexInt("0x31"),
                            timestamp=HexInt("0x40"),
                            metadata_uri="http://meadata.uri/0x31",
                            metadata="0x31 metadata",
                        )
                    )
                ),
            ]
        )

    async def test_uses_ipfs_client_when_uri_is_ipfs(self):
        self.__inbound_queue.get_nowait.side_effect = [
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
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__ipfs_batch_client.get.assert_called_once_with(
            {"ipfs://metadata.uri/1", "ipfs://metadata.uri/2"}
        )

    async def test_adds_ipfs_client_timing_when_uri_is_ipfs(self):
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ipfs://metadata.uri",
                ),
            ),
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__stats_service.timer.assert_called_once_with("metadata_retrieval_ipfs")

    async def test_adds_error_stats_when_ipfs_errors(self):
        self.__ipfs_batch_client.get.return_value = {"ipfs://meadata.uri": ProtocolError()}
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ipfs://meadata.uri",
                )
            ),
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__stats_service.increment.assert_any_call("metadata_retrieval_ipfs_error")

    async def test_no_change_to_token_when_ipfs_errors(self):
        self.__ipfs_batch_client.get.return_value = {"ipfs://meadata.uri": ProtocolError()}
        tto = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x30"),
                timestamp=HexInt("0x40"),
                metadata_uri="ipfs://meadata.uri",
            )
        )
        self.__inbound_queue.get_nowait.side_effect = [
            tto,
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__outbound_queue.put.assert_called_once_with(tto)

    async def test_places_ipfs_response_in_outbound_token(self):
        self.__inbound_queue.get_nowait.side_effect = [
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
            QueueEmpty(),
        ]
        self.__ipfs_batch_client.get.return_value = {
            "ipfs://meadata.uri/0x31": "0x31 metadata",
            "ipfs://meadata.uri/0x30": "0x30 metadata",
        }

        await self.__run_processor()
        self.assertEqual(
            2, self.__outbound_queue.put.call_count, "Expected two outbound queue puts"
        )
        self.__outbound_queue.put.assert_has_calls(
            [
                call(
                    TokenTransportObject(
                        token=Token(
                            collection_id="0x10",
                            original_owner="0x20",
                            token_id=HexInt("0x30"),
                            timestamp=HexInt("0x40"),
                            metadata_uri="ipfs://meadata.uri/0x30",
                            metadata="0x30 metadata",
                        )
                    )
                ),
                call(
                    TokenTransportObject(
                        token=Token(
                            collection_id="0x10",
                            original_owner="0x20",
                            token_id=HexInt("0x31"),
                            timestamp=HexInt("0x40"),
                            metadata_uri="ipfs://meadata.uri/0x31",
                            metadata="0x31 metadata",
                        )
                    )
                ),
            ]
        )

    async def test_uses_arweave_client_when_uri_is_arweave(self):
        self.__inbound_queue.get_nowait.side_effect = [
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
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__arweave_batch_client.get.assert_called_once_with(
            {"ar://metadata.uri/1", "ar://metadata.uri/2"}
        )

    async def test_adds_arweave_client_timing_when_uri_is_arweave(self):
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ar://metadata.uri",
                ),
            ),
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__stats_service.timer.assert_called_once_with("metadata_retrieval_arweave")

    async def test_adds_error_stats_when_arweave_errors(self):
        self.__arweave_batch_client.get.return_value = {"ar://meadata.uri": ProtocolError()}
        self.__inbound_queue.get_nowait.side_effect = [
            TokenTransportObject(
                token=Token(
                    collection_id="0x10",
                    original_owner="0x20",
                    token_id=HexInt("0x30"),
                    timestamp=HexInt("0x40"),
                    metadata_uri="ar://meadata.uri",
                )
            ),
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__stats_service.increment.assert_any_call("metadata_retrieval_arweave_error")

    async def test_no_change_to_token_when_arweave_errors(self):
        self.__arweave_batch_client.get.return_value = {"ar://meadata.uri": ProtocolError()}
        tto = TokenTransportObject(
            token=Token(
                collection_id="0x10",
                original_owner="0x20",
                token_id=HexInt("0x30"),
                timestamp=HexInt("0x40"),
                metadata_uri="ar://meadata.uri",
            )
        )
        self.__inbound_queue.get_nowait.side_effect = [
            tto,
            QueueEmpty(),
        ]
        await self.__run_processor()
        self.__outbound_queue.put.assert_called_once_with(tto)

    async def test_places_arweave_response_in_outbound_token(self):
        self.__inbound_queue.get_nowait.side_effect = [
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
            QueueEmpty(),
        ]
        self.__arweave_batch_client.get.return_value = {
            "ar://meadata.uri/0x31": "0x31 metadata",
            "ar://meadata.uri/0x30": "0x30 metadata",
        }

        await self.__run_processor()
        self.assertEqual(
            2, self.__outbound_queue.put.call_count, "Expected two outbound queue puts"
        )
        self.__outbound_queue.put.assert_has_calls(
            [
                call(
                    TokenTransportObject(
                        token=Token(
                            collection_id="0x10",
                            original_owner="0x20",
                            token_id=HexInt("0x30"),
                            timestamp=HexInt("0x40"),
                            metadata_uri="ar://meadata.uri/0x30",
                            metadata="0x30 metadata",
                        )
                    )
                ),
                call(
                    TokenTransportObject(
                        token=Token(
                            collection_id="0x10",
                            original_owner="0x20",
                            token_id=HexInt("0x31"),
                            timestamp=HexInt("0x40"),
                            metadata_uri="ar://meadata.uri/0x31",
                            metadata="0x31 metadata",
                        )
                    )
                ),
            ]
        )

    async def test_token_transaction_from_inbound_queue_to_outbound_queue(self):
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
        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty()]
        await self.__run_processor()
        self.__outbound_queue.put.assert_called_once()


class TokenPersistenceProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__inbound_queue = MagicMock()
        self.__inbound_queue.get_nowait.side_effect = [
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
            QueueEmpty(),
        ]

        self.__dynamodb = AsyncMock()
        self.__dynamodb.Table.return_value.batch_writer = async_context_manager_mock()
        self.__dynamodb_batch_writer_context = (
            self.__dynamodb.Table.return_value.batch_writer.return_value.__aenter__.return_value
        )
        self.__stats_service = MagicMock()
        self.__event_bus = AsyncMock()
        self.__processor = TokenPersistenceProcessor(
            dynamodb=self.__dynamodb,
            stats_service=self.__stats_service,
            event_bus=self.__event_bus,
            inbound_queue=self.__inbound_queue,
            dynamodb_batch_size=0,
            max_batch_wait=0,
        )

    async def __run_processor(self):
        await self.__processor.stop()
        await self.__processor.start()

    async def test_increments_stats_for_each_token_mint(self):
        tokens = random.randint(1, 99)
        inbound_queue_gets = list()
        for _ in range(tokens):
            inbound_queue_gets.append(
                TokenTransportObject(
                    token=Token(
                        collection_id="0x10",
                        original_owner="0x20",
                        token_id=HexInt("0x30"),
                        timestamp=HexInt("0x40"),
                    )
                )
            )
        inbound_queue_gets.append(QueueEmpty())
        self.__inbound_queue.get_nowait.side_effect = inbound_queue_gets
        await self.__run_processor()
        self.assertEqual(
            tokens,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were transactions",
        )
        self.__stats_service.increment.assert_called_with("tokens_persisted")

    async def test_triggers_stopped_event_in_event_bus(self):
        await self.__run_processor()
        self.__event_bus.trigger.assert_called_once_with("token_persistence_processor_stopped")

    async def test_records_dynamodb_timer_in_stats(self):
        await self.__run_processor()
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
        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty()]
        await self.__run_processor()
        keccak_patch.assert_called_once_with(b"Ethereum Mainnet0x10")
        self.__dynamodb_batch_writer_context.put_item.assert_called_once()
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
        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty()]

        await self.__run_processor()
        self.__dynamodb_batch_writer_context.put_item.assert_called_once_with(
            Item={
                "blockchain_collection_id": b"keccak_hash".hex(),
                "token_id": "48",
                "blockchain": "Ethereum Mainnet",
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
        self.__inbound_queue.get_nowait.side_effect = [tto, QueueEmpty()]

        await self.__run_processor()
        self.__dynamodb_batch_writer_context.put_item.assert_called_once_with(
            Item={
                "blockchain_collection_id": b"keccak_hash".hex(),
                "token_id": "48",
                "blockchain": "Ethereum Mainnet",
                "collection_id": "0x10",
                "original_owner": "0x20",
                "mint_timestamp": 64,
                "metadata_uri": "Metadata URI",
                "metadata": "the metadata",
            }
        )


class TokenTransportTestCase(TestCase):
    def test_same_attributes_is_equal(self):
        transaction = Transaction(
            hash="hash",
            block_hash="block_hash",
            block_number="0x01",
            from_="from_",
            gas="gas",
            gas_price="gas_price",
            input="input",
            nonce="nonce",
            to_="to_",
            transaction_index="0x00",
            value="value",
            v="v",
            r="r",
            s="s",
        )
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
            difficulty="difficulty",
            total_difficulty="total_difficulty",
            extra_data="extra_data",
            size="size",
            gas_limit="gas_limit",
            gas_used="gas_used",
            timestamp="timestamp",
            transactions=[transaction],
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
            transaction=transaction,
            transaction_receipt=transaction_receipt,
            token=token,
        )
        right = TokenTransportObject(
            block=block,
            transaction=transaction,
            transaction_receipt=transaction_receipt,
            token=token,
        )
        self.assertEqual(left, right)

    def test_different_transactions_is_not_equal(self):
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
            difficulty="difficulty",
            total_difficulty="total_difficulty",
            extra_data="extra_data",
            size="size",
            gas_limit="gas_limit",
            gas_used="gas_used",
            timestamp="timestamp",
            transactions=[
                Transaction(
                    hash="hash",
                    block_hash="block_hash",
                    block_number="0x01",
                    from_="from_",
                    gas="gas",
                    gas_price="gas_price",
                    input="input",
                    nonce="nonce",
                    to_="to_",
                    transaction_index="0x00",
                    value="value",
                    v="v",
                    r="r",
                    s="s",
                )
            ],
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
            transaction=Transaction(
                hash="hash",
                block_hash="block_hash",
                block_number="0x01",
                from_="from_",
                gas="gas",
                gas_price="gas_price",
                input="input",
                nonce="nonce",
                to_="to_",
                transaction_index="0x01",
                value="value",
                v="v",
                r="r",
                s="s",
            ),
            transaction_receipt=transaction_receipt,
            token=token,
        )
        right = TokenTransportObject(
            block=block,
            transaction=Transaction(
                hash="hash",
                block_hash="block_hash",
                block_number="0x01",
                from_="from_",
                gas="gas",
                gas_price="gas_price",
                input="input",
                nonce="nonce",
                to_="to_",
                transaction_index="0x02",
                value="value",
                v="v",
                r="r",
                s="s",
            ),
            transaction_receipt=transaction_receipt,
            token=token,
        )
        self.assertNotEqual(left, right)

    def test_different_blocks_is_not_equal(self):
        transaction = Transaction(
            hash="hash",
            block_hash="block_hash",
            block_number="0x01",
            from_="from_",
            gas="gas",
            gas_price="gas_price",
            input="input",
            nonce="nonce",
            to_="to_",
            transaction_index="0x00",
            value="value",
            v="v",
            r="r",
            s="s",
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
                difficulty="difficulty",
                total_difficulty="total_difficulty",
                extra_data="extra_data",
                size="size",
                gas_limit="gas_limit",
                gas_used="gas_used",
                timestamp="timestamp",
                transactions=[transaction],
                uncles=["uncle1"],
            ),
            transaction=transaction,
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
                difficulty="difficulty",
                total_difficulty="total_difficulty",
                extra_data="extra_data",
                size="size",
                gas_limit="gas_limit",
                gas_used="gas_used",
                timestamp="timestamp",
                transactions=[transaction],
                uncles=["uncle1"],
            ),
            transaction=transaction,
            transaction_receipt=transaction_receipt,
            token=token,
        )
        self.assertNotEqual(left, right)

    def test_different_transaction_receipts_is_not_equal(self):
        transaction = Transaction(
            hash="hash",
            block_hash="block_hash",
            block_number="0x01",
            from_="from_",
            gas="gas",
            gas_price="gas_price",
            input="input",
            nonce="nonce",
            to_="to_",
            transaction_index="0x00",
            value="value",
            v="v",
            r="r",
            s="s",
        )
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
            difficulty="difficulty",
            total_difficulty="total_difficulty",
            extra_data="extra_data",
            size="size",
            gas_limit="gas_limit",
            gas_used="gas_used",
            timestamp="timestamp",
            transactions=[transaction],
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
            transaction=transaction,
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
            transaction=transaction,
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
        transaction = Transaction(
            hash="hash",
            block_hash="block_hash",
            block_number="0x01",
            from_="from_",
            gas="gas",
            gas_price="gas_price",
            input="input",
            nonce="nonce",
            to_="to_",
            transaction_index="0x00",
            value="value",
            v="v",
            r="r",
            s="s",
        )
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
            difficulty="difficulty",
            total_difficulty="total_difficulty",
            extra_data="extra_data",
            size="size",
            gas_limit="gas_limit",
            gas_used="gas_used",
            timestamp="timestamp",
            transactions=[transaction],
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
            transaction=transaction,
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
            transaction=transaction,
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

    def test_different_metadatas_is_not_equal(self):
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
