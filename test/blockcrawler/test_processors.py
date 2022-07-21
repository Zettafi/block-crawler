import random
import unittest
from asyncio import QueueEmpty
from unittest.mock import AsyncMock, MagicMock

import ddt
from eth_abi import encode

from chainconductor.blockcrawler.processors import (
    BlockProcessor,
    BlockIDTransportObject,
    ContractTransportObject,
    TokenTransportObject,
    TransactionProcessor,
    TokenProcessor,
)
from chainconductor.web3.types import Block, Transaction, TransactionReceipt
from chainconductor.web3.util import ERC721Functions, ERC1155Functions


@ddt.ddt
class BlockProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_client = AsyncMock()
        self.__rpc_client.get_blocks.return_value = []
        self.__block_id_queue = (
            MagicMock()
        )  # Queue is asynchronous, but we don't use any async methods
        self.__block_id_queue.get_nowait.side_effect = [
            BlockIDTransportObject(0),
            QueueEmpty(),
        ]
        self.__transaction_queue = AsyncMock()
        self.__stats_service = MagicMock()
        self.__event_bus = AsyncMock()
        self.__processor = BlockProcessor(
            rpc_client=self.__rpc_client,
            block_id_queue=self.__block_id_queue,
            transaction_queue=self.__transaction_queue,
            event_bus=self.__event_bus,
            stats_service=self.__stats_service,
            rpc_batch_size=0,
            max_batch_wait=0,
        )

    async def test_triggers_stopped_event_in_event_bus(self):
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__event_bus.trigger.assert_called_once_with("block_processor_stopped")

    async def test_records_rpc_call_timer(self):
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__stats_service.timer.assert_called_once_with("rpc_get_blocks")
        self.__stats_service.timer.return_value.__enter__.assert_called_once()  # enter starts the timer
        self.__stats_service.timer.return_value.__exit__.assert_called_once()  # exit ends the timer and records

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
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.assertEqual(
            block_ids,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were blocks",
        )
        self.__stats_service.increment.assert_called_with("blocks_processed")

    async def test_does_not_add_items_to_transaction_queue_if_there_are_no_transactions(
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
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__transaction_queue.put.assert_not_called()

    async def test_does_not_adds_items_if_transaction_has_to_and_input_does_not_have_function(
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
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__transaction_queue.put.assert_not_called()

    async def test_adds_items_as_contract_dtos_to_transaction_queue_if_transaction_has_no_to(
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
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__transaction_queue.put.assert_called_once()
        self.assertIsInstance(
            self.__transaction_queue.put.call_args.args[0], ContractTransportObject
        )

    @ddt.data(
        ERC721Functions.SAFE_TRANSFER_FROM_WITH_DATA.function_hash + "otherstuff",
        ERC721Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA.function_hash + "otherstuff",
        ERC721Functions.TRANSFER_FROM.function_hash + "otherstuff",
        # Not handling ERC1155 for now
        # ERC1155Functions.SAFE_TRANSFER_FROM_WITH_DATA.function_hash + "otherstuff",
        # ERC1155Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA.function_hash + "otherstuff",
        # ERC1155Functions.SAFE_BATCH_TRANSFER_FROM.function_hash + "otherstuff",
    )
    async def test_adds_items_as_token_dtos_to_transaction_queue_if_token_transfer(
        self, input_value
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
                transactions=[
                    Transaction(
                        block_hash="",
                        block_number="",
                        from_="",
                        gas="",
                        gas_price="",
                        hash="",
                        input=input_value,
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
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__transaction_queue.put.assert_called_once()
        self.assertIsInstance(
            self.__transaction_queue.put.call_args.args[0], TokenTransportObject
        )


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
                logs_bloom="",
                root="",
                status="",
            )
        ]
        self.__transaction_queue = (
            MagicMock()
        )  # Queue is asynchronous, but we don't use any async methods
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
        self.__transaction_queue.get_nowait.side_effect = [
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
            ),
            QueueEmpty(),
        ]
        self.__contract_queue = AsyncMock()
        self.__token_queue = AsyncMock()
        self.__stats_service = MagicMock()
        self.__event_bus = AsyncMock()
        self.__processor = TransactionProcessor(
            rpc_client=self.__rpc_client,
            transaction_queue=self.__transaction_queue,
            contract_queue=self.__contract_queue,
            token_queue=self.__token_queue,
            event_bus=self.__event_bus,
            stats_service=self.__stats_service,
            rpc_batch_size=0,
            max_batch_wait=0,
        )

    async def test_triggers_stopped_event_in_event_bus(self):
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__event_bus.trigger.assert_called_once_with(
            "transaction_processor_stopped"
        )

    async def test_records_rpc_call_timer(self):
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__stats_service.timer.assert_called_once_with(
            "rpc_get_transaction_receipts"
        )
        self.__stats_service.timer.return_value.__enter__.assert_called_once()  # enter starts the timer
        self.__stats_service.timer.return_value.__exit__.assert_called_once()  # exit ends the timer and records

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
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
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
        self.__transaction_queue.get_nowait.side_effect = [cto, QueueEmpty()]
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__contract_queue.put.assert_called_once()

    async def test_puts_tokens_on_token_queue(self):
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
        self.__transaction_queue.get_nowait.side_effect = [tto, QueueEmpty()]

        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__token_queue.put.assert_called_once()


class AsyncContextManager:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@ddt.ddt
class TestTokenProcessor(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__token_queue = MagicMock()

        # At this point it's expected that a properly formatted input exists for the transaction
        encoded_params = encode(
            ERC721Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA.param_types,
            [
                "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
                "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
                1,
            ],
        ).hex()
        transaction_input = f"{ERC721Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA.function_hash}{encoded_params}"

        self.__token_queue.get_nowait.side_effect = [
            TokenTransportObject(
                transaction=Transaction(
                    block_hash="",
                    block_number="",
                    from_="",
                    gas="",
                    gas_price="",
                    hash="",
                    input=transaction_input,
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
        self.__dynamodb.Table.return_value.batch_writer = MagicMock(
            AsyncContextManager()
        )
        self.__stats_service = MagicMock()
        self.__event_bus = AsyncMock()
        self.__processor = TokenProcessor(
            dynamodb=self.__dynamodb,
            stats_service=self.__stats_service,
            event_bus=self.__event_bus,
            token_queue=self.__token_queue,
            dynamodb_batch_size=0,
            max_batch_wait=0,
        )

    async def test_increments_stats_for_each_token(self):
        tokens = random.randint(1, 99)
        token_queue_gets = list()
        encoded_params = encode(
            ERC721Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA.param_types,
            [
                "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
                "0x759a401A287FFaD0fA2dEae15Fe3b6169506d657",
                1,
            ],
        ).hex()
        transaction_input = f"{ERC721Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA.function_hash}{encoded_params}"
        for _ in range(tokens):
            token_queue_gets.append(
                TokenTransportObject(
                    transaction=Transaction(
                        block_hash="",
                        block_number="",
                        from_="",
                        gas="",
                        gas_price="",
                        hash="",
                        input=transaction_input,
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
                )
            )
        token_queue_gets.append(QueueEmpty())
        self.__token_queue.get_nowait.side_effect = token_queue_gets
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.assertEqual(
            tokens,
            self.__stats_service.increment.call_count,
            "Expected as many increment calls as there were transactions",
        )
        self.__stats_service.increment.assert_called_with("tokens_processed")

    async def test_triggers_stopped_event_in_event_bus(self):
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__event_bus.trigger.assert_called_once_with("token_processor_stopped")

    async def test_records_dynamodb_timer_in_stats(self):
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__stats_service.timer.assert_called_once_with(
            "dynamodb_write_token_transfer"
        )
        self.__stats_service.timer.return_value.__enter__.assert_called_once()  # enter starts the timer
        self.__stats_service.timer.return_value.__exit__.assert_called_once()  # exit ends the timer and records

    @ddt.data(
        # The proper parameters are from_address, to_address, token_id, and then any other parameters the function may have that are not used
        (
            ERC721Functions.SAFE_TRANSFER_FROM_WITH_DATA,
            (
                "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
                "0x759a401A287FFaD0fA2dEae15Fe3b6169506d657",
                1,
                b"data",
            ),
        ),
        (
            ERC721Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA,
            (
                "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
                "0x759a401A287FFaD0fA2dEae15Fe3b6169506d657",
                1,
            ),
        ),
        (
            ERC721Functions.TRANSFER_FROM,
            (
                "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
                "0x759a401A287FFaD0fA2dEae15Fe3b6169506d657",
                1,
            ),
        ),
    )
    @ddt.unpack
    async def test_stores_correct_data_for_erc721_contracts(self, function, parameters):
        encoded_params = encode(function.param_types, parameters).hex()
        transaction_input = f"{ERC721Functions.SAFE_TRANSFER_FROM_WITHOUT_DATA.function_hash}{encoded_params}"
        tto = TokenTransportObject(
            transaction=Transaction(
                block_hash="",
                block_number="",
                from_="wrong from",
                gas="",
                gas_price="",
                hash="",
                input=transaction_input,
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
                from_="",
                to_="to address",
                cumulative_gas_used="",
                gas_used="",
                contract_address="expected contract",
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
                timestamp="0x0f",
                transactions=list(),
                uncles=list(),
            ),
        )

        self.__token_queue.get_nowait.side_effect = [tto, QueueEmpty]
        await self.__processor.stop()  # If we don't tell the processor to stop before it starts, it will run forever
        await self.__processor.start()
        self.__dynamodb.Table.return_value.batch_writer.return_value.__aenter__.return_value.put_item.assert_called_once_with(
            Item={
                "blockchain": "Ethereum Mainnet",
                "transaction_hash": "transaction hash",
                "collection_id": "to address",
                "token_id": "1",
                "from": "0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d",
                "to": "0x759a401a287ffad0fa2deae15fe3b6169506d657",
                "timestamp": 15,
            }
        )
