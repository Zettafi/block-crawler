import asyncio
import unittest
from unittest import IsolatedAsyncioTestCase
from unittest import TestCase
from unittest.mock import patch, ANY, MagicMock

import aiohttp.client_exceptions
import ddt

from chainconductor.web3.rpc import EthCall, RPCClient, RPCError, RPCServerError
from chainconductor.web3.types import TransactionReceipt, Log, Block, Transaction, HexInt
from chainconductor.web3.util import (
    ERC721Functions,
    ERC165Functions,
    ERC721MetadataFunctions,
)


class EthCallTestCase(TestCase):
    def test_equal_attributes_are_equal(self):
        left = EthCall(
            identifier="identifier",
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            identifier="identifier",
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        self.assertEqual(left, right)

    def test_different_identifiers_are_not_equal(self):
        left = EthCall(
            identifier="identifier 1",
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            identifier="identifier 2",
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_from_are_not_equal(self):
        left = EthCall(
            identifier="identifier",
            from_="from 1",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            identifier="identifier",
            from_="from 2",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_tos_are_not_equal(self):
        left = EthCall(
            identifier="identifier",
            from_="from",
            to="to 1",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            identifier="identifier",
            from_="from",
            to="to 2",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_functions_are_not_equal(self):
        left = EthCall(
            identifier="identifier",
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            identifier="identifier",
            from_="from",
            to="to",
            function=ERC721Functions.IS_APPROVED_FOR_ALL,
            parameters=["parameter"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_parameters_are_not_equal(self):
        left = EthCall(
            identifier="identifier",
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter 1"],
            block="block",
        )
        right = EthCall(
            identifier="identifier",
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter 2"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_blocks_are_not_equal(self):
        left = EthCall(
            identifier="identifier",
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block 1",
        )
        right = EthCall(
            identifier="identifier",
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block 2",
        )
        self.assertNotEqual(left, right)


class BaseRPCClientTestCase(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        patcher = patch(
            "aiohttp.ClientSession.post",
        )
        self._post_patch = patcher.start()
        self.addCleanup(patcher.stop)

        # Make sure the `id` for each response matches with the index of the request made
        self._rpc_responses = [dict(jsonrpc="Mock", id=0, result="")]

        self._rpc_url = "PROVIDER_URL"
        self._rpc_client = RPCClient(self._rpc_url)
        self._set_post_patch_return_value(self._rpc_responses)

    def _get_request_json_from_post_patch(self) -> dict:
        # Get the call tuple for the first call to the post patch
        # The third item in the call tuple is the key word args
        # The json is in teh "json" key word arg
        self._post_patch.assert_called()
        json = self._post_patch.call_args.kwargs["json"]
        return json

    def _get_request_json_params_from_post_patch_call(self, index=0):
        return self._get_request_json_from_post_patch()[index]["params"][0]

    def _get_request_json_method_from_post_patch_call(self, index=0):
        return self._get_request_json_from_post_patch()[index]["method"]

    def _set_post_patch_return_value(self, response):
        self._post_patch.return_value.__aenter__.return_value.json.return_value = response


class TestRPCClient(BaseRPCClientTestCase):
    async def test_get_transactions_raise_RPCError_for_aio_client_error(
        self,
    ):
        self._post_patch.side_effect = aiohttp.client_exceptions.ClientError
        with self.assertRaises(RPCError):
            await self._rpc_client.get_transaction_receipts(["hash"])

    async def test_get_transactions_raise_RPCError_for_asyncio_timeout(
        self,
    ):
        self._post_patch.side_effect = asyncio.TimeoutError
        with self.assertRaises(RPCError):
            await self._rpc_client.get_transaction_receipts(["hash"])

    async def test_get_blocks_raise_RPCError_for_aio_client_error(
        self,
    ):
        self._post_patch.side_effect = aiohttp.client_exceptions.ClientError
        with self.assertRaises(RPCError):
            await self._rpc_client.get_blocks({1})

    async def test_get_blocks_raise_RPCError_for_asyncio_timeout(
        self,
    ):
        self._post_patch.side_effect = asyncio.TimeoutError
        with self.assertRaises(RPCError):
            await self._rpc_client.get_blocks(set())

    async def test_get_block_number_raises_RPCError_for_aio_client_error(
        self,
    ):
        self._post_patch.side_effect = aiohttp.client_exceptions.ClientError
        with self.assertRaises(RPCError):
            await self._rpc_client.get_block_number()

    async def test_get_block_number_raises_RPCError_for_asyncio_timeout(
        self,
    ):
        self._post_patch.side_effect = asyncio.TimeoutError
        with self.assertRaises(RPCError):
            await self._rpc_client.get_block_number()

    async def test_calls_raise_RPCError_for_aio_client_error(
        self,
    ):
        self._post_patch.side_effect = aiohttp.client_exceptions.ClientError
        with self.assertRaises(RPCError):
            await self._rpc_client.calls([MagicMock()])

    async def test_calls_raises_RPCError_for_asyncio_timeout(
        self,
    ):
        self._post_patch.side_effect = asyncio.TimeoutError
        with self.assertRaises(RPCError):
            await self._rpc_client.calls([MagicMock()])


class GetTransactionReceiptTestCase(BaseRPCClientTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._set_post_patch_return_value(
            [{"jsonrpc": "mock", "id": "di", "error": {"code": "edoc", "message": "egassem"}}]
        )

    async def test_get_transaction_receipts_sends_request_to_correct_endpoint(self):
        await self._rpc_client.get_transaction_receipts(["hash"])
        self._post_patch.assert_called_once_with(self._rpc_url, json=ANY)

    async def test_get_transaction_receipts_sends_all_requests(self):
        await self._rpc_client.get_transaction_receipts(["hash"])
        self._post_patch.assert_called_once_with(
            ANY,
            json=[
                {
                    "jsonrpc": "2.0",
                    "method": "eth_getTransactionReceipt",
                    "params": ("hash",),
                    "id": 0,
                }
            ],
        )

    async def test_get_transaction_receipts_provides_all_responses(self):
        self._set_post_patch_return_value(
            [
                {
                    "id": 1,
                    "jsonrpc": "2.0",
                    "result": {
                        "transactionHash": "0xb903239f8543d04b5dc1ba6579132b143087c68db1b2168786408fcbce568238",  # noqa: E501
                        # noqa: E501
                        "transactionIndex": "0x1",
                        "blockNumber": "0xb",
                        "blockHash": "0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b",  # noqa: E501
                        # noqa: E501
                        "cumulativeGasUsed": "0x33bc",
                        "gasUsed": "0x4dc",
                        "contractAddress": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                        "logs": [
                            {
                                "logIndex": "0x1",
                                "blockNumber": "0x1b4",
                                "blockHash": "0x8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcfdf829c5a142f1fccd7d",  # noqa: E501
                                # noqa: E501
                                "transactionHash": "0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcf",  # noqa: E501
                                # noqa: E501
                                "transactionIndex": "0x0",
                                "address": "0x16c5785ac562ff41e2dcfdf829c5a142f1fccd7d",
                                "data": "0x0000000000000000000000000000000000000000000000000000000000000000",  # noqa: E501
                                # noqa: E501
                                "topics": [
                                    "0x59ebeb90bc63057b6515673c3ecf9438e5058bca0f92585014eced636878c9a5"  # noqa: E501
                                    # noqa: E501
                                ],
                                "removed": False,
                            },
                            {
                                "logIndex": "0x2",
                                "blockNumber": "0x1b5",
                                "blockHash": "0x8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcfdf829c5a142f1fccd8e",  # noqa: E501
                                # noqa: E501
                                "transactionHash": "0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2ed0",  # noqa: E501
                                # noqa: E501
                                "transactionIndex": "0x1",
                                "address": None,
                                "data": "0x0000000000000000000000000000000000000000000000000000000000000001",  # noqa: E501
                                # noqa: E501
                                "topics": [
                                    "0x59ebeb90b6d4057b6515673c3ecf9438e5058bcae8c2585014eced636878c9a5"  # noqa: E501
                                    # noqa: E501
                                ],
                                "removed": True,
                            },
                        ],
                        "logsBloom": "0x0011223344",
                        "status": "0x1",
                        "from": "0x00112233",
                        "to": "0x44556677",
                        "root": "0x998877",
                    },
                }
            ]
        )

        expected = [
            TransactionReceipt(
                transaction_hash="0xb903239f8543d04b5dc1ba6579132b143087c68db1b2168786408fcbce568238",  # noqa: E501
                # noqa: E501
                transaction_index="0x1",
                block_number="0xb",
                block_hash="0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b",
                cumulative_gas_used="0x33bc",
                gas_used="0x4dc",
                contract_address="0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                logs=[
                    Log(
                        log_index="0x1",
                        block_number="0x1b4",
                        block_hash="0x8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcfdf829c5a142f1fccd7d",  # noqa: E501
                        # noqa: E501
                        transaction_hash="0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcf",  # noqa: E501
                        # noqa: E501
                        transaction_index="0x0",
                        address="0x16c5785ac562ff41e2dcfdf829c5a142f1fccd7d",
                        data="0x0000000000000000000000000000000000000000000000000000000000000000",
                        topics=[
                            "0x59ebeb90bc63057b6515673c3ecf9438e5058bca0f92585014eced636878c9a5"
                        ],
                        removed=False,
                    ),
                    Log(
                        log_index="0x2",
                        block_number="0x1b5",
                        block_hash="0x8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcfdf829c5a142f1fccd8e",  # noqa: E501
                        # noqa: E501
                        transaction_hash="0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2ed0",  # noqa: E501
                        # noqa: E501
                        transaction_index="0x1",
                        address=None,
                        data="0x0000000000000000000000000000000000000000000000000000000000000001",
                        topics=[
                            "0x59ebeb90b6d4057b6515673c3ecf9438e5058bcae8c2585014eced636878c9a5"
                        ],
                        removed=True,
                    ),
                ],
                logs_bloom="0x0011223344",
                status="0x1",
                from_="0x00112233",
                to_="0x44556677",
                root="0x998877",
            )
        ]
        actual = await self._rpc_client.get_transaction_receipts(["hash"])
        self.assertEqual(expected, actual)


class CallsTestCase(BaseRPCClientTestCase):
    async def test_eth_call_is_sent_as_method(self):
        calls = [EthCall("someid", "from", "to", ERC721Functions.IS_APPROVED_FOR_ALL)]
        await self._rpc_client.calls(calls)
        actual = self._get_request_json_method_from_post_patch_call()
        self.assertEqual("eth_call", actual)

    async def test_no_params_sends_expected(self):
        calls = [EthCall("someid", "from", "to", ERC721Functions.IS_APPROVED_FOR_ALL)]
        await self._rpc_client.calls(calls)
        actual = self._get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(ERC721Functions.IS_APPROVED_FOR_ALL.function_hash, actual)

    async def test_params_sends_expected(self):
        calls = [EthCall("someid", "from", "to", ERC721Functions.OWNER_OF_TOKEN, [1])]
        await self._rpc_client.calls(calls)
        actual = self._get_request_json_params_from_post_patch_call()["data"]
        expected = (
            ERC721Functions.OWNER_OF_TOKEN.function_hash
            + "0000000000000000000000000000000000000000000000000000000000000001"
        )
        self.assertEqual(expected, actual)

    async def test_decodes_false_response(self):
        expected = {"someid": (False,)}
        self._rpc_responses[0][
            "result"
        ] = "0x0000000000000000000000000000000000000000000000000000000000000000"
        calls = [EthCall("someid", "from", "to", ERC165Functions.SUPPORTS_INTERFACE)]
        actual = await self._rpc_client.calls(calls)
        self.assertEqual(expected, actual)

    async def test_decode_BAYC_name_response(self):
        expected = {"someid": ("BoredApeYachtClub",)}
        self._rpc_responses[0][
            "result"
        ] = "0x00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000011426f7265644170655961636874436c7562000000000000000000000000000000"  # noqa: E501
        calls = [EthCall("someid", "from", "to", ERC721MetadataFunctions.NAME)]
        actual = await self._rpc_client.calls(calls)
        self.assertEqual(expected, actual)


@ddt.ddt
class GetBlocksTestCase(BaseRPCClientTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._set_post_patch_return_value([])

    async def test_get_transaction_receipts_sends_request_to_correct_endpoint(self):
        await self._rpc_client.get_blocks({1, 2, 3})
        self._post_patch.assert_called_once_with(self._rpc_url, json=ANY)

    async def test_get_blocks_sends_all_requests_and_defaults_full_transactions_to_false(self):
        await self._rpc_client.get_blocks({1, 2})
        self._post_patch.assert_called_once_with(
            ANY,
            json=[
                {
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": ("0x1", False),
                    "id": 0,
                },
                {
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": ("0x2", False),
                    "id": 1,
                },
            ],
        )

    @ddt.data([True, False])
    async def test_get_blocks_passes_full_transactions_value(self, full_transactions: bool):
        await self._rpc_client.get_blocks({1}, full_transactions)
        self._post_patch.assert_called_once_with(
            ANY,
            json=[
                {
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": ("0x1", full_transactions),
                    "id": 0,
                },
            ],
        )

    async def test_get_blocks_parses_eresponse_into_block_entities_without_full_txs(self):
        self._set_post_patch_return_value(
            [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "difficulty": "0x4ea3f27bc",
                        "extraData": "0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32",  # noqa: E501
                        "gasLimit": "0x1388",
                        "gasUsed": "0x0",
                        "hash": "0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae",  # noqa: E501
                        "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",  # noqa: E501
                        "miner": "0xbb7b8287f3f0a933474a79eae42cbca977791171",
                        "mixHash": "0x4fffe9ae21f1c9e15207b1f472d5bbdd68c9595d461666602f2be20daf5e7843",  # noqa: E501
                        "nonce": "0x689056015818adbe",
                        "number": "0x1b4",
                        "parentHash": "0xe99e022112df268087ea7eafaf4790497fd21dbeeb6bd7a1721df161a6657a54",  # noqa: E501
                        "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",  # noqa: E501
                        "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",  # noqa: E501
                        "size": "0x220",
                        "stateRoot": "0xddc8b0234c2e0cad087c8b389aa7ef01f7d79b2570bccb77ce48648aa61c904d",  # noqa: E501
                        "timestamp": "0x55ba467c",
                        "totalDifficulty": "0x78ed983323d",
                        "transactions": ["tx hash 1", "tx hash 2"],
                        "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",  # noqa: E501
                        "uncles": ["uncle hash 1", "uncle hash 2"],
                    },
                }
            ]
        )
        expected = [
            Block(
                difficulty="0x4ea3f27bc",
                extra_data="0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32",
                gas_limit="0x1388",
                gas_used="0x0",
                hash="0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae",
                logs_bloom="0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",  # noqa: E501
                miner="0xbb7b8287f3f0a933474a79eae42cbca977791171",
                mix_hash="0x4fffe9ae21f1c9e15207b1f472d5bbdd68c9595d461666602f2be20daf5e7843",
                nonce="0x689056015818adbe",
                number="0x1b4",
                parent_hash="0xe99e022112df268087ea7eafaf4790497fd21dbeeb6bd7a1721df161a6657a54",
                receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                size="0x220",
                state_root="0xddc8b0234c2e0cad087c8b389aa7ef01f7d79b2570bccb77ce48648aa61c904d",
                timestamp="0x55ba467c",
                total_difficulty="0x78ed983323d",
                transactions=["tx hash 1", "tx hash 2"],
                transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",  # noqa: E501
                uncles=["uncle hash 1", "uncle hash 2"],
            )
        ]
        actual = await self._rpc_client.get_blocks({1})
        self.assertEqual(expected, actual)

    async def test_get_blocks_parses_response_into_block_entities_with_full_txs(self):
        self._set_post_patch_return_value(
            [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {
                        "difficulty": "0x4ea3f27bc",
                        "extraData": "0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32",  # noqa: E501
                        "gasLimit": "0x1388",
                        "gasUsed": "0x0",
                        "hash": "0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae",  # noqa: E501
                        "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",  # noqa: E501
                        "miner": "0xbb7b8287f3f0a933474a79eae42cbca977791171",
                        "mixHash": "0x4fffe9ae21f1c9e15207b1f472d5bbdd68c9595d461666602f2be20daf5e7843",  # noqa: E501
                        "nonce": "0x689056015818adbe",
                        "number": "0x1b4",
                        "parentHash": "0xe99e022112df268087ea7eafaf4790497fd21dbeeb6bd7a1721df161a6657a54",  # noqa: E501
                        "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",  # noqa: E501
                        "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",  # noqa: E501
                        "size": "0x220",
                        "stateRoot": "0xddc8b0234c2e0cad087c8b389aa7ef01f7d79b2570bccb77ce48648aa61c904d",  # noqa: E501
                        "timestamp": "0x55ba467c",
                        "totalDifficulty": "0x78ed983323d",
                        "transactions": [
                            {
                                "blockHash": "0x1d59ff54b1eb26b013ce3cb5fc9dab3705b415a67127a003c3e61eb445bb8df2",  # noqa: E501
                                "blockNumber": "0x5daf3b",
                                "from": "0xa7d9ddbe1f17865597fbd27ec712455208b6b76d",
                                "gas": "0xc350",
                                "gasPrice": "0x4a817c800",
                                "hash": "0x88df016429689c079f3b2f6ad39fa052532c56795b733da78a91ebe6a713944b",  # noqa: E501
                                "input": "0x68656c6c6f21",
                                "nonce": "0x15",
                                "to": "0xf02c1c8e6114b1dbe8937a39260b5b0a374432bb",
                                "transactionIndex": "0x41",
                                "value": "0xf3dbb76162000",
                                "v": "0x25",
                                "r": "0x1b5e176d927f8e9ab405058b2d2457392da3e20f328b16ddabcebc33eaac5fea",  # noqa: E501
                                "s": "0x4ba69724e8f69de52f0125ad8b3c5c2cef33019bac3249e2c0a2192766d1721c",  # noqa: E501
                            }
                        ],
                        "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",  # noqa: E501
                        "uncles": ["uncle hash 1", "uncle hash 2"],
                    },
                }
            ]
        )
        expected = [
            Block(
                difficulty="0x4ea3f27bc",
                extra_data="0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32",
                gas_limit="0x1388",
                gas_used="0x0",
                hash="0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae",
                logs_bloom="0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",  # noqa: E501
                miner="0xbb7b8287f3f0a933474a79eae42cbca977791171",
                mix_hash="0x4fffe9ae21f1c9e15207b1f472d5bbdd68c9595d461666602f2be20daf5e7843",
                nonce="0x689056015818adbe",
                number="0x1b4",
                parent_hash="0xe99e022112df268087ea7eafaf4790497fd21dbeeb6bd7a1721df161a6657a54",
                receipts_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                sha3_uncles="0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                size="0x220",
                state_root="0xddc8b0234c2e0cad087c8b389aa7ef01f7d79b2570bccb77ce48648aa61c904d",
                timestamp="0x55ba467c",
                total_difficulty="0x78ed983323d",
                transactions=[
                    Transaction(
                        block_hash="0x1d59ff54b1eb26b013ce3cb5fc9dab3705b415a67127a003c3e61eb445bb8df2",  # noqa: E501
                        block_number="0x5daf3b",
                        from_="0xa7d9ddbe1f17865597fbd27ec712455208b6b76d",
                        gas="0xc350",
                        gas_price="0x4a817c800",
                        hash="0x88df016429689c079f3b2f6ad39fa052532c56795b733da78a91ebe6a713944b",
                        input="0x68656c6c6f21",
                        nonce="0x15",
                        to_="0xf02c1c8e6114b1dbe8937a39260b5b0a374432bb",
                        transaction_index="0x41",
                        value="0xf3dbb76162000",
                        v="0x25",
                        r="0x1b5e176d927f8e9ab405058b2d2457392da3e20f328b16ddabcebc33eaac5fea",
                        s="0x4ba69724e8f69de52f0125ad8b3c5c2cef33019bac3249e2c0a2192766d1721c",
                    )
                ],
                transactions_root="0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",  # noqa: E501
                uncles=["uncle hash 1", "uncle hash 2"],
            )
        ]
        actual = await self._rpc_client.get_blocks({1}, True)
        self.assertEqual(expected, actual)


class GetBlockNumberTestCase(BaseRPCClientTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._set_post_patch_return_value({"jsonrpc": "2.0", "id": 1, "result": "0x0"})

    async def test_get_block_number_sends_request_to_correct_endpoint(self):
        await self._rpc_client.get_block_number()
        self._post_patch.assert_called_once_with(self._rpc_url, json=ANY)

    async def test_get_block_number_sends_proper_json(self):
        await self._rpc_client.get_block_number()
        self._post_patch.assert_called_once_with(
            ANY,
            json={
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": tuple(),
                "id": 0,
            },
        )

    async def test_get_block_number_returns_expected_value(self):
        expected = HexInt("0x0")
        actual = await self._rpc_client.get_block_number()
        self.assertEqual(expected, actual)

    async def test_get_block_number_raises_exception_for_rpc_error(self):
        self._set_post_patch_return_value(
            {"jsonrpc": "rpc", "id": "id", "error": {"code": "code", "message": "message"}}
        )
        expected = "RPC rpc - Req id - code: message"
        with self.assertRaisesRegex(RPCServerError, expected):
            await self._rpc_client.get_block_number()


if __name__ == "__main__":
    unittest.main()
