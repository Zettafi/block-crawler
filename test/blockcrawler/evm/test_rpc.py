import asyncio
import unittest
from typing import Dict, Any
from unittest import IsolatedAsyncioTestCase
from unittest import TestCase
from unittest.mock import patch, ANY, MagicMock, AsyncMock, call

import aiohttp.client_exceptions
import ddt
from hexbytes import HexBytes

from blockcrawler.core.rpc import RpcError, RpcServerError
from blockcrawler.core.stats import StatsService
from blockcrawler.evm.rpc import (
    EthCall,
    EvmRpcClient,
    ConnectionPoolingEvmRpcClient,
)
from blockcrawler.evm.types import (
    EvmTransactionReceipt,
    EvmLog,
    EvmBlock,
    HexInt,
    Address,
    EvmTransaction,
)
from blockcrawler.evm.util import (
    Erc721Functions,
    Erc165Functions,
    Erc721MetadataFunctions,
)


class EthCallTestCase(TestCase):
    def test_equal_attributes_are_equal(self):
        left = EthCall(
            from_="from",
            to="to",
            function=Erc721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            from_="from",
            to="to",
            function=Erc721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        self.assertEqual(left, right)

    def test_different_from_are_not_equal(self):
        left = EthCall(
            from_="from 1",
            to="to",
            function=Erc721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            from_="from 2",
            to="to",
            function=Erc721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_tos_are_not_equal(self):
        left = EthCall(
            from_="from",
            to="to 1",
            function=Erc721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            from_="from",
            to="to 2",
            function=Erc721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_functions_are_not_equal(self):
        left = EthCall(
            from_="from",
            to="to",
            function=Erc721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            from_="from",
            to="to",
            function=Erc721Functions.IS_APPROVED_FOR_ALL,
            parameters=["parameter"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_parameters_are_not_equal(self):
        left = EthCall(
            from_="from",
            to="to",
            function=Erc721Functions.TRANSFER_FROM,
            parameters=["parameter 1"],
            block="block",
        )
        right = EthCall(
            from_="from",
            to="to",
            function=Erc721Functions.TRANSFER_FROM,
            parameters=["parameter 2"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_blocks_are_not_equal(self):
        left = EthCall(
            from_="from",
            to="to",
            function=Erc721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block 1",
        )
        right = EthCall(
            from_="from",
            to="to",
            function=Erc721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block 2",
        )
        self.assertNotEqual(left, right)


class BaseRPCClientTestCase(IsolatedAsyncioTestCase):
    async def send_json_patch(self, rpc_json: dict):
        self._rpc_response_id = rpc_json["id"]

    async def receive_json_patch(self):
        while not self._rpc_response_id:
            await asyncio.sleep(0)
        self._rpc_response["id"] = self._rpc_response_id
        self._rpc_response_id = None
        return self._rpc_response

    async def asyncSetUp(self) -> None:
        patcher = patch("aiohttp.ClientSession.ws_connect", AsyncMock())
        self._ws_connect_patch = patcher.start()
        self.addCleanup(patcher.stop)
        self._ws_connect_patch.return_value.receive_json.side_effect = self.receive_json_patch
        self._ws_connect_patch.return_value.send_json.side_effect = self.send_json_patch
        self._ws_connect_patch.return_value.closed = False
        self._rpc_response_id = None

        # Make sure the `id` for each response matches with the index of the request made
        self._rpc_response: Dict[str, Any] = dict(result=None)
        self._stats_service = MagicMock(StatsService)
        self._rpc_url = "PROVIDER_URL"
        self._rpc_client = EvmRpcClient(self._rpc_url, self._stats_service)
        await self._rpc_client.__aenter__()
        self.addAsyncCleanup(self._rpc_client.__aexit__, None, None, None)

    async def asyncTearDown(self) -> None:
        self._ws_connect_patch.return_value.closed = True

    def _get_request_json_from_ws_connect_patch(self) -> dict:
        # Get the call tuple for the first call to the post patch
        # The third item in the call tuple is the key word args
        # The json is in teh "json" key word arg
        self._ws_connect_patch.assert_called()
        return self._ws_connect_patch.return_value.send_json.call_args.args[0]

    def _get_request_json_params_from_post_patch_call(self):
        return self._get_request_json_from_ws_connect_patch()["params"][0]

    def _get_request_json_method_from_post_patch_call(self):
        return self._get_request_json_from_ws_connect_patch()["method"]


class TestRPCClient(BaseRPCClientTestCase):
    async def test_get_transactions_raise_RPCError_for_aio_client_error(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = (
            aiohttp.client_exceptions.ClientError
        )
        with self.assertRaises(RpcError):
            await self._rpc_client.get_transaction_receipt(HexBytes(b"hash"))

    async def test_get_transactions_raise_RPCError_for_asyncio_timeout(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = asyncio.TimeoutError
        with self.assertRaises(RpcError):
            await self._rpc_client.get_transaction_receipt(HexBytes(b"hash"))

    async def test_get_blocks_raise_RPCError_for_aio_client_error(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = (
            aiohttp.client_exceptions.ClientError
        )
        with self.assertRaises(RpcError):
            await self._rpc_client.get_block(HexInt(1))

    async def test_get_blocks_raise_RPCError_for_asyncio_timeout(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = asyncio.TimeoutError
        with self.assertRaises(RpcError):
            await self._rpc_client.get_block(HexInt(1))

    async def test_get_block_number_raises_RPCError_for_aio_client_error(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = (
            aiohttp.client_exceptions.ClientError
        )
        with self.assertRaises(RpcError):
            await self._rpc_client.get_block_number()

    async def test_get_block_number_raises_RPCError_for_asyncio_timeout(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = asyncio.TimeoutError
        with self.assertRaises(RpcError):
            await self._rpc_client.get_block_number()

    async def test_calls_raise_RPCError_for_aio_client_error(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = (
            aiohttp.client_exceptions.ClientError
        )
        with self.assertRaises(RpcError):
            await self._rpc_client.call(MagicMock())

    async def test_calls_raises_RPCError_for_asyncio_timeout(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = asyncio.TimeoutError
        with self.assertRaises(RpcError):
            await self._rpc_client.call(MagicMock())


class GetTransactionReceiptTestCase(BaseRPCClientTestCase):
    # noinspection SpellCheckingInspection
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        # noinspection LongLine
        self._rpc_response = {
            "jsonrpc": "2.0",
            "result": {
                "transactionHash": "0xb903239f8543d04b5dc1ba6579132b143087c68db1b2168786408fcbce568238",  # noqa: E501
                # noqa: E501
                # noqa: E501
                "transactionIndex": "0x1",
                "blockNumber": "0xb",
                "blockHash": "0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b",
                # noqa: E501
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
                        # noqa: E501
                        "transactionHash": "0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcf",  # noqa: E501
                        # noqa: E501
                        # noqa: E501
                        "transactionIndex": "0x0",
                        "address": "0x16c5785ac562ff41e2dcfdf829c5a142f1fccd7d",
                        "data": "0x0000000000000000000000000000000000000000000000000000000000000000",  # noqa: E501
                        # noqa: E501
                        # noqa: E501
                        "topics": [
                            "0x59ebeb90bc63057b6515673c3ecf9438e5058bca0f92585014eced636878c9a5"
                            # noqa: E501
                            # noqa: E501
                        ],
                        "removed": False,
                    },
                    {
                        "logIndex": "0x2",
                        "blockNumber": "0x1b5",
                        "blockHash": "0x8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcfdf829c5a142f1fccd8e",  # noqa: E501
                        # noqa: E501
                        # noqa: E501
                        "transactionHash": "0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2ed0",  # noqa: E501
                        # noqa: E501
                        # noqa: E501
                        "transactionIndex": "0x1",
                        "address": None,
                        "data": "0x0000000000000000000000000000000000000000000000000000000000000001",  # noqa: E501
                        # noqa: E501
                        # noqa: E501
                        "topics": [
                            "0x59ebeb90b6d4057b6515673c3ecf9438e5058bcae8c2585014eced636878c9a5"
                            # noqa: E501
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

    async def test_get_transaction_receipts_sends_all_requests(self):
        try:
            await self._rpc_client.get_transaction_receipt(HexBytes("0x1234"))
        except RpcError:
            pass
        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": "2.0",
                "method": "eth_getTransactionReceipt",
                "params": ("0x1234",),
                "id": ANY,
            }
        )

    # noinspection SpellCheckingInspection
    async def test_get_transaction_receipt_provides_all_responses(self):
        expected = EvmTransactionReceipt(
            transaction_hash=HexBytes(
                "0xb903239f8543d04b5dc1ba6579132b143087c68db1b2168786408fcbce568238"
            ),  # noqa: E501
            # noqa: E501
            transaction_index=HexInt("0x1"),
            block_number=HexInt("0xb"),
            block_hash=HexBytes(
                "0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b"
            ),
            cumulative_gas_used=HexInt("0x33bc"),
            gas_used=HexInt("0x4dc"),
            contract_address=Address("0xb60e8dd61c5d32be8058bb8eb970870f07233155"),
            logs=[
                EvmLog(
                    log_index=HexInt("0x1"),
                    block_number=HexInt("0x1b4"),
                    block_hash=HexBytes(
                        "0x8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcfdf829c5a142f1fccd7d"
                    ),  # noqa: E501
                    # noqa: E501
                    transaction_hash=HexBytes(
                        "0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcf"
                    ),  # noqa: E501
                    # noqa: E501
                    transaction_index=HexInt("0x0"),
                    address=Address("0x16c5785ac562ff41e2dcfdf829c5a142f1fccd7d"),
                    data=HexBytes(
                        "0x0000000000000000000000000000000000000000000000000000000000000000"
                    ),
                    topics=[
                        HexBytes(
                            "0x59ebeb90bc63057b6515673c3ecf9438e5058bca0f92585014eced636878c9a5"
                        )
                    ],
                    removed=False,
                ),
                EvmLog(
                    log_index=HexInt("0x2"),
                    block_number=HexInt("0x1b5"),
                    block_hash=HexBytes(
                        "0x8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcfdf829c5a142f1fccd8e"
                    ),
                    transaction_hash=HexBytes(
                        "0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2ed0"
                    ),  # noqa: E501
                    # noqa: E501
                    transaction_index=HexInt("0x1"),
                    address=None,
                    data=HexBytes(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    topics=[
                        HexBytes(
                            "0x59ebeb90b6d4057b6515673c3ecf9438e5058bcae8c2585014eced636878c9a5"
                        ),
                    ],
                    removed=True,
                ),
            ],
            logs_bloom=HexInt("0x0011223344"),
            status=HexInt("0x1"),
            from_=Address("0x00112233"),
            to_=Address("0x44556677"),
            root=HexBytes("0x998877"),
        )
        actual = await self._rpc_client.get_transaction_receipt(HexBytes(b"hash"))
        self.assertEqual(expected, actual)


class CallTestCase(BaseRPCClientTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self._rpc_response = dict(result="0x")

    async def test_eth_call_is_sent_as_method(self):
        eth_call = EthCall("from", "to", Erc721MetadataFunctions.NAME)
        await self._rpc_client.call(eth_call)
        actual = self._get_request_json_method_from_post_patch_call()
        self.assertEqual("eth_call", actual)

    async def test_no_params_sends_expected(self):
        eth_call = EthCall("from", "to", Erc721MetadataFunctions.NAME)
        await self._rpc_client.call(eth_call)
        actual = self._get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(Erc721MetadataFunctions.NAME.function_signature_hash.hex(), actual)

    async def test_params_sends_expected(self):
        eth_call = EthCall("from", "to", Erc721Functions.OWNER_OF_TOKEN, [1])
        await self._rpc_client.call(eth_call)
        actual = self._get_request_json_params_from_post_patch_call()["data"]
        expected = (
            Erc721Functions.OWNER_OF_TOKEN.function_signature_hash.hex()
            + "0000000000000000000000000000000000000000000000000000000000000001"
        )
        self.assertEqual(expected, actual)

    async def test_decodes_false_response(self):
        expected = (False,)
        self._rpc_response = dict(
            result="0x0000000000000000000000000000000000000000000000000000000000000000"
        )
        eth_call = EthCall("from", "to", Erc165Functions.SUPPORTS_INTERFACE)
        actual = await self._rpc_client.call(eth_call)
        self.assertEqual(expected, actual)

    async def test_decode_BAYC_name_response(self):
        expected = ("BoredApeYachtClub",)
        # noinspection LongLine
        self._rpc_response = dict(
            result="0x00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000011426f7265644170655961636874436c7562000000000000000000000000000000"  # noqa: E501
        )  # noqa: E501
        eth_call = EthCall("from", "to", Erc721MetadataFunctions.NAME)
        actual = await self._rpc_client.call(eth_call)
        self.assertEqual(expected, actual)

    async def test_decode_null_returns_null(self):
        self._rpc_response = dict(result="0x")
        eth_call = EthCall("from", "to", Erc721MetadataFunctions.NAME)
        actual = await self._rpc_client.call(eth_call)
        self.assertEqual((None,), actual)


@ddt.ddt
class GetBlockTestCase(BaseRPCClientTestCase):
    # noinspection SpellCheckingInspection
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        # noinspection LongLine
        self._rpc_response["result"] = {
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
            "transactions": ["0x1111", "0x1112"],
            "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",  # noqa: E501
            "uncles": ["0x90", "0x91"],
        }

        self._transactions = [
            {
                "blockHash": "0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae",
                "blockNumber": "0x1b4",
                "from": "0xa7d9ddbe1f17865597fbd27ec712455208b6b76d",
                "gas": "0xc350",
                "gasPrice": "0x4a817c800",
                "hash": "0x1111",
                "input": "0x68656c6c6f21",
                "nonce": "0x15",
                "to": "0xf02c1c8e6114b1dbe8937a39260b5b0a374432bb",
                "transactionIndex": "0x41",
                "value": "0xf3dbb76162000",
                "v": "0x25",
                "r": "0x1b5e176d927f8e9ab405058b2d2457392da3e20f328b16ddabcebc33eaac5fea",
                "s": "0x4ba69724e8f69de52f0125ad8b3c5c2cef33019bac3249e2c0a2192766d1721c",
            },
            {
                "blockHash": "0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae",
                "blockNumber": "0x1b4",
                "from": "0xa7d9ddbe1f17865597fbd27ec712455208b6b76e",
                "gas": "0xc351",
                "gasPrice": "0x4a817c801",
                "hash": "0x1112",
                "input": "0x68656c6c6f22",
                "nonce": "0x16",
                "to": "0xf02c1c8e6114b1dbe8937a39260b5b0a374432bc",
                "transactionIndex": "0x42",
                "value": "0xf3dbb76162001",
                "v": "0x26",
                "r": "0x1b5e176d927f8e9ab405058b2d2457392da3e20f328b16ddabcebc33eaac5feb",
                "s": "0x4ba69724e8f69de52f0125ad8b3c5c2cef33019bac3249e2c0a2192766d1721d",
            },
        ]

    async def test_get_block_sends_requests_with_full_transactions_false_when_method_is_false(self):
        await self._rpc_client.get_block(HexInt(1), False)
        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": ("0x1", False),
                "id": ANY,
            },
        )

    async def test_get_block_sends_requests_with_full_transactions_true_when_method_is_true(self):
        self._rpc_response["result"]["transactions"] = self._transactions
        await self._rpc_client.get_block(HexInt(1), True)
        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": ("0x1", True),
                "id": ANY,
            },
        )

    async def test_get_block_sends_requests_with_full_transactions_defaulting_to_false(self):
        await self._rpc_client.get_block(HexInt(1))
        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": ("0x1", False),
                "id": ANY,
            },
        )

    # noinspection SpellCheckingInspection
    async def test_get_block_without_full_transactions_parses_response_into_block_entity(self):
        # noinspection LongLine
        expected = EvmBlock(
            difficulty=HexInt("0x4ea3f27bc"),
            extra_data=HexBytes("0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32"),
            gas_limit=HexInt("0x1388"),
            gas_used=HexInt("0x0"),
            hash=HexBytes("0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae"),
            logs_bloom=HexInt(
                "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"  # noqa: E501
            ),  # noqa: E501
            miner=Address("0xbb7b8287f3f0a933474a79eae42cbca977791171"),
            mix_hash=HexBytes("0x4fffe9ae21f1c9e15207b1f472d5bbdd68c9595d461666602f2be20daf5e7843"),
            nonce=HexBytes("0x689056015818adbe"),
            number=HexInt("0x1b4"),
            parent_hash=HexBytes(
                "0xe99e022112df268087ea7eafaf4790497fd21dbeeb6bd7a1721df161a6657a54"
            ),
            receipts_root=HexBytes(
                "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
            ),
            sha3_uncles=HexBytes(
                "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
            ),
            size=HexInt("0x220"),
            state_root=HexBytes(
                "0xddc8b0234c2e0cad087c8b389aa7ef01f7d79b2570bccb77ce48648aa61c904d"
            ),
            timestamp=HexInt("0x55ba467c"),
            total_difficulty=HexInt("0x78ed983323d"),
            transaction_hashes=[HexBytes("0x1111"), HexBytes("0x1112")],
            transactions=None,
            transactions_root=HexBytes(
                "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
            ),  # noqa: E501
            uncles=[HexBytes("0x90"), HexBytes("0x91")],
        )
        actual = await self._rpc_client.get_block(HexInt(1), False)
        self.assertEqual(expected, actual)

    # noinspection SpellCheckingInspection
    async def test_get_block_with_full_transactions_parses_response_into_block_entity(self):
        self._rpc_response["result"]["transactions"] = self._transactions
        # noinspection LongLine
        expected = EvmBlock(
            difficulty=HexInt("0x4ea3f27bc"),
            extra_data=HexBytes("0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32"),
            gas_limit=HexInt("0x1388"),
            gas_used=HexInt("0x0"),
            hash=HexBytes("0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae"),
            logs_bloom=HexInt(
                "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"  # noqa: E501
            ),  # noqa: E501
            miner=Address("0xbb7b8287f3f0a933474a79eae42cbca977791171"),
            mix_hash=HexBytes("0x4fffe9ae21f1c9e15207b1f472d5bbdd68c9595d461666602f2be20daf5e7843"),
            nonce=HexBytes("0x689056015818adbe"),
            number=HexInt("0x1b4"),
            parent_hash=HexBytes(
                "0xe99e022112df268087ea7eafaf4790497fd21dbeeb6bd7a1721df161a6657a54"
            ),
            receipts_root=HexBytes(
                "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
            ),
            sha3_uncles=HexBytes(
                "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"
            ),
            size=HexInt("0x220"),
            state_root=HexBytes(
                "0xddc8b0234c2e0cad087c8b389aa7ef01f7d79b2570bccb77ce48648aa61c904d"
            ),
            timestamp=HexInt("0x55ba467c"),
            total_difficulty=HexInt("0x78ed983323d"),
            transaction_hashes=[HexBytes("0x1111"), HexBytes("0x1112")],
            transactions=[
                EvmTransaction(
                    block_hash=HexBytes(
                        "0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae"
                    ),
                    block_number=HexInt("0x1b4"),
                    from_=Address("0xa7d9ddbe1f17865597fbd27ec712455208b6b76d"),
                    gas=HexInt("0xc350"),
                    gas_price=HexInt("0x4a817c800"),
                    hash=HexBytes("0x1111"),
                    input=HexBytes("0x68656c6c6f21"),
                    nonce=HexInt("0x15"),
                    transaction_index=HexInt("0x41"),
                    v=HexInt("0x25"),
                    r=HexBytes(
                        "0x1b5e176d927f8e9ab405058b2d2457392da3e20f328b16ddabcebc33eaac5fea"
                    ),
                    s=HexBytes(
                        "0x4ba69724e8f69de52f0125ad8b3c5c2cef33019bac3249e2c0a2192766d1721c"
                    ),
                    to_=Address("0xf02c1c8e6114b1dbe8937a39260b5b0a374432bb"),
                    value=HexInt("0xf3dbb76162000"),
                ),
                EvmTransaction(
                    block_hash=HexBytes(
                        "0xdc0818cf78f21a8e70579cb46a43643f78291264dda342ae31049421c82d21ae"
                    ),
                    block_number=HexInt("0x1b4"),
                    from_=Address("0xa7d9ddbe1f17865597fbd27ec712455208b6b76e"),
                    gas=HexInt("0xc351"),
                    gas_price=HexInt("0x4a817c801"),
                    hash=HexBytes("0x1112"),
                    input=HexBytes("0x68656c6c6f22"),
                    nonce=HexInt("0x16"),
                    transaction_index=HexInt("0x42"),
                    v=HexInt("0x26"),
                    r=HexBytes(
                        "0x1b5e176d927f8e9ab405058b2d2457392da3e20f328b16ddabcebc33eaac5feb"
                    ),
                    s=HexBytes(
                        "0x4ba69724e8f69de52f0125ad8b3c5c2cef33019bac3249e2c0a2192766d1721d"
                    ),
                    to_=Address("0xf02c1c8e6114b1dbe8937a39260b5b0a374432bc"),
                    value=HexInt("0xf3dbb76162001"),
                ),
            ],
            transactions_root=HexBytes(
                "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
            ),  # noqa: E501
            uncles=[HexBytes("0x90"), HexBytes("0x91")],
        )
        actual = await self._rpc_client.get_block(HexInt(1), True)
        self.assertEqual(expected, actual)


class GetBlockNumberTestCase(BaseRPCClientTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self._rpc_response["result"] = "0x0"

    async def test_get_block_number_sends_proper_json(self):
        await self._rpc_client.get_block_number()
        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": tuple(),
                "id": ANY,
            },
        )

    async def test_get_block_number_returns_expected_value(self):
        expected = HexInt("0x0")
        actual = await self._rpc_client.get_block_number()
        self.assertEqual(expected, actual)

    async def test_get_block_number_raises_exception_for_rpc_error(self):
        self._rpc_response = {"jsonrpc": "rpc", "error": {"code": "code", "message": "message"}}
        expected = r"RPC rpc - Req .+ - code: message"
        with self.assertRaisesRegex(RpcServerError, expected):
            try:
                await self._rpc_client.get_block_number()
            except Exception as e:
                raise e


@ddt.ddt
class GetLogsTestCase(BaseRPCClientTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self._rpc_response = dict(result=list())

    async def test_sends_the_correct_rpc_version(self):
        async for _ in self._rpc_client.get_logs([], HexInt(0), HexInt(1), Address("contract")):
            pass

        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": "2.0",
                "method": ANY,
                "params": ANY,
                "id": ANY,
            },
        )

    async def test_sends_the_correct_rpc_method(self):
        async for _ in self._rpc_client.get_logs([], HexInt(0), HexInt(1), Address("contract")):
            pass

        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": ANY,
                "method": "eth_getLogs",
                "params": ANY,
                "id": ANY,
            },
        )

    async def test_sends_topic_array_in_params(self):
        expected_topics = [["topic 1 - option 1", "topic 1 - option 2"], "topic 2"]
        async for _ in self._rpc_client.get_logs(
            [topic[:] for topic in expected_topics], HexInt(0), HexInt(1), Address("contract")
        ):
            pass

        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": ANY,
                "method": ANY,
                "params": (
                    {
                        "fromBlock": ANY,
                        "toBlock": ANY,
                        "address": ANY,
                        "topics": expected_topics,
                    },
                ),
                "id": ANY,
            },
        )

    async def test_sends_the_correct_from_block(self):
        async for _ in self._rpc_client.get_logs([], HexInt(0), HexInt(1), Address("contract")):
            pass

        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": ANY,
                "method": ANY,
                "params": (
                    {
                        "fromBlock": "0x0",
                        "toBlock": ANY,
                        "address": ANY,
                        "topics": ANY,
                    },
                ),
                "id": ANY,
            },
        )

    async def test_sends_the_correct_to_block(self):
        async for _ in self._rpc_client.get_logs([], HexInt(0), HexInt(1), Address("contract")):
            pass

        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": ANY,
                "method": ANY,
                "params": (
                    {
                        "fromBlock": ANY,
                        "toBlock": "0x1",
                        "address": ANY,
                        "topics": ANY,
                    },
                ),
                "id": ANY,
            },
        )

    async def test_sends_the_correct_address(self):
        async for _ in self._rpc_client.get_logs([], HexInt(0), HexInt(1), Address("contract")):
            pass

        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": ANY,
                "method": ANY,
                "params": (
                    {
                        "fromBlock": ANY,
                        "toBlock": ANY,
                        "address": "contract",
                        "topics": ANY,
                    },
                ),
                "id": ANY,
            },
        )

    async def test_breaks_up_block_into_block_chunk_size_chunks(self):
        async for _ in self._rpc_client.get_logs(
            [], HexInt(1), HexInt(100_001), Address("contract"), 100_000
        ):
            pass

        self._ws_connect_patch.return_value.send_json.assert_has_awaits(
            (
                call(
                    {
                        "jsonrpc": "2.0",
                        "method": "eth_getLogs",
                        "params": (
                            {
                                "fromBlock": hex(1),
                                "toBlock": hex(100_000),
                                "address": ANY,
                                "topics": ANY,
                            },
                        ),
                        "id": ANY,
                    }
                ),
                call(
                    {
                        "jsonrpc": "2.0",
                        "method": "eth_getLogs",
                        "params": (
                            {
                                "fromBlock": hex(100_001),
                                "toBlock": hex(100_001),
                                "address": ANY,
                                "topics": ANY,
                            },
                        ),
                        "id": ANY,
                    }
                ),
            )
        )

    @ddt.data(
        -32005,  # Infura
        -32602,  # Alchemy
    )
    async def test_reduces_block_range_until_one_and_errors_when_to_many_records_error_returned(
        self,
        error_code,
    ):
        self._rpc_response = dict(
            jsonrpc="2.0", error=dict(code=error_code, message="Too many records")
        )

        with self.assertRaises(RpcServerError) as actual:
            async for _ in self._rpc_client.get_logs(
                [], HexInt(1), HexInt(100), Address("contract")
            ):
                pass

        self.assertEqual(error_code, actual.exception.error_code)
        self.assertEqual("Too many records", actual.exception.error_message)

        self._ws_connect_patch.return_value.send_json.assert_has_awaits(
            (
                call(
                    {
                        "jsonrpc": "2.0",
                        "method": "eth_getLogs",
                        "params": (
                            {
                                "fromBlock": hex(1),
                                "toBlock": hex(100),
                                "address": ANY,
                                "topics": ANY,
                            },
                        ),
                        "id": ANY,
                    }
                ),
                call(
                    {
                        "jsonrpc": "2.0",
                        "method": "eth_getLogs",
                        "params": (
                            {
                                "fromBlock": hex(1),
                                "toBlock": hex(10),
                                "address": ANY,
                                "topics": ANY,
                            },
                        ),
                        "id": ANY,
                    }
                ),
                call(
                    {
                        "jsonrpc": "2.0",
                        "method": "eth_getLogs",
                        "params": (
                            {
                                "fromBlock": hex(1),
                                "toBlock": hex(1),
                                "address": ANY,
                                "topics": ANY,
                            },
                        ),
                        "id": ANY,
                    }
                ),
            )
        )

    async def test_waits_to_make_subsequent_rpc_calls_until_looped_through_current_result(self):
        self._rpc_response = dict(
            result=[
                dict(
                    removed=False,
                    logIndex="0x10",
                    transactionIndex="0x11",
                    transactionHash="0x12",
                    blockHash="0x13",
                    blockNumber="0x14",
                    address="0x15",
                    data="0x16",
                    topics=["0x17", "0x18"],
                )
            ]
        )
        logs = self._rpc_client.get_logs(
            [], HexInt(1), HexInt(200_000), Address("contract"), 100_000
        ).__aiter__()
        self.assertEqual(0, self._ws_connect_patch.return_value.send_json.await_count)
        await logs.__anext__()
        self.assertEqual(1, self._ws_connect_patch.return_value.send_json.await_count)
        self._rpc_response = dict(
            result=[
                dict(
                    removed=True,
                    logIndex="0x20",
                    transactionIndex="0x21",
                    transactionHash="0x22",
                    blockHash="0x23",
                    blockNumber="0x24",
                    address="0x25",
                    data="0x26",
                    topics=["0x27", "0x28"],
                )
            ]
        )
        await logs.__anext__()
        self.assertEqual(2, self._ws_connect_patch.return_value.send_json.await_count)

    async def test_returns_correct_logs(self):
        self._rpc_response = dict(
            result=[
                dict(
                    removed=False,
                    logIndex="0x10",
                    transactionIndex="0x11",
                    transactionHash="0x12",
                    blockHash="0x13",
                    blockNumber="0x14",
                    address="0x15",
                    data="0x16",
                    topics=["0x17", "0x18"],
                ),
                dict(
                    removed=True,
                    logIndex="0x20",
                    transactionIndex="0x21",
                    transactionHash="0x22",
                    blockHash="0x23",
                    blockNumber="0x24",
                    address="0x25",
                    data="0x26",
                    topics=["0x27", "0x28"],
                ),
            ]
        )
        expected = [
            EvmLog(
                removed=False,
                log_index=HexInt("0x10"),
                transaction_index=HexInt("0x11"),
                transaction_hash=HexBytes("0x12"),
                block_hash=HexBytes("0x13"),
                block_number=HexInt("0x14"),
                address=Address("0x15"),
                data=HexBytes("0x16"),
                topics=[HexBytes("0x17"), HexBytes("0x18")],
            ),
            EvmLog(
                removed=True,
                log_index=HexInt("0x20"),
                transaction_index=HexInt("0x21"),
                transaction_hash=HexBytes("0x22"),
                block_hash=HexBytes("0x23"),
                block_number=HexInt("0x24"),
                address=Address("0x25"),
                data=HexBytes("0x26"),
                topics=[HexBytes("0x27"), HexBytes("0x28")],
            ),
        ]
        actual = list()
        async for log in self._rpc_client.get_logs(
            [], HexInt(1), HexInt(20_000), Address("contract")
        ):
            actual.append(log)
        self.assertEqual(expected, actual)


class TestConnectionPoolingEvmRpcClientTestCase(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__uri = "Some URI"
        self.__client_1 = AsyncMock(EvmRpcClient)
        self.__client_2 = AsyncMock(EvmRpcClient)
        self.__client_3 = AsyncMock(EvmRpcClient)
        self.__stats_service = MagicMock(StatsService)
        self.__client = ConnectionPoolingEvmRpcClient(
            [self.__client_1, self.__client_2, self.__client_3]
        )

    async def test_aenter_called_for_each_client(self):
        async with self.__client:
            pass

        self.__client_1.__aenter__.assert_awaited_once()
        self.__client_2.__aenter__.assert_awaited_once()
        self.__client_3.__aenter__.assert_awaited_once()

    async def test_aexit_called_for_each_client(self):
        async with self.__client:
            pass

        self.__client_1.__aexit__.assert_awaited_once()
        self.__client_2.__aexit__.assert_awaited_once()
        self.__client_3.__aexit__.assert_awaited_once()

    async def test_passes_calls_to_pool_clients_round_robin(self):
        self.__client_1.send.return_value = "0x1"
        self.__client_2.send.return_value = "0x2"
        self.__client_3.send.return_value = "0x3"
        async with self.__client as client:
            await client.get_block_number()
            self.__client_1.send.assert_awaited_once_with("eth_blockNumber")
            await client.get_block_number()
            self.__client_2.send.assert_awaited_once_with("eth_blockNumber")
            await client.get_block_number()
            self.__client_3.send.assert_awaited_once_with("eth_blockNumber")
            await client.get_block_number()
            self.assertEqual(
                2, self.__client_1.send.call_count, "Expected client 1 to be called twice"
            )

    async def test_passes_result_from_pool_clients_as_result(self):
        self.__client_1.send.return_value = "0x1"
        async with self.__client as client:
            actual = await client.get_block_number()
            self.assertEqual(HexInt(0x1), actual)


if __name__ == "__main__":
    unittest.main()
