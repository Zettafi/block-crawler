import asyncio
import unittest
from typing import Dict, Any
from unittest import IsolatedAsyncioTestCase
from unittest import TestCase
from unittest.mock import patch, ANY, MagicMock, AsyncMock

import aiohttp.client_exceptions
import ddt
from hexbytes import HexBytes

from blockrail.blockcrawler.core.rpc import RPCError, RPCServerError
from blockrail.blockcrawler.evm.rpc import EthCall, EVMRPCClient
from blockrail.blockcrawler.evm.types import (
    EVMTransactionReceipt,
    EVMLog,
    EVMBlock,
    HexInt,
    Address,
)
from blockrail.blockcrawler.evm.util import (
    ERC721Functions,
    ERC165Functions,
    ERC721MetadataFunctions,
)


class EthCallTestCase(TestCase):
    def test_equal_attributes_are_equal(self):
        left = EthCall(
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        self.assertEqual(left, right)

    def test_different_from_are_not_equal(self):
        left = EthCall(
            from_="from 1",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            from_="from 2",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_tos_are_not_equal(self):
        left = EthCall(
            from_="from",
            to="to 1",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            from_="from",
            to="to 2",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_functions_are_not_equal(self):
        left = EthCall(
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block",
        )
        right = EthCall(
            from_="from",
            to="to",
            function=ERC721Functions.IS_APPROVED_FOR_ALL,
            parameters=["parameter"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_parameters_are_not_equal(self):
        left = EthCall(
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter 1"],
            block="block",
        )
        right = EthCall(
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter 2"],
            block="block",
        )
        self.assertNotEqual(left, right)

    def test_different_blocks_are_not_equal(self):
        left = EthCall(
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
            parameters=["parameter"],
            block="block 1",
        )
        right = EthCall(
            from_="from",
            to="to",
            function=ERC721Functions.TRANSFER_FROM,
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

        self._rpc_url = "PROVIDER_URL"
        self._rpc_client = EVMRPCClient(self._rpc_url)
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
        with self.assertRaises(RPCError):
            await self._rpc_client.get_transaction_receipt(HexBytes(b"hash"))

    async def test_get_transactions_raise_RPCError_for_asyncio_timeout(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = asyncio.TimeoutError
        with self.assertRaises(RPCError):
            await self._rpc_client.get_transaction_receipt(HexBytes(b"hash"))

    async def test_get_blocks_raise_RPCError_for_aio_client_error(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = (
            aiohttp.client_exceptions.ClientError
        )
        with self.assertRaises(RPCError):
            await self._rpc_client.get_block(HexInt(1))

    async def test_get_blocks_raise_RPCError_for_asyncio_timeout(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = asyncio.TimeoutError
        with self.assertRaises(RPCError):
            await self._rpc_client.get_block(HexInt(1))

    async def test_get_block_number_raises_RPCError_for_aio_client_error(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = (
            aiohttp.client_exceptions.ClientError
        )
        with self.assertRaises(RPCError):
            await self._rpc_client.get_block_number()

    async def test_get_block_number_raises_RPCError_for_asyncio_timeout(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = asyncio.TimeoutError
        with self.assertRaises(RPCError):
            await self._rpc_client.get_block_number()

    async def test_calls_raise_RPCError_for_aio_client_error(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = (
            aiohttp.client_exceptions.ClientError
        )
        with self.assertRaises(RPCError):
            await self._rpc_client.call(MagicMock())

    async def test_calls_raises_RPCError_for_asyncio_timeout(
        self,
    ):
        self._ws_connect_patch.return_value.send_json.side_effect = asyncio.TimeoutError
        with self.assertRaises(RPCError):
            await self._rpc_client.call(MagicMock())


class GetTransactionReceiptTestCase(BaseRPCClientTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
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
        except RPCError:
            pass
        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": "2.0",
                "method": "eth_getTransactionReceipt",
                "params": ("0x1234",),
                "id": ANY,
            }
        )

    async def test_get_transaction_receipt_provides_all_responses(self):
        expected = EVMTransactionReceipt(
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
                EVMLog(
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
                EVMLog(
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


class CallsTestCase(BaseRPCClientTestCase):
    async def test_eth_call_is_sent_as_method(self):
        call = EthCall("from", "to", ERC721MetadataFunctions.NAME)
        await self._rpc_client.call(call)
        actual = self._get_request_json_method_from_post_patch_call()
        self.assertEqual("eth_call", actual)

    async def test_no_params_sends_expected(self):
        call = EthCall("from", "to", ERC721MetadataFunctions.NAME)
        await self._rpc_client.call(call)
        actual = self._get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(ERC721MetadataFunctions.NAME.function_signature_hash.hex(), actual)

    async def test_params_sends_expected(self):
        call = EthCall("from", "to", ERC721Functions.OWNER_OF_TOKEN, [1])
        await self._rpc_client.call(call)
        actual = self._get_request_json_params_from_post_patch_call()["data"]
        expected = (
            ERC721Functions.OWNER_OF_TOKEN.function_signature_hash.hex()
            + "0000000000000000000000000000000000000000000000000000000000000001"
        )
        self.assertEqual(expected, actual)

    async def test_decodes_false_response(self):
        expected = (False,)
        self._rpc_response = dict(
            result="0x0000000000000000000000000000000000000000000000000000000000000000"
        )
        call = EthCall("from", "to", ERC165Functions.SUPPORTS_INTERFACE)
        actual = await self._rpc_client.call(call)
        self.assertEqual(expected, actual)

    async def test_decode_BAYC_name_response(self):
        expected = ("BoredApeYachtClub",)
        self._rpc_response = dict(
            result="0x00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000011426f7265644170655961636874436c7562000000000000000000000000000000"  # noqa: E501
        )  # noqa: E501
        call = EthCall("from", "to", ERC721MetadataFunctions.NAME)
        actual = await self._rpc_client.call(call)
        self.assertEqual(expected, actual)


@ddt.ddt
class GetBlockTestCase(BaseRPCClientTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
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

    async def test_get_block_sends_requests_with_full_transactions_to_false(self):
        await self._rpc_client.get_block(HexInt(1))
        self._ws_connect_patch.return_value.send_json.assert_awaited_once_with(
            {
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": ("0x1", False),
                "id": ANY,
            },
        )

    async def test_get_blocks_parses_response_into_block_entities(self):
        expected = EVMBlock(
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
            transactions=[HexBytes("0x1111"), HexBytes("0x1112")],
            transactions_root=HexBytes(
                "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
            ),  # noqa: E501
            uncles=[HexBytes("0x90"), HexBytes("0x91")],
        )
        actual = await self._rpc_client.get_block(HexInt(1))
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
        expected = r"RPC rpc - Req \d - code: message"
        with self.assertRaisesRegex(RPCServerError, expected):
            try:
                await self._rpc_client.get_block_number()
            except Exception as e:
                raise e


if __name__ == "__main__":
    unittest.main()
