import unittest
from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import patch, AsyncMock, ANY

from chainconductor.web3.rpc import RPCClient, EthCall
from chainconductor.web3.types import TransactionReceipt, Log
from chainconductor.web3.util import (
    ERC721Functions,
    ERC165Functions,
    ERC721MetadataFunctions,
)

from .. import async_context_manager_mock


class TestGetTransactionReceipts(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.__rpc_url = "test URL"
        self.__rpc_client = RPCClient(self.__rpc_url)
        patcher = patch("chainconductor.web3.rpc.aiohttp", AsyncMock)
        self.__aiohttp_patch = patcher.start()
        self.addAsyncCleanup(patcher.stop)
        self.__aiohttp_patch.ClientSession = async_context_manager_mock()
        self.__aiohttp_session = (
            self.__aiohttp_patch.ClientSession.return_value.__aenter__.return_value
        )
        self.__aiohttp_session.post = async_context_manager_mock()
        self.__aiohttp_session_post_response = (
            self.__aiohttp_session.post.return_value.__aenter__.return_value
        )
        self.__aiohttp_session_post_response.json.return_value = [
            {"jsonrpc": "mock", "id": "di", "error": {"code": "edoc", "message": "egassem"}}
        ]

    async def test_get_transaction_receipts_sends_request_to_correct_endpoint(self):
        await self.__rpc_client.get_transaction_receipts(["hash"])
        self.__aiohttp_session.post.assert_called_once_with(self.__rpc_url, json=ANY)

    async def test_get_transaction_receipts_sends_all_requests(self):
        await self.__rpc_client.get_transaction_receipts(["hash"])
        self.__aiohttp_session.post.assert_called_once_with(
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
        self.__aiohttp_session_post_response.json.return_value = [
            {
                "id": 1,
                "jsonrpc": "2.0",
                "result": {
                    "transactionHash": "0xb903239f8543d04b5dc1ba6579132b143087c68db1b2168786408fcbce568238",  # noqa: E501
                    "transactionIndex": "0x1",
                    "blockNumber": "0xb",
                    "blockHash": "0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b",  # noqa: E501
                    "cumulativeGasUsed": "0x33bc",
                    "gasUsed": "0x4dc",
                    "contractAddress": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                    "logs": [
                        {
                            "logIndex": "0x1",
                            "blockNumber": "0x1b4",
                            "blockHash": "0x8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcfdf829c5a142f1fccd7d",  # noqa: E501
                            "transactionHash": "0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcf",  # noqa: E501
                            "transactionIndex": "0x0",
                            "address": "0x16c5785ac562ff41e2dcfdf829c5a142f1fccd7d",
                            "data": "0x0000000000000000000000000000000000000000000000000000000000000000",  # noqa: E501
                            "topics": [
                                "0x59ebeb90bc63057b6515673c3ecf9438e5058bca0f92585014eced636878c9a5"
                            ],
                            "removed": False,
                        },
                        {
                            "logIndex": "0x2",
                            "blockNumber": "0x1b5",
                            "blockHash": "0x8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcfdf829c5a142f1fccd8e",  # noqa: E501
                            "transactionHash": "0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2ed0",  # noqa: E501
                            "transactionIndex": "0x1",
                            "address": None,
                            "data": "0x0000000000000000000000000000000000000000000000000000000000000001",  # noqa: E501
                            "topics": [
                                "0x59ebeb90b6d4057b6515673c3ecf9438e5058bcae8c2585014eced636878c9a5"
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

        expected = [
            TransactionReceipt(
                transaction_hash="0xb903239f8543d04b5dc1ba6579132b143087c68db1b2168786408fcbce568238",  # noqa: E501
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
                        transaction_hash="0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2dcf",  # noqa: E501
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
                        transaction_hash="0xdf829c5a142f1fccd7d8216c5785ac562ff41e2dcfdf5785ac562ff41e2ed0",  # noqa: E501
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
        actual = await self.__rpc_client.get_transaction_receipts(["hash"])
        self.assertEqual(expected, actual)


class CallMethodTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        patcher = patch(
            "aiohttp.ClientSession.post",
        )
        self.__post_patch = patcher.start()
        self.addCleanup(patcher.stop)

        patcher = patch(
            "aiohttp.TCPConnector",
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        # Make sure the `id` for each response matches with the index of the request made
        self.__rpc_responses = [dict(jsonrpc="Mock", id=0, result="")]

        self.__post_patch.return_value.__aenter__.return_value.json.return_value = (
            self.__rpc_responses
        )
        self.__provider_url = "PROVIDER_URL"
        self.__rpc_client = RPCClient(self.__provider_url)

    def __get_request_json_from_post_patch(self) -> dict:
        # Get the call tuple for the first call to the post patch
        # The third item in the call tuple is the key word args
        # The json is in teh "json" key word arg
        self.__post_patch.assert_called()
        json = self.__post_patch.call_args.kwargs["json"]
        return json

    def __get_request_json_params_from_post_patch_call(self, index=0):
        return self.__get_request_json_from_post_patch()[index]["params"][0]

    def __get_request_json_method_from_post_patch_call(self, index=0):
        return self.__get_request_json_from_post_patch()[index]["method"]

    async def test_eth_call_is_sent_as_method(self):
        calls = [EthCall("someid", "from", "to", ERC721Functions.IS_APPROVED_FOR_ALL)]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_method_from_post_patch_call()
        self.assertEqual("eth_call", actual)

    async def test_no_params_sends_expected(self):
        calls = [EthCall("someid", "from", "to", ERC721Functions.IS_APPROVED_FOR_ALL)]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(ERC721Functions.IS_APPROVED_FOR_ALL.function_hash, actual)

    async def test_decodes_false_response(self):
        expected = {"someid": (False,)}
        self.__rpc_responses[0][
            "result"
        ] = "0x0000000000000000000000000000000000000000000000000000000000000000"
        calls = [EthCall("someid", "from", "to", ERC165Functions.SUPPORTS_INTERFACE)]
        actual = await self.__rpc_client.calls(calls)
        self.assertEqual(expected, actual)

    async def test_decode_BAYC_name_response(self):
        expected = {"someid": ("BoredApeYachtClub",)}
        self.__rpc_responses[0][
            "result"
        ] = "0x00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000011426f7265644170655961636874436c7562000000000000000000000000000000"  # noqa: E501
        calls = [EthCall("someid", "from", "to", ERC721MetadataFunctions.NAME)]
        actual = await self.__rpc_client.calls(calls)
        self.assertEqual(expected, actual)


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


if __name__ == "__main__":
    unittest.main()
