import unittest
from random import randrange
from unittest.mock import patch

from chainconductor.web3.rpc import RPCClient, EthCall


class CallMethodTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        patcher = patch(
            "aiohttp.ClientSession.post",
        )
        self.__post_patch = patcher.start()
        self.addCleanup(patcher.stop)

        # Make sure the `id` for each response matches with the index of the request made
        self.__rpc_responses = [dict(jsonrpc="Mock", id=0, result="")]

        self.__post_patch.return_value.__aenter__.return_value.json.return_value = (
            self.__rpc_responses
        )
        self.__provider_url = "PROVIDER_URL"
        self.__rpc_client = RPCClient(self.__provider_url)

    def tearDown(self) -> None:
        super().tearDown()

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
        calls = [EthCall("someid", "from", "to", "method_hash", [], list())]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_method_from_post_patch_call()
        self.assertEqual("eth_call", actual)

    async def test_no_params_sends_expected(self):
        calls = [EthCall("someid", "from", "to", "method_hash", [], list())]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual("method_hash", actual)

    async def test_bool_true_sends_expected_params(self):
        expected = "method_hash0000000000000000000000000000000000000000000000000000000000000001"
        calls = [EthCall("someid", "from", "to", "method_hash", ["bool"], [True])]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(expected, actual)

    async def test_bool_false_sends_expected_params(self):
        expected = "method_hash0000000000000000000000000000000000000000000000000000000000000000"
        calls = [EthCall("someid", "from", "to", "method_hash", ["bool"], [False])]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(expected, actual)

    async def test_bytes_sends_expected_params(self):
        expected = "method_hash6162630000000000000000000000000000000000000000000000000000000000"
        calls = [EthCall("someid", "from", "to", "method_hash", ["bytes3"], [b"abc"])]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(expected, actual)

    async def test_str_sends_expected_params(self):
        expected = "method_hash6162630000000000000000000000000000000000000000000000000000000000"
        calls = [EthCall("someid", "from", "to", "method_hash", ["bytes3"], [b"abc"])]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(expected, actual)

    async def test_int_sends_expected_params(self):
        expected = "method_hash0000000000000000000000000000000000000000000000000000000000000045"
        calls = [EthCall("someid", "from", "to", "method_hash", ["uint32"], [69])]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(expected, actual)

    async def test_solidity_lang_example_1(self):
        expected = "0xcdcd77c000000000000000000000000000000000000000000000000000000000000000450000000000000000000000000000000000000000000000000000000000000001"
        param_types = ["uint32", "bool"]
        method_hash = "0xcdcd77c0"
        args = [69, True]
        calls = [EthCall("someid", "from", "to", method_hash, param_types, args)]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(expected, actual)

    async def test_solidity_lang_example_2(self):
        expected = (
            "0xfce353f6"
            "6162630000000000000000000000000000000000000000000000000000000000"
            "6465660000000000000000000000000000000000000000000000000000000000"
        )
        param_types = ["bytes3[2]"]
        method_hash = "0xfce353f6"
        args = [[b"abc", b"def"]]
        calls = [EthCall("someid", "from", "to", method_hash, param_types, args)]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(expected, actual)

    async def test_solidity_lang_example_3(self):
        expected = (
            "0xa5643bf2"
            "0000000000000000000000000000000000000000000000000000000000000060"
            "0000000000000000000000000000000000000000000000000000000000000001"
            "00000000000000000000000000000000000000000000000000000000000000a0"
            "0000000000000000000000000000000000000000000000000000000000000004"
            "6461766500000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000003"
            "0000000000000000000000000000000000000000000000000000000000000001"
            "0000000000000000000000000000000000000000000000000000000000000002"
            "0000000000000000000000000000000000000000000000000000000000000003"
        )
        param_types = ["bytes", "bool", "uint256[]"]
        method_hash = "0xa5643bf2"
        args = [b"dave", True, [1, 2, 3]]
        calls = [EthCall("someid", "from", "to", method_hash, param_types, args)]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(expected, actual)

    async def test_solidity_lang_example_4(self):
        expected = (
            "0x8be65246"
            "0000000000000000000000000000000000000000000000000000000000000123"
            "0000000000000000000000000000000000000000000000000000000000000080"
            "3132333435363738393000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000e0"
            "0000000000000000000000000000000000000000000000000000000000000002"
            "0000000000000000000000000000000000000000000000000000000000000456"
            "0000000000000000000000000000000000000000000000000000000000000789"
            "000000000000000000000000000000000000000000000000000000000000000d"
            "48656c6c6f2c20776f726c642100000000000000000000000000000000000000"
        )
        param_types = ["uint", "uint32[]", "bytes10", "bytes"]
        method_hash = "0x8be65246"
        args = [0x123, [0x456, 0x789], b"1234567890", b"Hello, world!"]
        calls = [EthCall("someid", "from", "to", method_hash, param_types, args)]
        await self.__rpc_client.calls(calls)
        actual = self.__get_request_json_params_from_post_patch_call()["data"]
        self.assertEqual(expected, actual)

    async def test_decodes_false_response(self):
        expected = {"someid": (False,)}
        self.__rpc_responses[0][
            "result"
        ] = "0x0000000000000000000000000000000000000000000000000000000000000000"
        calls = [EthCall("someid", "from", "to", "method_hash", [], list(), "bool")]
        actual = await self.__rpc_client.calls(calls)
        self.assertEqual(expected, actual)

    async def test_decode_BAYC_name_response(self):
        expected = {"someid": ("BoredApeYachtClub",)}
        self.__rpc_responses[0][
            "result"
        ] = "0x00000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000011426f7265644170655961636874436c7562000000000000000000000000000000"
        calls = [EthCall("someid", "from", "to", "method_hash", [], list(), "string")]
        actual = await self.__rpc_client.calls(calls)
        self.assertEqual(expected, actual)


if __name__ == "__main__":
    unittest.main()
