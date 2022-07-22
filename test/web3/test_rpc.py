import unittest
from unittest.mock import patch

from chainconductor.web3.rpc import RPCClient, EthCall
from chainconductor.web3.util import (
    ERC721Functions,
    ERC165Functions,
    ERC721MetadataFunctions,
)


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


if __name__ == "__main__":
    unittest.main()
