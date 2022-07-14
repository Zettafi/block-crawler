import asyncio

from chainconductor.web3.rpc import RPCClient, EthCall

BAYC_CONTRACT_ADDR = "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D"

codes = {
    "00": ("STOP", 0),
    "01": ("ADD", 0),
    "02": ("MUL", 0),
    "03": ("SUB", 0),
    "04": ("DIV", 0),
    "05": ("SDIV", 0),
    "06": ("MOD", 0),
    "07": ("SMOD", 0),
    "08": ("ADDMOD", 0),
    "09": ("MULMOD", 0),
    "0a": ("EXP", 0),
    "0b": ("SIGNEXTEND", 0),
    "0c": ("INVALID", 0),
    "0d": ("INVALID", 0),
    "0e": ("INVALID", 0),
    "0f": ("INVALID", 0),
    "10": ("LT", 0),
    "11": ("GT", 0),
    "12": ("SLT", 0),
    "13": ("SGT", 0),
    "14": ("EQ", 0),
    "15": ("ISZERO", 0),
    "16": ("AND", 0),
    "17": ("OR", 0),
    "18": ("XOR", 0),
    "19": ("NOT", 0),
    "1a": ("BYTE", 0),
    "1b": ("SHL", 0),
    "1c": ("SHR", 0),
    "1d": ("SAR", 0),
    "1e": ("INVALID", 0),
    "1f": ("INVALID", 0),
    "20": ("SHA3", 0),
    "21": ("INVALID", 0),
    "22": ("INVALID", 0),
    "23": ("INVALID", 0),
    "24": ("INVALID", 0),
    "25": ("INVALID", 0),
    "26": ("INVALID", 0),
    "27": ("INVALID", 0),
    "28": ("INVALID", 0),
    "29": ("INVALID", 0),
    "2a": ("INVALID", 0),
    "2b": ("INVALID", 0),
    "2c": ("INVALID", 0),
    "2d": ("INVALID", 0),
    "2e": ("INVALID", 0),
    "2f": ("INVALID", 0),
    "30": ("ADDRESS", 0),
    "31": ("BALANCE", 0),
    "32": ("ORIGIN", 0),
    "33": ("CALLER", 0),
    "34": ("CALLVALUE", 0),
    "35": ("CALLDATALOAD", 0),
    "36": ("CALLDATASIZE", 0),
    "37": ("CALLDATACOPY", 0),
    "38": ("CODESIZE", 0),
    "39": ("CODECOPY", 0),
    "3a": ("GASPRICE", 0),
    "3b": ("EXTCODESIZE", 0),
    "3c": ("EXTCODECOPY", 0),
    "3d": ("RETURNDATASIZE", 0),
    "3e": ("RETURNDATACOPY", 0),
    "3f": ("EXTCODEHASH", 0),
    "40": ("BLOCKHASH", 0),
    "41": ("COINBASE", 0),
    "42": ("TIMESTAMP", 0),
    "43": ("NUMBER", 0),
    "44": ("DIFFICULTY", 0),
    "45": ("GASLIMIT", 0),
    "46": ("CHAINID", 0),
    "47": ("SELFBALANCE", 0),
    "48": ("BASEFEE", 0),
    "49": ("INVALID", 0),
    "4a": ("INVALID", 0),
    "4b": ("INVALID", 0),
    "4c": ("INVALID", 0),
    "4d": ("INVALID", 0),
    "4e": ("INVALID", 0),
    "4f": ("INVALID", 0),
    "50": ("POP", 0),
    "51": ("MLOAD", 0),
    "52": ("MSTORE", 0),
    "53": ("MSTORE8", 0),
    "54": ("SLOAD", 0),
    "55": ("SSTORE", 0),
    "56": ("JUMP", 0),
    "57": ("JUMPI", 0),
    "58": ("PC", 0),
    "59": ("MSIZE", 0),
    "5a": ("GAS", 0),
    "5b": ("JUMPDEST", 0),
    "5c": ("INVALID", 0),
    "5d": ("INVALID", 0),
    "5e": ("INVALID", 0),
    "5f": ("INVALID", 0),
    "60": ("PUSH1", 1),
    "61": ("PUSH2", 2),
    "62": ("PUSH3", 3),
    "63": ("PUSH4", 4),
    "64": ("PUSH5", 5),
    "65": ("PUSH6", 6),
    "66": ("PUSH7", 7),
    "67": ("PUSH8", 8),
    "68": ("PUSH9", 9),
    "69": ("PUSH10", 10),
    "6a": ("PUSH11", 11),
    "6b": ("PUSH12", 12),
    "6c": ("PUSH13", 13),
    "6d": ("PUSH14", 14),
    "6e": ("PUSH15", 15),
    "6f": ("PUSH16", 16),
    "70": ("PUSH17", 17),
    "71": ("PUSH18", 18),
    "72": ("PUSH19", 19),
    "73": ("PUSH20", 20),
    "74": ("PUSH21", 21),
    "75": ("PUSH22", 22),
    "76": ("PUSH23", 23),
    "77": ("PUSH24", 24),
    "78": ("PUSH25", 25),
    "79": ("PUSH26", 26),
    "7a": ("PUSH27", 27),
    "7b": ("PUSH28", 28),
    "7c": ("PUSH29", 29),
    "7d": ("PUSH30", 30),
    "7e": ("PUSH31", 31),
    "7f": ("PUSH32", 32),
    "80": ("DUP1", 0),
    "81": ("DUP2", 0),
    "82": ("DUP3", 0),
    "83": ("DUP4", 0),
    "84": ("DUP5", 0),
    "85": ("DUP6", 0),
    "86": ("DUP7", 0),
    "87": ("DUP8", 0),
    "88": ("DUP9", 0),
    "89": ("DUP10", 0),
    "8a": ("DUP11", 0),
    "8b": ("DUP12", 0),
    "8c": ("DUP13", 0),
    "8d": ("DUP14", 0),
    "8e": ("DUP15", 0),
    "8f": ("DUP16", 0),
    "90": ("SWAP1", 0),
    "91": ("SWAP2", 0),
    "92": ("SWAP3", 0),
    "93": ("SWAP4", 0),
    "94": ("SWAP5", 0),
    "95": ("SWAP6", 0),
    "96": ("SWAP7", 0),
    "97": ("SWAP8", 0),
    "98": ("SWAP9", 0),
    "99": ("SWAP10", 0),
    "9a": ("SWAP11", 0),
    "9b": ("SWAP12", 0),
    "9c": ("SWAP13", 0),
    "9d": ("SWAP14", 0),
    "9e": ("SWAP15", 0),
    "9f": ("SWAP16", 0),
    "a0": ("LOG0", 0),
    "a1": ("LOG1", 0),
    "a2": ("LOG2", 0),
    "a3": ("LOG3", 0),
    "a4": ("LOG4", 0),
    "a5": ("INVALID", 0),
    "a6": ("INVALID", 0),
    "a7": ("INVALID", 0),
    "a8": ("INVALID", 0),
    "a9": ("INVALID", 0),
    "aa": ("INVALID", 0),
    "ab": ("INVALID", 0),
    "ac": ("INVALID", 0),
    "ad": ("INVALID", 0),
    "ae": ("INVALID", 0),
    "af": ("INVALID", 0),
    "b0": ("INVALID", 0),
    "b1": ("INVALID", 0),
    "b2": ("INVALID", 0),
    "b3": ("INVALID", 0),
    "b4": ("INVALID", 0),
    "b5": ("INVALID", 0),
    "b6": ("INVALID", 0),
    "b7": ("INVALID", 0),
    "b8": ("INVALID", 0),
    "b9": ("INVALID", 0),
    "ba": ("INVALID", 0),
    "bb": ("INVALID", 0),
    "bc": ("INVALID", 0),
    "bd": ("INVALID", 0),
    "be": ("INVALID", 0),
    "bf": ("INVALID", 0),
    "c0": ("INVALID", 0),
    "c1": ("INVALID", 0),
    "c2": ("INVALID", 0),
    "c3": ("INVALID", 0),
    "c4": ("INVALID", 0),
    "c5": ("INVALID", 0),
    "c6": ("INVALID", 0),
    "c7": ("INVALID", 0),
    "c8": ("INVALID", 0),
    "c9": ("INVALID", 0),
    "ca": ("INVALID", 0),
    "cb": ("INVALID", 0),
    "cc": ("INVALID", 0),
    "cd": ("INVALID", 0),
    "ce": ("INVALID", 0),
    "cf": ("INVALID", 0),
    "d0": ("INVALID", 0),
    "d1": ("INVALID", 0),
    "d2": ("INVALID", 0),
    "d3": ("INVALID", 0),
    "d4": ("INVALID", 0),
    "d5": ("INVALID", 0),
    "d6": ("INVALID", 0),
    "d7": ("INVALID", 0),
    "d8": ("INVALID", 0),
    "d9": ("INVALID", 0),
    "da": ("INVALID", 0),
    "db": ("INVALID", 0),
    "dc": ("INVALID", 0),
    "dd": ("INVALID", 0),
    "de": ("INVALID", 0),
    "df": ("INVALID", 0),
    "e0": ("INVALID", 0),
    "e1": ("INVALID", 0),
    "e2": ("INVALID", 0),
    "e3": ("INVALID", 0),
    "e4": ("INVALID", 0),
    "e5": ("INVALID", 0),
    "e6": ("INVALID", 0),
    "e7": ("INVALID", 0),
    "e8": ("INVALID", 0),
    "e9": ("INVALID", 0),
    "ea": ("INVALID", 0),
    "eb": ("INVALID", 0),
    "ec": ("INVALID", 0),
    "ed": ("INVALID", 0),
    "ee": ("INVALID", 0),
    "ef": ("INVALID", 0),
    "f0": ("CREATE", 0),
    "f1": ("CALL", 0),
    "f2": ("CALLCODE", 0),
    "f3": ("RETURN", 0),
    "f4": ("DELEGATECALL", 0),
    "f5": ("CREATE2", 0),
    "f6": ("INVALID", 0),
    "f7": ("INVALID", 0),
    "f8": ("INVALID", 0),
    "f9": ("INVALID", 0),
    "fa": ("STATICCALL", 0),
    "fb": ("INVALID", 0),
    "fc": ("INVALID", 0),
    "fd": ("REVERT", 0),
    "fe": ("INVALID", 0),
    "ff": ("SELFDESTRUCT", 0),
}


contract_methods = {
    "ERC-20": {
        "23b872dd": ("transferFrom(address,address,uint256)", True, False),
        "313ce567": ("decimals()", False, True),
        "095ea7b3": ("approve(address,uint256)", True, False),
        "95d89b41": ("symbol()", False, True),
        "18160ddd": ("totalSupply()", True, True),
        "a9059cbb": ("transfer(address,uint256)", True, False),
        "dd62ed3e": ("allowance(address,address)", False, True),
        "06fdde03": ("name()", False, True),
    },
    "ERC-721": {
        "4f6ccce7": ("tokenByIndex(uint256)", False, True),
        "e985e9c5": ("isApprovedForAll(address,address)", True, True),
        "23b872dd": ("transferFrom(address,address,uint256)", True, False),
        "150b7a02": ("onERC721Received(address,address,uint256,bytes)", False, False),
        "095ea7b3": ("approve(address,uint256)", True, False),
        "42842e0e": ("safeTransferFrom(address,address,uint256)", True, False),
        "081812fc": ("getApproved(uint256)", True, True),
        "a22cb465": ("setApprovalForAll(address,bool)", True, False),
        "95d89b41": ("symbol()", False, True),
        "6352211e": ("ownerOf(uint256)", True, True),
        "06fdde03": ("name()", False, True),
        "c87b56dd": ("tokenURI(uint256)", False, True),
        "18160ddd": ("totalSupply()", False, True),
        "2f745c59": ("tokenOfOwnerByIndex(address,uint256)", False, True),
        "b88d4fde": ("safeTransferFrom(address,address,uint256,bytes)", True, False),
        "01ffc9a7": ("supportsInterface(bytes4)", True, True),
        "70a08231": ("balanceOf(address)", True, True),
    },
    "ERC-165": {
        "01ffc9a7": ("supportsInterface(bytes4)", True, True),
    },
}


async def main(archive_node_uri):
    client = RPCClient(archive_node_uri)
    tx_hash = "0x22199329b0aa1aa68902a78e3b32ca327c872fab166c7a2838273de6ad383eba"
    receipt = (await client.get_transaction_receipts([tx_hash]))[0]
    block_id = receipt.block_number.int_value
    block = (await client.get_blocks({block_id}, True))[0]
    evm_code = None
    for transaction in block.transactions:
        if (
            transaction.transaction_index == receipt.transaction_index
            and transaction.hash == tx_hash
        ):
            evm_code = transaction.input
            break
    # evm_code: str = await client.get_code(ETH_TUTORIAL_CONTRACT)
    # evm_code = "0x60606040526000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063d46300fd146047578063ee919d5014606d575b600080fd5b3415605157600080fd5b6057608d565b6040518082815260200191505060405180910390f35b3415607757600080fd5b608b60048080359060200190919050506097565b005b6000805490505b90565b806000819055505b505600a165627a7a72305820f8109b567099d4f11dfc2edb5ded9d1fba0ff7b7236df05d77c809486ed326b50029"
    code = evm_code[2:]
    chunks = list()
    for i in range(0, len(code), 2):
        try:
            chunk = code[i : i + 2]
        except ValueError:
            raise ValueError(f"illegal hex character {code[i:i + 2]} in contract code")
        chunks.append(chunk)

    opcodes = list()
    while len(chunks) > 0:
        opcode_code = chunks.pop(0)
        opcode, instr_chunks = codes[opcode_code]
        if instr_chunks > 0:
            opcode += " "
            for _ in range(instr_chunks):
                opcode += chunks.pop(0)
        opcodes.append(opcode)

    for contract, methods in contract_methods.items():
        method_results = list()
        implements = True
        for hash, (method, required, view_func) in methods.items():
            opcode = "PUSH4 {}".format(hash)
            if opcode in opcodes:
                result = "Y"
            elif required:
                result = "X"
                implements = False
            else:
                result = "N"
            method_results.append("{} {}".format(result, method))
        print(contract, ":", implements)
        for method_result in method_results:
            print(method_result)

    responses = await client.calls(
        [
            EthCall(
                "Symbol:",
                None,
                "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
                "0x95d89b41",
                [],
                [],
                "string",
            ),
            EthCall(
                "Name:",
                None,
                "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
                "0x06fdde03",
                [],
                [],
                "string",
            ),
            EthCall(
                "Total Supply:",
                None,
                "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
                "0x18160ddd",
                [],
                [],
                "uint256",
            ),
        ]
    )
    for key, result in responses.items():
        print(key, result[0])


if __name__ == "__main__":
    asyncio.run(
        main(
            "wss://dawn-old-feather.quiknode.pro/6cd19e9c836664d7ed84d98dff8ea26c96b3a596/"
        )
    )
