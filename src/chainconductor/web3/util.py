from typing import List, Dict


class Function:
    def __init__(
        self,
        function_hash: str,
        description: str,
        param_types: List[str],
        return_types: List[str],
        is_view: bool,
    ):
        self.__function_hash = function_hash
        self.__description = description
        self.__param_types = param_types
        self.__return_types = return_types
        self.__is_view = is_view

    @property
    def function_hash(self) -> str:
        return self.__function_hash

    @property
    def description(self) -> str:
        return self.__description

    @property
    def param_types(self) -> List[str]:
        return self.__param_types

    @property
    def return_types(self) -> List[str]:
        return self.__return_types

    @property
    def is_view(self):
        return self.__is_view


class ERC165Functions:
    SUPPORTS_INTERFACE = Function(
        "0x01ffc9a7",
        "supportsInterface(bytes4)->(bool)",
        ["bytes4"],
        ["bool"],
        True,
    )


class ERC721Functions:
    BALANCE_OF_ADDRESS = Function(
        "0x70a08231",
        "balanceOf(address)->(uint256)",
        ["address"],
        ["bool"],
        True,
    )
    OWNER_OF_TOKEN = Function(
        "0x6352211e",
        "ownerOf(uint256)->(address)",
        ["uint256"],
        ["address"],
        True,
    )
    GET_APPROVED_TOKEN = Function(
        "0x081812fc",
        "getApproved(uint256)->(address)",
        ["uint256"],
        ["address"],
        True,
    )
    IS_APPROVED_FOR_ALL = Function(
        "0xe985e9c5",
        "isApprovedForAll(address,address)",
        ["address", "address"],
        ["bool"],
        True,
    )


class ERC721TokenReceiverFunctions:
    ON_ERC721_RECEIVED = Function(
        "0x01ffc9a7",
        "onERC721Received(address,address,uint256,bytes)->(bytes4)",
        ["address", "address", "uint256", "bytes"],
        ["bytes4"],
        True,
    )


class ERC721MetadataFunctions:
    NAME = Function(
        "0x06fdde03",
        "name()->(string)",
        [],
        ["string"],
        True,
    )
    SYMBOL = Function(
        "0x95d89b41",
        "symbol()->(string)",
        [],
        ["string"],
        True,
    )
    TOKEN_URI = Function(
        "0xc87b56dd",
        "tokenURI(uint256)->(string)",
        ["uint256"],
        ["string"],
        True,
    )


class ERC721EnumerableFunctions:
    TOTAL_SUPPLY = Function(
        "0x18160ddd",
        "totalSupply()->(uint256)",
        [],
        ["uint256"],
        True,
    )
    TOKEN_BY_INDEX = Function(
        "0x4f6ccce7",
        "tokenByIndex(uint256)->uint(256)",
        ["uint256"],
        ["uint256"],
        True,
    )
    TOKEN_OF_OWNER_BY_INDEX = Function(
        "0x2f745c59",
        "tokenOfOwnerByIndex(address,uint256)->(uint256)",
        ["address", "uint256"],
        ["uint256"],
        True,
    )


class ERC1155MedataURIFunctions:
    URI = Function(
        "0x0e89341c",
        "uri(uint256)->(string)",
        ["uint256"],
        ["string"],
        True,
    )


OPCODE_MAP: Dict[str, int] = {
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


def contract_data_to_opcodes(contract_data: str):
    code = contract_data[2:]
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
        opcode, instr_chunks = OPCODE_MAP[opcode_code]
        if instr_chunks > 0:
            opcode += " 0x"
            for _ in range(instr_chunks):
                try:
                    opcode += chunks.pop(0)
                except IndexError:
                    # It looks like all the utilities just use what they can create
                    # After verifying a number of contracts bytecode that didn't complete,
                    # they seem to work fine.
                    return opcodes
        opcodes.append(opcode)
    return opcodes


def contract_implements_function(contract_data: str, function_hash):
    # opcodes = contract_data_to_opcodes(contract_data)
    return f"63{function_hash[2:]}" in contract_data
