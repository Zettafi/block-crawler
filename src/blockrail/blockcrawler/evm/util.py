import csv
import pathlib
from dataclasses import dataclass
from typing import List, Tuple

from hexbytes import HexBytes


@dataclass(frozen=True)
class Function:
    function_signature_hash: HexBytes
    description: str
    param_types: List[str]
    return_types: List[str]
    is_view: bool


@dataclass(frozen=True)
class Event:
    event_signature_hash: HexBytes
    description: str
    indexed_param_types: List[str]
    non_indexed_param_types: List[str]


class Erc165Functions:
    SUPPORTS_INTERFACE = Function(
        HexBytes("0x01ffc9a7"),
        "supportsInterface(bytes4)->(bool)",
        ["bytes4"],
        ["bool"],
        True,
    )


class Erc721TokenReceiverFunctions:
    ON_ERC721_RECEIVED = Function(
        HexBytes("0x01ffc9a7"),
        "onERC721Received(address,address,uint256,bytes)->(bytes4)",
        ["address", "address", "uint256", "bytes"],
        ["bytes4"],
        True,
    )


class Erc721Functions:
    BALANCE_OF_ADDRESS = Function(
        HexBytes("0x70a08231"),
        "balanceOf(address)->(uint256)",
        ["address"],
        ["bool"],
        True,
    )
    OWNER_OF_TOKEN = Function(
        HexBytes("0x6352211e"),
        "ownerOf(uint256)->(address)",
        ["uint256"],
        ["address"],
        True,
    )
    GET_APPROVED_TOKEN = Function(
        HexBytes("0x081812fc"),
        "getApproved(uint256)->(address)",
        ["uint256"],
        ["address"],
        True,
    )
    IS_APPROVED_FOR_ALL = Function(
        HexBytes("0xe985e9c5"),
        "isApprovedForAll(address,address)",
        ["address", "address"],
        ["bool"],
        True,
    )
    SAFE_TRANSFER_FROM_WITH_DATA = Function(
        HexBytes("0xb88d4fde"),
        "safeTransferFrom(address,address,uint256,bytes)",
        ["address", "address", "uint256", "bytes"],
        [],
        False,
    )
    SAFE_TRANSFER_FROM_WITHOUT_DATA = Function(
        HexBytes("0x42842e0e"),
        "safeTransferFrom(address,address,uint256)",
        ["address", "address", "uint256"],
        [],
        False,
    )
    TRANSFER_FROM = Function(
        HexBytes("0x42842e0e"),
        "safeTransferFrom(address,address,uint256)",
        ["address", "address", "uint256"],
        [],
        False,
    )


class Erc721MetadataFunctions:
    NAME = Function(
        HexBytes("0x06fdde03"),
        "name()->(string)",
        [],
        ["string"],
        True,
    )
    SYMBOL = Function(
        HexBytes("0x95d89b41"),
        "symbol()->(string)",
        [],
        ["string"],
        True,
    )
    TOKEN_URI = Function(
        HexBytes("0xc87b56dd"),
        "tokenURI(uint256)->(string)",
        ["uint256"],
        ["string"],
        True,
    )


class Erc721EnumerableFunctions:
    TOTAL_SUPPLY = Function(
        HexBytes("0x18160ddd"),
        "totalSupply()->(uint256)",
        [],
        ["uint256"],
        True,
    )
    # noinspection SpellCheckingInspection
    TOKEN_BY_INDEX = Function(
        HexBytes("0x4f6ccce7"),
        "tokenByIndex(uint256)->uint(256)",
        ["uint256"],
        ["uint256"],
        True,
    )
    TOKEN_OF_OWNER_BY_INDEX = Function(
        HexBytes("0x2f745c59"),
        "tokenOfOwnerByIndex(address,uint256)->(uint256)",
        ["address", "uint256"],
        ["uint256"],
        True,
    )


class Erc1155MetadataUriFunctions:  # pragma: no cover
    URI = Function(
        HexBytes("0x0e89341c"),
        "uri(uint256)->(string)",
        ["uint256"],
        ["string"],
        True,
    )


class AdditionalFunctions:  # pragma: no cover
    OWNER = Function(
        HexBytes("0x8da5cb5b"),
        "owner()->(address)",
        [],
        ["address"],
        True,
    )


class Erc721Events:  # pragma: no cover
    TRANSFER = Event(
        HexBytes("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
        "Transfer(address, address indexed, uint256 indexed)",
        ["address", "address", "uint256"],
        [],
    )


class Erc1155Events:  # pragma: no cover
    # noinspection SpellCheckingInspection
    TRANSFER_SINGLE = Event(
        HexBytes("0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"),
        "TransferSingle(address indexed, address indexed, address indexed, uint256, uint256)",
        ["address", "address", "address"],
        ["uint256", "uint256"],
    )
    TRANSFER_BATCH = Event(
        HexBytes("0xf5f16c58bf69e14e9fa06e742215b42aa896de1c15af339f09e3360557089f43"),
        "TransferMultiple(address indexed, address indexed, address indexed, uint256[], uint256[])",
        ["address", "address", "address"],
        ["uint256[]", "uint256[]"],
    )
    URI = Event(
        HexBytes("0x4a4d592fe8a6f7daf17876002d0f8dfb179fa30f514ee60bf4aa407ebb0fb326"),
        "URI(string, unit256 indexed)",
        ["uint256"],
        ["string"],
    )


class BlockTimeCacheManager:
    def __init__(self, cache_file: pathlib.Path) -> None:
        self.__cache_file = cache_file

    def get_block_times_from_cache(self):
        block_times: List[Tuple[int, int]] = list()
        try:
            with open(self.__cache_file, "r") as file:
                for block_id, timestamp in csv.reader(file):
                    block_times.append((int(block_id), int(timestamp)))
        except FileNotFoundError:
            pass
        return block_times

    def write_block_times_to_cache(self, block_timestamps: List[Tuple[int, int]]):
        with open(self.__cache_file, "w+") as file:
            csv_writer = csv.writer(file)
            for row in block_timestamps:
                csv_writer.writerow(row)
