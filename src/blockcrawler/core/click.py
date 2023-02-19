"""Classes to integrate with teh click library"""

import typing as t

from click import ParamType, Parameter, Context
from hexbytes import HexBytes

from blockcrawler.core.entities import BlockChain
from blockcrawler.core.types import Address, HexInt
from blockcrawler.nft.entities import EthereumCollectionType, CollectionType


class BlockChainParamType(ParamType):
    """CLick param type to parse input data and produce BlockChain enums"""

    name = "BlockChain"

    def convert(
        self, value: t.Any, param: t.Optional["Parameter"], ctx: t.Optional["Context"]
    ) -> t.Any:
        options = []
        for item in BlockChain:
            if item.value == value:
                return item
            else:
                options.append(item.value)
        self.fail(f"Invalid blockchain \"{value}\"! Must be one of: {', '.join(options)}")


class HexIntParamType(ParamType):
    """CLick param type to parse input data and produce HexInt instances"""

    name = "HexInt"

    def convert(
        self, value: t.Any, param: t.Optional["Parameter"], ctx: t.Optional["Context"]
    ) -> t.Any:
        try:
            if isinstance(value, str) and value.startswith("0x"):
                converted = HexInt(value)
            else:
                converted = HexInt(int(value))
            return converted
        except ValueError:
            pass

        self.fail(f'Invalid value "{value}"! Must be either a hexadecimal string or integer')


class AddressParamType(ParamType):
    """Click param type to parse input data and produce Address instances"""

    name = "Address"

    def convert(
        self, value: t.Any, param: t.Optional["Parameter"], ctx: t.Optional["Context"]
    ) -> t.Any:
        if isinstance(value, str) and value.startswith("0x") and len(value) == 42:
            try:
                lowered = value.lower()
                bytes.fromhex(lowered[2:])
                return Address(lowered)
            except ValueError:
                pass

        self.fail(f'Invalid value "{value}"! Must be a hexadecimal string of 20 bytes')


class HexBytesParamType(ParamType):
    """Click param type to parse input data and produce HexBytes instances"""

    name = "HexBytes"

    def convert(
        self, value: t.Any, param: t.Optional["Parameter"], ctx: t.Optional["Context"]
    ) -> t.Any:
        if isinstance(value, str) and value.startswith("0x"):
            try:
                return HexBytes(value)
            except ValueError:
                pass

        self.fail(f'Invalid value "{value}"! Must be a hexadecimal bytes string')


class EthereumCollectionTypeParamType(ParamType):
    """Click param type to parse input data and produce CollectionType instances"""

    name = "EthereumCollectionType"

    def __init__(self) -> None:
        self.__valid_types = [
            getattr(EthereumCollectionType, attr)
            for attr in dir(EthereumCollectionType)
            if attr.startswith("ERC")
        ]

    def convert(
        self, value: t.Any, param: t.Optional["Parameter"], ctx: t.Optional["Context"]
    ) -> t.Any:
        if value in self.__valid_types or (param and value == param.default):
            return CollectionType(value)

        self.fail(f'Value must be one of: {", ".join(self.__valid_types)}')
