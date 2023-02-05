import typing as t

from click import ParamType, Parameter, Context

from blockcrawler.core.entities import BlockChain, HexInt
from blockcrawler.evm.types import Address


class BlockChainParamType(ParamType):
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
