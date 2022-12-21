from enum import Enum
from typing import Union


class Entity:
    pass


class BlockChain(Enum):
    ETHEREUM_MAINNET = "ethereum-mainnet"
    POLYGON_MAINNET = "polygon-mainnet"


class HexInt:
    def __init__(self, value: Union[str, int]) -> None:
        if isinstance(value, str):
            self.__hex_str = value
            self.__int_value = int(value, 16)
        elif isinstance(value, int):
            self.__hex_str = hex(value)
            self.__int_value = value
        else:
            raise TypeError("parameter value must be str or int")

    def __eq__(self, o) -> bool:
        if isinstance(o, self.__class__):
            return o.hex_value == self.__hex_str
        elif isinstance(o, int):
            return self.int_value == o
        else:
            return NotImplemented

    def __str__(self) -> str:
        return self.__hex_str

    def __hash__(self) -> int:
        return self.int_value

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.__hex_str}')"

    def __lt__(self, other):
        if isinstance(other, self.__class__):
            return self.int_value < other.__int_value
        elif isinstance(other, int):
            return self.int_value < other
        else:
            return NotImplemented

    def __le__(self, other):
        if isinstance(other, self.__class__):
            return self.int_value <= other.__int_value
        elif isinstance(other, int):
            return self.int_value <= other
        else:
            return NotImplemented

    def __gt__(self, other):
        if isinstance(other, self.__class__):
            return self.int_value > other.__int_value
        elif isinstance(other, int):
            return self.int_value > other
        else:
            return NotImplemented

    def __ge__(self, other):
        if isinstance(other, self.__class__):
            return self.int_value >= other.__int_value
        elif isinstance(other, int):
            return self.int_value >= other
        else:
            return NotImplemented

    def __add__(self, other):
        if isinstance(other, self.__class__):
            return HexInt(self.int_value + other.int_value)
        elif isinstance(other, int):
            return HexInt(self.int_value + other)
        else:
            return NotImplemented

    def __radd__(self, other):
        return self.__add__(other).int_value

    def __sub__(self, other):
        if isinstance(other, self.__class__):
            return HexInt(self.int_value - other.int_value)
        elif isinstance(other, int):
            return HexInt(self.int_value - other)
        else:
            return NotImplemented

    def __rsub__(self, other):
        if isinstance(other, self.__class__):
            return HexInt(other.int_value - self.int_value)
        elif isinstance(other, int):
            return HexInt(other - self.int_value).int_value
        else:
            return NotImplemented

    @property
    def hex_value(self) -> str:
        return self.__hex_str

    @property
    def int_value(self) -> int:
        if self.__int_value is None:
            self.__int_value = int(self.__hex_str, 16)
        return self.__int_value

    def padded_hex(self, length: int):
        return "0x" + self.hex_value[2:].zfill(length)


class Type:
    pass


class TransportObject:  # pragma: no cover
    pass
