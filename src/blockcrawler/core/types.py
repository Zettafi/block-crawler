"""Core types"""

from typing import NewType, Union

Address = NewType("Address", str)
"""A address type for explicitly identifying an address in usage"""


class HexInt:
    """
    A representation of an integer than can be easily translated between a hexadecimal
    string and an integer. It will evaluate in most forms as an integer representation.
    """

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
            return o.int_value == self.int_value
        elif isinstance(o, int):
            return self.int_value == o
        else:
            return NotImplemented

    def __str__(self) -> str:
        return self.__hex_str

    def __hash__(self) -> int:
        return self.int_value.__hash__()

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
        """Get the hexadecimal string representation of the object"""
        return self.__hex_str

    @property
    def int_value(self) -> int:
        """Get the integer representation of the object"""
        if self.__int_value is None:
            self.__int_value = int(self.__hex_str, 16)
        return self.__int_value

    def padded_hex(self, length: int):
        """
        Get a zero-padded hexadecimal string of the object. for example::

            HexInt(1).padded_hex(4)

        will return the hexadecimal string `0x00001`.

        """
        return "0x" + self.hex_value[2:].zfill(length)
