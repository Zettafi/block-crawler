from enum import Enum
from typing import Union


class Entity:
    pass


class BlockChain(Enum):
    ETHEREUM_MAINNET = "ethereum-mainnet"


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

    def __eq__(self, o: object) -> bool:
        return isinstance(o, self.__class__) and o.hex_value == self.__hex_str

    def __str__(self) -> str:
        return self.__hex_str

    def __hash__(self) -> int:
        return self.int_value

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.__hex_str}')"

    @property
    def hex_value(self) -> str:
        return self.__hex_str

    @property
    def int_value(self) -> int:
        if self.__int_value is None:
            self.__int_value = int(self.__hex_str, 16)
        return self.__int_value


class Type:
    pass


class TransportObject:  # pragma: no cover
    pass
