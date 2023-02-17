"""Core entities"""
from abc import ABC
from enum import Enum


class Entity(ABC):
    """Base class from which all entities are derived"""

    pass


class BlockChain(Enum):
    """Enum to identify a blockchain/network combination"""

    ETHEREUM_MAINNET = "ethereum-mainnet"
    """Ethereum blockchain and mainnet network"""

    POLYGON_MAINNET = "polygon-mainnet"
    """Polygon blockchain and mainnet network"""
