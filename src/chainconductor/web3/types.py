from binascii import unhexlify
from enum import Enum
from typing import List, Union, Dict, Optional


class ERC165InterfaceID(Enum):
    ERC721 = "0x80ac58cd"
    ERC721_TOKEN_RECEIVER = "0x150b7a02"
    ERC721_METADATA = "0x5b5e139f"
    ERC721_ENUMERABLE = "0x780e9d63"
    ERC998_ERC721_TOP_DOWN = "0xcde244d9"
    ERC998_ERC721_TOP_DOWN_ENUMERABLE = "0xa344afe4"
    ERC998_ERC721_BOTTOM_UP = "0xa1b23002"
    ERC998_ERC721_BOTTOM_UP_ENUMERABLE = "0x8318b539"
    ERC998_ERC20_TOP_DOWN = "0x7294ffed"
    ERC998_ERC20_TOP_DOWN_ENUMERABLE = "0xc5fd96cd"
    ERC998_ERC20_BOTTOM_UP = "0xffafa991"
    ERC1155 = "0xd9b67a26"
    ERC1155_TOKEN_RECEIVER = "0x4e2312e0"
    ERC1155_METADATA_URI = "0x0e89341c"

    @property
    def bytes(self) -> bytes:
        return unhexlify(self.value[2:])

    @classmethod
    def from_value(cls, value: str):
        for item in cls:
            if item.value == value:
                return item


class HexInt:
    def __init__(self, hex_str: str) -> None:
        assert isinstance(hex_str, str)
        self.__hex_str: str = hex_str
        self.__int_value: Union[int, None] = None

    def __eq__(self, o: object) -> bool:
        return isinstance(o, self.__class__) and o.hex_value == self.__hex_str

    def __str__(self) -> str:
        return self.__hex_str

    def __hash__(self) -> int:
        return self.int_value

    def __repr__(self):
        return self.__class__.__name__ + ": " + self.__hex_str

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


class Block(Type):
    def __init__(
        self,
        *,
        number: str,
        hash: str,
        parent_hash: str,
        nonce: str,
        sha3_uncles: str,
        logs_bloom: str,
        transactions_root: str,
        state_root: str,
        receipts_root: str,
        miner: str,
        mix_hash: str,
        difficulty: str,
        total_difficulty: str,
        extra_data: str,
        size: str,
        gas_limit: str,
        gas_used: str,
        timestamp: str,
        transactions: List[str],
        uncles: List[str],
    ):
        self.__number = HexInt(number)
        self.__hash = hash
        self.__parent_hash = parent_hash
        self.__nonce = nonce
        self.__sha3_uncles = sha3_uncles
        self.__logs_bloom = HexInt(logs_bloom)
        self.__transactions_root = transactions_root
        self.__state_root = state_root
        self.__receipts_root = receipts_root
        self.__miner = miner
        self.__mix_hash = mix_hash
        self.__difficulty = HexInt(difficulty)
        self.__total_difficulty = HexInt(total_difficulty)
        self.__extra_data = extra_data
        self.__size = HexInt(size)
        self.__gas_limit = HexInt(gas_limit)
        self.__gas_used = HexInt(gas_used)
        self.__timestamp = HexInt(timestamp)
        self.__transactions = transactions.copy()
        self.__uncles = uncles.copy()

    def __eq__(self, o: object) -> bool:
        return (
            isinstance(o, self.__class__)
            and self.number == o.number
            and self.hash == o.hash
            and self.parent_hash == o.parent_hash
            and self.nonce == o.nonce
            and self.sha3_uncles == o.sha3_uncles
            and self.logs_bloom == o.logs_bloom
            and self.transactions_root == o.transactions_root
            and self.state_root == o.state_root
            and self.receipts_root == o.receipts_root
            and self.miner == o.miner
            and self.mix_hash == o.mix_hash
            and self.difficulty == o.difficulty
            and self.total_difficulty == o.total_difficulty
            and self.extra_data == o.extra_data
            and self.size == o.size
            and self.gas_limit == o.gas_limit
            and self.gas_used == o.gas_used
            and self.timestamp == o.timestamp
            and self.transactions == o.transactions
            and self.uncles == o.uncles
        )

    def __repr__(self) -> str:
        return (
            str(self.__class__.__name__)
            + ": "
            + repr(
                {
                    "number": self.number,
                    "hash": self.hash,
                    "parent_hash": self.parent_hash,
                    "nonce": self.nonce,
                    "sha3_uncles": self.sha3_uncles,
                    "logs_bloom": self.logs_bloom,
                    "transactions_root": self.transactions_root,
                    "state_root": self.state_root,
                    "receipts_root": self.receipts_root,
                    "miner": self.miner,
                    "mix_hash": self.mix_hash,
                    "difficulty": self.difficulty,
                    "total_difficulty": self.total_difficulty,
                    "extra_data": self.extra_data,
                    "size": self.size,
                    "gas_limit": self.gas_limit,
                    "gas_used": self.gas_used,
                    "timestamp": self.timestamp,
                    "transactions": self.transactions,
                    "uncles": self.uncles,
                }
            )
        )

    @property
    def number(self) -> HexInt:
        return self.__number

    @property
    def hash(self) -> str:
        return self.__hash

    @property
    def parent_hash(self) -> str:
        return self.__parent_hash

    @property
    def nonce(self) -> str:
        return self.__nonce

    @property
    def sha3_uncles(self) -> str:
        return self.__sha3_uncles

    @property
    def logs_bloom(self) -> HexInt:
        return self.__logs_bloom

    @property
    def transactions_root(self) -> str:
        return self.__transactions_root

    @property
    def state_root(self) -> str:
        return self.__state_root

    @property
    def receipts_root(self) -> str:
        return self.__receipts_root

    @property
    def miner(self) -> str:
        return self.__miner

    @property
    def mix_hash(self) -> str:
        return self.__mix_hash

    @property
    def difficulty(self) -> HexInt:
        return self.__difficulty

    @property
    def total_difficulty(self) -> HexInt:
        return self.__total_difficulty

    @property
    def extra_data(self) -> str:
        return self.__extra_data

    @property
    def size(self) -> HexInt:
        return self.__size

    @property
    def gas_limit(self) -> HexInt:
        return self.__gas_limit

    @property
    def gas_used(self) -> HexInt:
        return self.__gas_used

    @property
    def transactions(self) -> List[str]:
        return self.__transactions[:]

    @property
    def timestamp(self) -> HexInt:
        return self.__timestamp

    @property
    def uncles(self) -> List[str]:
        return self.__uncles.copy()


class Log(Type):
    def __init__(
        self,
        *,
        removed: bool,
        log_index: str,
        transaction_index: str,
        transaction_hash: str,
        block_hash: str,
        block_number: str,
        address: Optional[str],
        data: str,
        topics: List[str],
    ) -> None:
        self.__removed = removed
        self.__log_index = HexInt(log_index)
        self.__transaction_index = HexInt(transaction_index)
        self.__transaction_hash = transaction_hash
        self.__block_hash = block_hash
        self.__block_number = HexInt(block_number)
        self.__address = address
        self.__data = data
        self.__topics = topics.copy()

    def __eq__(self, o: object) -> bool:
        return (
            isinstance(o, self.__class__)
            and self.removed == o.removed
            and self.log_index == o.log_index
            and self.transaction_index == o.transaction_index
            and self.transaction_hash == o.transaction_hash
            and self.block_hash == o.block_hash
            and self.block_number == o.block_number
            and self.address == o.address
            and self.data == o.data
            and self.topics == o.topics
        )

    def __repr__(self) -> str:
        return (
            str(self.__class__.__name__)
            + {
                "removed": self.__removed,
                "log_index": self.__log_index,
                "transaction_index": self.__transaction_index,
                "transaction_hash": self.__transaction_hash,
                "block_hash": self.__block_hash,
                "block_number": self.__block_number,
                "address": self.__address,
                "data": self.__data,
                "topics": self.__topics,
            }.__repr__()
        )

    @property
    def removed(self) -> bool:
        return self.__removed

    @property
    def log_index(self) -> HexInt:
        return self.__log_index

    @property
    def transaction_index(self) -> HexInt:
        return self.__transaction_index

    @property
    def transaction_hash(self) -> str:
        return self.__transaction_hash

    @property
    def block_hash(self) -> str:
        return self.__block_hash

    @property
    def block_number(self) -> HexInt:
        return self.__block_number

    @property
    def address(self) -> Optional[str]:
        return self.__address

    @property
    def data(self) -> str:
        return self.__data

    @property
    def topics(self) -> List[str]:
        return self.__topics.copy()


class TransactionReceipt(Type):
    def __init__(
        self,
        *,
        transaction_hash: str,
        transaction_index: str,
        block_hash: str,
        block_number: str,
        from_: str,
        to_: Optional[str],
        cumulative_gas_used: str,
        gas_used: str,
        contract_address: Optional[str],
        logs: List[Log],
        logs_bloom: str,
        root: Optional[str],
        status: str,
    ) -> None:
        self.__transaction_hash = transaction_hash
        self.__transaction_index = HexInt(transaction_index)
        self.__block_hash = block_hash
        self.__block_number = HexInt(block_number)
        self.__from_ = from_
        self.__to_ = to_
        self.__cumulative_gas_used = HexInt(cumulative_gas_used)
        self.__gas_used = HexInt(gas_used)
        self.__contract_address = contract_address
        self.__logs = logs.copy()
        self.__logs_bloom = HexInt(logs_bloom)
        self.__root = root
        self.__status = None if status is None else HexInt(status)

    def __repr__(self) -> str:
        return (
            str(self.__class__.__name__)
            + {
                "transaction_hash ": self.__transaction_hash,
                "transaction_index": self.__transaction_index,
                "block_hash": self.__block_hash,
                "block_number": self.__block_number,
                "from_": self.__from_,
                "to_": self.__to_,
                "cumulative_gas_used": self.__cumulative_gas_used,
                "gas_used": self.__gas_used,
                "contract_address": self.__contract_address,
                "logs": self.__logs,
                "logs_bloom": self.__logs_bloom,
                "root": self.__root,
                "status": self.__status,
            }.__repr__()
        )

    def __eq__(self, o: object) -> bool:
        return (
            isinstance(o, self.__class__)
            and self.transaction_hash == o.transaction_hash
            and self.transaction_index == o.transaction_index
            and self.block_hash == o.block_hash
            and self.block_number == o.block_number
            and self.from_ == o.from_
            and self.to_ == o.to_
            and self.cumulative_gas_used == o.cumulative_gas_used
            and self.gas_used == o.gas_used
            and self.contract_address == o.contract_address
            and self.logs == o.logs
            and self.logs_bloom == o.logs_bloom
            and self.root == o.root
            and self.transaction_hash == o.transaction_hash
            and self.status == o.status
        )

    @property
    def transaction_hash(self) -> str:
        return self.__transaction_hash

    @property
    def transaction_index(self) -> HexInt:
        return self.__transaction_index

    @property
    def block_hash(self) -> str:
        return self.__block_hash

    @property
    def block_number(self) -> HexInt:
        return self.__block_number

    @property
    def from_(self) -> str:
        return self.__from_

    @property
    def to_(self) -> Optional[str]:
        return self.__to_

    @property
    def cumulative_gas_used(self) -> HexInt:
        return self.__cumulative_gas_used

    @property
    def gas_used(self) -> HexInt:
        return self.__gas_used

    @property
    def contract_address(self) -> Optional[str]:
        return self.__contract_address

    @property
    def logs(self) -> List[Log]:
        return self.__logs.copy()

    @property
    def logs_bloom(self) -> HexInt:
        return self.__logs_bloom

    @property
    def root(self) -> Optional[str]:
        return self.__root

    @property
    def status(self) -> HexInt:
        return self.__status


class Metadata:  # pragma: no cover
    pass


class ERC1155Metadata(Metadata):  # pragma: no cover
    def __init__(
        self, name: str, description: str, image: str, properties: Dict[str, object]
    ) -> None:
        pass


class Token:  # pragma: no cover
    def __init__(
        self,
        *,
        token_id: int,
        owner: str,
        metadata_uri: str,
        raw_metadata: str,
        metadata: Metadata,
    ) -> None:
        self.__token_id: int = token_id
        self.__owner = owner
        self.__metadata_uri = metadata_uri
        self.__raw_metadata: str = raw_metadata
        self.__metadata: Metadata = metadata

    @property
    def token_id(self) -> int:
        return self.__token_id

    @property
    def owner(self) -> str:
        return self.__owner

    @property
    def metadata_uri(self) -> str:
        return self.__metadata_uri

    @property
    def raw_metadata(self) -> str:
        return self.__raw_metadata

    @property
    def metadata(self) -> Metadata:
        return self.__metadata


class Contract:  # pragma: no cover
    def __init__(
        self,
        *,
        address: str,
        creator: str,
        interfaces: List[ERC165InterfaceID],
        name: Union[str, None],
        symbol: Union[str, None],
        total_supply: Union[int, None],  # TODO: Needs to be HexInt
    ) -> None:
        self.__address: str = address
        self.__creator: str = creator
        self.__interfaces: List[ERC165InterfaceID] = interfaces
        self.__name: Optional[str] = name
        self.__symbol: Optional[str] = symbol
        self.__total_supply: Optional[int] = total_supply

    @property
    def address(self) -> str:
        return self.__address

    @property
    def creator(self) -> str:
        return self.__creator

    @property
    def interfaces(self) -> List[ERC165InterfaceID]:
        return self.__interfaces

    @property
    def name(self) -> Optional[str]:
        return self.__name

    @property
    def symbol(self) -> Optional[str]:
        return self.__symbol

    @property
    def total_supply(self) -> Optional[int]:
        return self.__total_supply
