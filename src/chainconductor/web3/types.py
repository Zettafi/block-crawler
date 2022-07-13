from typing import List, Union, NewType

MethodParams = NewType(
    "MethodParams", List[Union["CallMethodParams", int, str, bool, float, bytes]]
)


class HexInt:
    def __init__(self, hex_str: str) -> None:
        assert isinstance(hex_str, str)
        self.__hex_str = hex_str
        self.__int_value = None

    def __eq__(self, o: object) -> bool:
        return isinstance(o, self.__class__) and o.hex_value == self.__hex_str

    def __str__(self) -> str:
        return self.__hex_str

    def __hash__(self) -> int:
        return self.int_value

    @property
    def hex_value(self) -> str:
        return self.__hex_str

    @property
    def int_value(self) -> int:
        if self.__int_value is None:
            self.__int_value = int(self.__hex_str[2:], 16)
        return self.__int_value


class Type:
    pass


class Transaction(Type):
    def __init__(
        self,
        *,
        hash: str,
        block_hash: str,
        block_number: str,
        from_: str,
        gas: str,
        gas_price: str,
        input: str,
        nonce: str,
        to_: str,
        transaction_index: str,
        value: str,
        v: str,
        r: str,
        s: str,
    ) -> None:
        self.__block_hash = block_hash
        self.__block_number = HexInt(block_number)
        self.__from_ = from_
        self.__gas = HexInt(gas)
        self.__gas_price = HexInt(gas_price)
        self.__hash = hash
        self.__input = input
        self.__nonce = HexInt(nonce)
        self.__to_ = to_
        self.__transaction_index = HexInt(transaction_index)
        self.__value = HexInt(value)
        self.__v = HexInt(v)
        self.__r = HexInt(r)
        self.__s = HexInt(s)

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
    def gas(self) -> HexInt:
        return self.__gas

    @property
    def gas_price(self) -> HexInt:
        return self.__gas_price

    @property
    def hash(self) -> str:
        return self.__hash

    @property
    def input(self) -> str:
        return self.__input

    @property
    def nonce(self) -> HexInt:
        return self.__nonce

    @property
    def to_(self) -> str:
        return self.__to_

    @property
    def transaction_index(self) -> HexInt:
        return self.__transaction_index

    @property
    def value(self) -> HexInt:
        return self.__value

    @property
    def v(self) -> HexInt:
        return self.__v

    @property
    def r(self) -> HexInt:
        return self.__r

    @property
    def s(self) -> HexInt:
        return self.__s


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
        difficulty: str,
        total_difficulty: str,
        extra_data: str,
        size: str,
        gas_limit: str,
        gas_used: str,
        timestamp: str,
        transactions: List[Union[Transaction, str]],
        uncles: List[str],
    ):
        self.__number = HexInt(number)
        self.__hash = hash
        self.__parent_hash = parent_hash
        self.__nonce = nonce
        self.__sha3_uncles = sha3_uncles
        self.__logs_bloom = logs_bloom
        self.__transactions_root = transactions_root
        self.__state_root = state_root
        self.__receipts_root = receipts_root
        self.__miner = miner
        self.__difficulty = HexInt(difficulty)
        self.__total_difficulty = total_difficulty
        self.__extra_data = extra_data
        self.__size = HexInt(size)
        self.__gas_limit = HexInt(gas_limit)
        self.__gas_used = HexInt(gas_used)
        self.__timestamp = HexInt(timestamp)
        self.__transactions = transactions.copy()
        self.__uncles = uncles

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
    def logs_bloom(self) -> str:
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
    def difficulty(self) -> HexInt:
        return self.__difficulty

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
    def transactions(self) -> List[Union[Transaction, str]]:
        return self.__transactions.copy()

    @property
    def timestamp(self) -> HexInt:
        return self.__timestamp


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
        address: str,
        data: str,
        topics: List[str],
    ):
        self.__removed = removed
        self.__log_index = HexInt(log_index)
        self.__transaction_index = HexInt(transaction_index)
        self.__transaction_hash = transaction_hash
        self.__block_hash = block_hash
        self.__block_number = HexInt(block_number)
        self.__address = address
        self.__data = data
        self.__topics = topics.copy()

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
    def address(self) -> str:
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
        to_: str,
        cumulative_gas_used: str,
        gas_used: str,
        contract_address: str,
        logs: List[Log],
        logs_bloom: str,
        root: str,
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
        self.__logs_bloom = logs_bloom
        self.__root = root
        self.__status = None if status is None else HexInt(status)

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
    def to_(self) -> str:
        return self.__to_

    @property
    def cumulative_gas_used(self) -> HexInt:
        return self.__cumulative_gas_used

    @property
    def gas_used(self) -> HexInt:
        return self.__gas_used

    @property
    def contract_address(self) -> str:
        return self.__contract_address

    @property
    def logs(self):
        return self.__logs.copy()

    @property
    def logs_bloom(self) -> str:
        return self.__logs_bloom

    @property
    def root(self) -> str:
        return self.__root

    @property
    def status(self) -> HexInt:
        return self.__status


class SmartContract(Type):
    def __init__(self):
        pass


class ERC20(SmartContract):
    pass


class ERC171(ERC20):
    pass
