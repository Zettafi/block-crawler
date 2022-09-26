from blockrail.blockcrawler.core.entities import HexInt
from blockrail.blockcrawler.evm.types import EVMLog


class LogVersionOracle:
    @classmethod
    def version_from_log(cls, log: EVMLog) -> HexInt:
        return cls.version(
            log.block_number.int_value,
            log.transaction_index.int_value,
            log.log_index.int_value,
        )

    @staticmethod
    def version(block_number: int, transaction_index: int, log_index: int) -> HexInt:
        version = HexInt(block_number * 1_000_000_000 + transaction_index * 10_000 + log_index)
        return HexInt("0x" + version.hex_value[2:].zfill(40))
