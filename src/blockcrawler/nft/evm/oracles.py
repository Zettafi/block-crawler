from hexbytes import HexBytes

from blockcrawler.core.entities import HexInt
from blockcrawler.evm.types import EvmLog
from blockcrawler.evm.util import Erc721Events, Erc1155Events
from blockcrawler.nft.entities import TokenTransactionType


class LogVersionOracle:
    @classmethod
    def version_from_log(cls, log: EvmLog) -> HexInt:
        return cls.version_from_attributes(
            log.block_number.int_value,
            log.transaction_index.int_value,
            log.log_index.int_value,
        )

    @staticmethod
    def version_from_attributes(
        block_number: int, transaction_index: int, log_index: int
    ) -> HexInt:
        version = HexInt(block_number * 1_000_000_000 + transaction_index * 10_000 + log_index)
        return HexInt("0x" + version.hex_value[2:].zfill(40))


class TokenTransactionTypeOracle:
    ZERO_ADDRESS = HexBytes("0x0000000000000000000000000000000000000000000000000000000000000000")

    def type_from_log(self, log: EvmLog) -> TokenTransactionType:
        event = log.topics[0]
        if event == Erc721Events.TRANSFER.event_signature_hash:
            from_address = log.topics[1]
            to_address = log.topics[2]
        elif event in (
            Erc1155Events.TRANSFER_BATCH.event_signature_hash,
            Erc1155Events.TRANSFER_SINGLE.event_signature_hash,
        ):
            from_address = log.topics[2]
            to_address = log.topics[3]
        else:
            raise ValueError("Log must be for a known transaction type")

        contract_address = HexBytes("0x000000000000000000000000") + HexBytes(str(log.address))
        if to_address == self.ZERO_ADDRESS:
            transaction_type = TokenTransactionType.BURN
        elif from_address in (self.ZERO_ADDRESS, contract_address) and to_address not in (
            self.ZERO_ADDRESS,
            contract_address,
        ):
            transaction_type = TokenTransactionType.MINT
        else:
            transaction_type = TokenTransactionType.TRANSFER
        return transaction_type
