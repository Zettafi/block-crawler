import dataclasses
from typing import Optional

from blockcrawler.core.types import HexInt


@dataclasses.dataclass
class BlockBoundTracker:
    low: Optional[HexInt] = HexInt(0)
    high: Optional[HexInt] = HexInt(0)
