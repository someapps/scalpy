from dataclasses import dataclass
from datetime import timedelta
from typing import List

from .enums import DataType, MessageType


@dataclass(frozen=True)
class EventInfo:
    symbol: str
    type: DataType
    period: int = None

    def __hash__(self):
        return hash((self.symbol, self.type, self.period))


@dataclass(frozen=True)
class StreamItem:
    timestamp: float
    producer_id: int


@dataclass(frozen=True)
class Event(StreamItem):
    info: EventInfo


@dataclass(frozen=True)
class Signal(StreamItem):
    ...


@dataclass(frozen=True)
class Advise:
    ...


@dataclass(frozen=True)
class Order(StreamItem):
    ...


@dataclass(frozen=True)
class MarketRequest:
    info: EventInfo
    preload: timedelta | int | None = None
    stream: bool = True

    def __post_init__(self):
        assert self.preload or self.stream, 'Either preload or stream must be set'


@dataclass(frozen=True)
class OHLC(StreamItem):
    start_timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float = None
    turnover: float = None


@dataclass(frozen=True)
class Trade(StreamItem):
    is_buy: bool
    size: float
    price: float
    trade_id: str

    @property
    def side(self):
        return 'Buy' if self.is_buy else 'Sell'


@dataclass(frozen=True)
class PriceVolume:
    price: float
    volume: float


@dataclass(frozen=True)
class OrderbookEvent(StreamItem):
    type: MessageType
    asks: List[PriceVolume]
    bids: List[PriceVolume]
