from dataclasses import dataclass
from datetime import timedelta

from .enums import DataType


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
    ...


@dataclass(frozen=True)
class Signal(StreamItem):
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
