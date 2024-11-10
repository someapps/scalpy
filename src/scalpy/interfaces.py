from abc import ABC, abstractmethod
from typing import AsyncGenerator, List, Iterable

from .enums import DataType
from .items import EventInfo, MarketRequest, Event, Signal, StreamItem, Order, Advise


class History(ABC):

    @abstractmethod
    async def get(self, info: EventInfo, start: int, end: int) -> AsyncGenerator[Event]:
        ...


class MarketIterator(ABC):

    def __init__(self):
        self._requests = set()
        self._sources = set()

    def __aiter__(self):
        return self

    def subscribe(self, request: MarketRequest):
        self._requests.add(request)
        self._sources.add(request.info)

    @abstractmethod
    async def __anext__(self) -> Event:
        ...

    @abstractmethod
    async def run(self):
        ...


class Handler(ABC):

    def __init__(self, requests: List[MarketRequest]):
        self._requests = requests

    @property
    def requests(self) -> List[MarketRequest]:
        return self._requests


class TradeConverter(Handler):

    def __init__(self, symbol: str):
        super().__init__([MarketRequest(EventInfo(symbol, DataType.TRADE))])

    def on_preload_trades(self, events: List[Event]) -> Iterable[Event]:
        for event in events:
            yield self.on_trade(event)

    @abstractmethod
    def on_trade(self, event: Event) -> Iterable[Event]:
        ...


class EventHandler(Handler):

    def on_preload_events(self, events: List[Event]) -> Iterable[StreamItem]:
        for event in events:
            yield self.on_event(event)

    @abstractmethod
    def on_event(self, event: Event) -> Iterable[StreamItem]:
        ...


class SignalHandler(EventHandler, ABC):

    def __init__(
            self,
            requests: List[MarketRequest],
            handlers: List[TradeConverter | EventHandler]
    ):
        super().__init__(requests)

        for handler in handlers:
            assert isinstance(handler, (TradeConverter, EventHandler)), (
                f'{handler} must be TradeConverter or EventHandler')

        self._handlers = handlers

    @property
    def handlers(self) -> List[TradeConverter | EventHandler]:
        return self._handlers

    def on_preload_signals(self, signals: List[Signal]) -> Iterable[StreamItem]:
        ...

    @abstractmethod
    def on_signal(self, signal: Signal) -> Iterable[Advise | Order]:
        ...


class AdviseHandler(SignalHandler, ABC):

    @abstractmethod
    def on_advise(self, advise: Advise) -> Iterable[Order]:
        ...
