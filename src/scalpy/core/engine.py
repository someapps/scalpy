from typing import List, Dict, Iterable, Callable

from ..interfaces import (
    MarketIterator,
    Handler,
    TradeConverter,
    EventHandler,
    SignalHandler,
    AdviseHandler
)
from ..items import EventInfo, Event, Signal, Order, StreamItem


class Engine:

    def __init__(
            self,
            preloader: MarketIterator,
            iterator: MarketIterator,
            handlers: List[Handler]
    ):
        self._preloader = preloader
        self._iterator = iterator
        self._handlers = handlers

        self._preload_trade_converters = dict()
        self._preload_event_handlers = dict()
        self._preload_signal_handlers = []

        self._trade_converters = dict()
        self._event_handlers = dict()
        self._signal_handlers = []
        self._advise_handlers = []

        self._analyze_handlers(handlers)

    def _analyze_handlers(self, handlers: List[Handler]) -> None:
        for handler in handlers:
            for request in handler.requests:
                if request.stream:
                    self._iterator.subscribe(request)

                    if issubclass(handler, TradeConverter):
                        if handler.on_trade != TradeConverter.on_trade:
                            self._trade_converters[request.info] = handler.on_trade

                    if issubclass(handler, EventHandler):
                        if handler.on_event != EventHandler.on_event:
                            self._event_handlers[request.info] = handler.on_event

                    if issubclass(handler, SignalHandler):
                        if handler.on_signal != SignalHandler.on_signal:
                            self._signal_handlers.append(handler.on_signal)
                            self._analyze_handlers(handler.handlers)  # noqa

                    if issubclass(handler, AdviseHandler):
                        if handler.on_advise != AdviseHandler.on_advise:
                            self._advise_handlers.append(handler.on_advise)
                            self._analyze_handlers(handler.handlers)  # noqa

                if request.preload:
                    self._preloader.subscribe(request)

                    if issubclass(handler, TradeConverter):
                        if handler.on_preload_trades != TradeConverter.on_preload_trades:
                            self._preload_trade_converters[request.info] = handler.on_preload_trades

                    if issubclass(handler, EventHandler):
                        if handler.on_preload_events != EventHandler.on_preload_events:
                            self._preload_event_handlers[request.info] = handler.on_preload_events

                    if issubclass(handler, SignalHandler):
                        if handler.on_preload_signals != SignalHandler.on_preload_signals:
                            self._preload_signal_handlers.append(handler.on_preload_signals)
                            self._analyze_handlers(handler.handlers)  # noqa

    async def _iterate_preloader(self) -> None:
        event_info = dict()

        async for event in self._preloader:
            event_info.setdefault(event.info, []).append(event)

        signals = self._preload_events(event_info)
        self._preload_signals(signals)

    def _preload_events(self, event_info: Dict[EventInfo, List[Event]]) -> List[Signal]:
        new_events = []
        signals = []

        for info, handle in self._preload_trade_converters.values():
            for events in event_info.get(info, []):
                new_events.extend(handle(events))

        for new_event in new_events:
            event_info.setdefault(new_event.info, []).append(new_event)

        for info, handle in self._preload_event_handlers.items():
            for events in event_info.get(info, []):
                signals.extend(handle(events))

        return signals

    def _preload_signals(self, signals: List[Signal]) -> None:
        for signal in signals:
            for handle in self._preload_signal_handlers:
                result = handle(signal)

                try:
                    list(result)  # execute if generator
                except TypeError:
                    pass

    async def _iterate(self) -> None:
        async for market_event in self._iterator:
            for order in self.get_orders(market_event):
                self._handle_order(order)

    def get_orders(self, market_event: Event) -> Iterable[Order]:
        for event in self._handle_trade_event(market_event):
            for signal in self._handle_event_by_type(event, self._event_handlers):
                for item in self._handle_item(signal, self._signal_handlers):
                    if isinstance(item, Order):
                        yield item
                        continue

                    yield from self._handle_item(item, self._advise_handlers)

    def _handle_trade_event(self, event: Event) -> Iterable[Event]:
        yield event
        yield from self._handle_event_by_type(event, self._trade_converters)

    @staticmethod
    def _handle_item(
            item: StreamItem,
            item_handlers: List[Callable[[StreamItem], Iterable[StreamItem]]],
    ) -> Iterable[StreamItem]:
        for handle in item_handlers:
            yield from handle(item)

    @staticmethod
    def _handle_event_by_type(
            event: Event,
            item_handlers: Dict[EventInfo, List[Callable[[Event], Iterable[StreamItem]]]],
    ) -> Iterable[StreamItem]:
        if handlers := item_handlers.get(event.info):
            for handle in handlers:
                yield from handle(event)

    def _handle_order(self, order: Order) -> None:
        print(order)

    async def run(self):
        await self._preloader.run()
        await self._iterator.run()

        await self._iterate_preloader()
        await self._iterate()
