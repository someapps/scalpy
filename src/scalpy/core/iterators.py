import asyncio
from typing import Iterable

import pendulum
from loguru import logger
from pendulum import Interval

from scalpy import EventInfo, Event, History, MarketIterator


class HistoryMixin(object):

    def __init__(self, history: History, interval: Interval):
        self._history = history
        self._interval = interval

    def _get_events(self, sources: Iterable[EventInfo]):
        events = []

        for source in sources:
            for item in self._history.get(source, self._interval):
                event = Event(
                    timestamp=item.timestamp,
                    producer_id=id(self),
                    info=source,
                    data=item
                )
                events.append(event)

        events.sort(key=lambda x: (x.data.timestamp, x.info.period))
        return events


class BacktestIterator(MarketIterator, HistoryMixin):

    def __init__(self, interval: Interval, history: History):
        MarketIterator.__init__(self)
        HistoryMixin.__init__(self, history, interval)

        self._ready_event = asyncio.Event()

        self._events = None
        self._history_iterator = None

    async def run(self):
        logger.info(f'Starting iterator for {self._interval}...')
        self._events = self._get_events(self._sources)
        self._history_iterator = iter(self._events)
        self._ready_event.set()
        logger.info(f'Iterator started with {len(self._events)} events.')

    async def __anext__(self) -> Event:
        if self._history_iterator is None:
            await self._ready_event.wait()

        try:
            return next(self._history_iterator)
        except StopIteration:
            self._history_iterator = iter(self._events)
            raise StopAsyncIteration


class ReplayIterator(BacktestIterator):

    def __init__(self, interval: Interval, history: History):
        super().__init__(interval, history)

        self._time_shift = None

    async def __anext__(self) -> Event:
        if self._history_iterator is None:
            await self._ready_event.wait()
            logger.info(f'Replay started with {len(self._events)} events.')

        try:
            next_event = next(self._history_iterator)
            next_time = next_event.data.timestamp
        except StopIteration:
            self._time_shift = None
            self._history_iterator = iter(self._events)
            raise StopAsyncIteration

        if self._time_shift is None:
            self._time_shift = pendulum.now().timestamp() * 1000 - next_time

        target_time = next_time + self._time_shift
        cur_time = pendulum.now().timestamp() * 1000

        sleep_time = float(target_time - cur_time) / 1000
        await asyncio.sleep(sleep_time)
        return next_event
