from typing import List, Sequence, Iterable

from loguru import logger
from pendulum import Interval
from sqlalchemy import select, Table

from scalpy import (
    EventInfo,
    DataType,
    MessageType,
    Orderbook,
    PriceVolume,
    Trade,
    OHLC,
)
from scalpy.utils import chunks
from .db import Database
from .table_manager import TableManager


class MarketService:

    def __init__(self, database: Database):
        self.database = database
        self.table_manager = TableManager(database)

    def save(self, info: EventInfo, items: List[Trade | Orderbook | OHLC]):
        logger.info(f'Saving {info} length {len(items)}...')

        match info.type:
            case DataType.TRADE:
                self.save_trades(info, items)
            case DataType.KLINE:
                self.save_kline(info, items)
            case DataType.ORDERBOOK:
                self.save_orderbook(info, items)
            case _:
                raise NotImplementedError

        logger.info(f'Saved {info}')

    def save_trades(self, info: EventInfo, items: List[Trade]):
        table = self.table_manager.get_table(info)
        mult = self._get_time_multiplier(info.type)

        items = [
            {
                'time': int(item.timestamp * mult),
                'side': item.is_buy,
                'price': item.price,
                'volume': item.size,
                'trade_id': item.trade_id,
            } for item in items
        ]

        self._save_many(table, items)

    def save_kline(self, info: EventInfo, items: List[OHLC]):
        table = self.table_manager.get_table(info)
        mult = self._get_time_multiplier(info.type)

        items = [
            {
                'time': int(item.timestamp * mult),
                'start_time': int(item.start_timestamp * mult),
                'open': item.open,
                'high': item.high,
                'low': item.low,
                'close': item.close,
                'volume': item.volume,
                'turnover': item.turnover,
            }
            for item in items
        ]

        self._save_many(table, items)

    def _save_many(self, table: Table, items: list):
        with self.database.new_session() as session:
            for chunk in chunks(items, 1000):
                stmt = table.insert().values(chunk)
                session.execute(stmt)
                session.commit()

    # TODO refactor
    def save_orderbook(self, info: EventInfo, items: List[Orderbook]):
        snapshots = []
        deltas = []

        for item in items:
            if item.type == MessageType.SNAPSHOT:
                snapshots.append(item)
            elif item.type == MessageType.DELTA:
                deltas.append(item)
            else:
                raise NotImplementedError

        for name, events in [('snapshot', snapshots), ('delta', deltas)]:
            table = self.table_manager.get_table(info, name)
            items = self._orderbook_events_to_items(events)
            self._save_many(table, list(items))

    # TODO refactor
    @staticmethod
    def _orderbook_events_to_items(events: Iterable[Orderbook]) -> Iterable[Sequence]:
        for event in events:
            for i, ask in enumerate(event.asks, start=1):
                yield event.timestamp, ask.price, True, ask.volume, i
            for i, bid in enumerate(event.bids, start=1):
                yield event.timestamp, bid.price, False, bid.volume, -i

    def get(self, info: EventInfo, interval: Interval) -> List[Trade | Orderbook | OHLC]:
        logger.info(f'Loading from database {info} for {interval}...')

        if not self.table_manager.is_table_exists(info):
            return []

        match info.type:
            case DataType.KLINE:
                return self.get_kline(info, interval)
            case DataType.TRADE:
                return self.get_trades(info, interval)
            # case DataType.ORDERBOOK:
            #     return self.get_orderbook(info, interval.start, interval.end)
            case _:
                raise NotImplementedError

    def get_trades(self, info: EventInfo, interval: Interval) -> List[Trade]:
        mult = self._get_time_multiplier(info.type)
        items = self._get_many(info, interval)

        return [Trade(
            timestamp=float(item[0] / mult),
            producer_id=id(self),
            is_buy=item[1],
            size=item[2],
            price=item[3],
            trade_id=item[4]
        ) for item in items]

    def get_kline(self, info: EventInfo, interval: Interval) -> List[OHLC]:
        mult = self._get_time_multiplier(info.type)
        items = self._get_many(info, interval)

        return [OHLC(
            timestamp=float(item[0] / mult),
            producer_id=id(self),
            start_timestamp=float(item[1] / mult),
            open=item[2],
            high=item[3],
            low=item[4],
            close=item[5],
            volume=item[6],
            turnover=item[7]
        ) for item in items]

    def _get_many(self, info: EventInfo, interval: Interval) -> List[tuple]:
        mult = self._get_time_multiplier(info.type)
        start = int(interval.start.float_timestamp * mult)
        end = int(interval.end.float_timestamp * mult)
        table = self.table_manager.get_table(info)

        stmt = (
            table.select()
            .where(table.c.time >= start)  # noqa
            .where(table.c.time <= end)
        )

        with self.database.new_session() as session:
            cursor = session.execute(stmt)
            session.commit()
            return [tuple(row) for row in cursor]

    # TODO refactor
    def get_orderbook(self, info: EventInfo, start: int, end: int) -> List[Orderbook]:
        snapshot_table = self.table_manager.get_table(info, 'snapshot')
        delta_table = self.table_manager.get_table(info, 'delta')

        snapshot = dict()
        time = self._get_closest_snapshot_time(snapshot_table, start)
        self._fill_snapshot(snapshot_table, snapshot, time)
        self._fill_snapshot(delta_table, snapshot, time, start)

        event = Orderbook(time, MessageType.SNAPSHOT, [], [])

        for price, (side, volume) in snapshot.items():
            price_volume = PriceVolume(price, volume)

            if side == 1:
                event.asks.append(price_volume)
            else:
                event.bids.append(price_volume)

        events = [event]
        events += self._get_orderbook_delta(delta_table, start, end)
        return events

    # TODO refactor
    def _get_closest_snapshot_time(self, table: Table,  time: int) -> int:
        stmt_time = (
            select(table.c.time)
            .where(table.c.time <= time)
            .order_by(table.c.time.desc())
            .limit(1)
        )

        with self.database.new_session() as session:
            cursor = session.execute(stmt_time)

            time = cursor.scalar_one_or_none()
            assert time is not None, 'No snapshot found'
        return int(time)

    # TODO refactor
    def _fill_snapshot(self, table: Table, snapshot: dict, snapshot_time: int, start: int = None) -> dict:
        is_first = len(snapshot) == 0

        stmt_snapshot = (
            select(
                table.c.price,
                table.c.side,
                table.c.volume,
            )
        )

        if is_first:
            stmt_snapshot = (stmt_snapshot
                             .where(table.c.time == snapshot_time))
        else:
            stmt_snapshot = (stmt_snapshot
                             .where(table.c.time > snapshot_time)
                             .where(table.c.time < start))

        with self.database.new_session() as session:
            cursor = session.execute(stmt_snapshot)
            self._modify_snapshot(snapshot, cursor)

        return snapshot

    # TODO refactor
    @staticmethod
    def _modify_snapshot(snapshot: dict, rows: Iterable[Sequence]):
        for row in rows:
            if row[2] <= 0:
                if row[0] in snapshot:
                    del snapshot[row[0]]
            else:
                snapshot[row[0]] = (row[1], row[2])

    # TODO refactor
    def _get_orderbook_delta(self, table: Table, start: int, end: int) -> List[Orderbook]:
        stmt = (
            select(
                table.c.time,
                table.c.price,
                table.c.side,
                table.c.size,
            )
            .where(table.c.time >= start)  # noqa
            .where(table.c.time <= end)
            .order_by(table.c.time.asc())
        )

        asks = []
        bids = []
        time = None

        with self.database.new_session() as session:
            cursor = session.execute(stmt)

            for row in cursor:
                if row[0] != time:
                    if time is not None:
                        yield Orderbook(time, MessageType.DELTA, asks, bids)
                    asks = []
                    bids = []
                    time = row[0]

                price_volume = PriceVolume(row[1], row[3])
                if row[2] == 1:
                    asks.append(price_volume)
                else:
                    bids.append(price_volume)

            if asks or bids:
                yield Orderbook(time, MessageType.DELTA, asks, bids)

    @staticmethod
    def _get_time_multiplier(type_: DataType) -> float:
        match type_:
            case DataType.TRADE:
                return 1_000_000
            case DataType.ORDERBOOK:
                return 1_000_000
            case DataType.KLINE:
                return 0.001
            case _:
                raise NotImplementedError
