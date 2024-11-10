from datetime import date
from typing import List, Sequence, Iterable

from loguru import logger
from sqlalchemy import select, insert, delete, Table

from .. import EventInfo, DataType, KlineEvent, MessageType, OrderbookEvent, TradeEvent, PriceVolume
from .db import Database
from .orm import Downloaded, get_kline_columns, get_trades_columns, get_orderbook_columns
from ..utils import chunks


class Service:

    def __init__(self, database: Database):
        self.database = database

    @property
    def metadata(self):
        return self.database.get_metadata()

    def is_downloaded(self, info: EventInfo, day: date) -> bool:
        # period = info.period if info.period is not None else 0

        stmt = (select(Downloaded)
                .where(Downloaded.symbol == info.symbol)  # noqa
                .where(Downloaded.type == info.type)
                .where(Downloaded.day == day))

        if info.period:
            stmt = stmt.where(Downloaded.period == info.period)

        with self.database.new_session() as session:
            downloaded = session.execute(stmt).one_or_none()
            session.commit()
            return downloaded is not None

    def set_downloaded(self, info: EventInfo, day: date, value: bool):
        period = info.period if info.period else 0

        if value:
            stmt = insert(Downloaded).values(
                {
                    'symbol': info.symbol,
                    'type': info.type,
                    'period': period,
                    'day': day,
                }
            )
        else:
            stmt = (delete(Downloaded)
                    .where(Downloaded.symbol == info.symbol)  # noqa
                    .where(Downloaded.type == info.type)
                    .where(Downloaded.period == period)
                    .where(Downloaded.day == day))

        with self.database.new_session() as session:
            session.execute(stmt)
            session.commit()

    def save(self, info: EventInfo, items: List[Sequence | OrderbookEvent]):
        logger.info(f'Saving {info} length {len(items)}...')

        match info.type:
            case DataType.KLINE | DataType.TRADE:
                table = self.get_table(info)
                self._save_many(table, items)
            case DataType.ORDERBOOK:
                self._save_orderbook(info, items)
            case _:
                raise NotImplementedError

        logger.info(f'Saved {info}')

    def _save_many(self, table: Table, items: list):
        with self.database.new_session() as session:
            for chunk in chunks(items, 1000):
                # TODO dont use ignore
                stmt = table.insert().values(chunk).prefix_with('IGNORE')
                session.execute(stmt)
                session.commit()

    def _save_orderbook(self, info: EventInfo, items: List[OrderbookEvent]):
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
            table = self.get_table(info, name)
            items = self._orderbook_events_to_items(events)
            self._save_many(table, list(items))

    @staticmethod
    def _orderbook_events_to_items(events: Iterable[OrderbookEvent]) -> Iterable[Sequence]:
        for event in events:
            for i, ask in enumerate(event.asks, start=1):
                yield event.timestamp, ask.price, True, ask.volume, i
            for i, bid in enumerate(event.bids, start=1):
                yield event.timestamp, bid.price, False, bid.volume, -i

    def get(self, info: EventInfo, start: int, end: int) -> List[TradeEvent | OrderbookEvent | KlineEvent]:
        logger.info(f'Loading from database {info}, start={start}, end={end}...')

        if info.type == DataType.ORDERBOOK:
            return self.get_orderbook(info, start, end)

        table_name = self.get_table_name(info)

        if table_name not in self.metadata.tables:
            return []

        table = self.metadata.tables[table_name]
        start = int(start / 1000)
        end = int(end / 1000)

        match info.type:
            case DataType.KLINE:
                return self.get_candles(table, start, end)
            case DataType.TRADE:
                return self.get_trades(table, start, end)
            case _:
                raise NotImplementedError

    def get_orderbook(self, info: EventInfo, start: int, end: int) -> List[OrderbookEvent]:
        snapshot_table = self.get_table(info, 'snapshot')
        delta_table = self.get_table(info, 'delta')

        snapshot = dict()
        time = self._get_closest_snapshot_time(snapshot_table, start)
        self._fill_snapshot(snapshot_table, snapshot, time)
        self._fill_snapshot(delta_table, snapshot, time, start)

        event = OrderbookEvent(time, MessageType.SNAPSHOT, [], [])

        for price, (side, volume) in snapshot.items():
            price_volume = PriceVolume(price, volume)

            if side == 1:
                event.asks.append(price_volume)
            else:
                event.bids.append(price_volume)

        events = [event]
        events += self._get_orderbook_delta(delta_table, start, end)
        return events

    def _get_closest_snapshot_time(self, table: Table, time: int) -> int:
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

    @staticmethod
    def _modify_snapshot(snapshot: dict, rows: Iterable[Sequence]):
        for row in rows:
            if row[2] <= 0:
                if row[0] in snapshot:
                    del snapshot[row[0]]
            else:
                snapshot[row[0]] = (row[1], row[2])

    def _get_orderbook_delta(self, table: Table, start: int, end: int) -> List[OrderbookEvent]:
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
                        yield OrderbookEvent(time, MessageType.DELTA, asks, bids)
                    asks = []
                    bids = []
                    time = row[0]

                price_volume = PriceVolume(row[1], row[3])
                if row[2] == 1:
                    asks.append(price_volume)
                else:
                    bids.append(price_volume)

            if asks or bids:
                yield OrderbookEvent(time, MessageType.DELTA, asks, bids)

    def get_candles(self, table: Table, start: int, end: int) -> List[KlineEvent]:
        stmt = (
            select(
                table.c.time * 1000,
                table.c.start_time * 1000,
                table.c.open,
                table.c.high,
                table.c.low,
                table.c.close,
                table.c.volume,
            )
            .where(table.c.start_time >= start)  # noqa
            .where(table.c.start_time <= end)
        )

        with self.database.new_session() as session:
            cursor = session.execute(stmt)
            return [KlineEvent(*row) for row in cursor]

    def get_trades(self, table: Table, start: float, end: float) -> List[Sequence | TradeEvent]:
        stmt = (
            select(
                # TODO fix date conversion
                table.c.time * 1000,
                table.c.side,
                table.c.size,
                table.c.price,
            )
            .where(table.c.time >= start)  # noqa
            .where(table.c.time <= end)
        )

        with self.database.new_session() as session:
            cursor = session.execute(stmt)
            session.commit()
            return [TradeEvent(
                timestamp=float(row[0]),
                is_buy=row[1],
                size=row[2],
                price=row[3]
            ) for row in cursor]

    def get_table(self, info: EventInfo, ext: str = None) -> Table:
        table_name = self.get_table_name(info, ext)

        if table_name not in self.metadata.tables:
            if info.type == DataType.KLINE:
                columns = get_kline_columns()
            elif info.type == DataType.TRADE:
                columns = get_trades_columns()
            elif info.type == DataType.ORDERBOOK:
                columns = get_orderbook_columns()
            else:
                raise NotImplementedError

            logger.info(f'Creating table {table_name}...')
            Table(table_name, self.metadata, *columns)
            self.metadata.create_all(bind=Database.get_engine())
            logger.info(f'Created table {table_name}')

        return self.metadata.tables[table_name]

    @staticmethod
    def get_table_name(info: EventInfo, ext: str = None):
        symbol = info.symbol.lower().replace('.', '_')

        if info.type == DataType.KLINE:
            return f'kline_{info.period}_{symbol}'

        if info.type == DataType.TRADE:
            return f'trade_{symbol}'

        if info.type == DataType.ORDERBOOK:
            if ext is None:
                raise ValueError('ext is required for orderbook')

            return f'orderbook_{ext}_{symbol}'

        raise NotImplementedError
