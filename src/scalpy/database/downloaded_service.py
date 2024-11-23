from datetime import date

from sqlalchemy import select, insert, delete

from scalpy import EventInfo, DataType
from .db import Database
from .orm import Downloaded


class DownloadedService:
    def __init__(self, database: Database):
        self.database = database

    def is_downloaded(self, info: EventInfo, day: date) -> bool:
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
        period = info.period if info.type == DataType.KLINE else 0

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
