from typing import Iterable

from sqlalchemy import Column, Integer, Float, String, Date, Boolean, Enum, Double, SmallInteger
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase

from .. import DataType


class Base(AsyncAttrs, DeclarativeBase):
    ...


class Downloaded(Base):
    __tablename__ = 'downloaded'

    symbol = Column(String(100), primary_key=True)
    type = Column(Enum(DataType), primary_key=True)
    period = Column(Integer, primary_key=True, default=0)
    day = Column(Date, primary_key=True)


def get_kline_columns() -> Iterable[Column]:
    return (
        Column('time', Integer, primary_key=True),
        Column('start_time', Integer),
        Column('open', Float, nullable=False),  # double ?
        Column('high', Float, nullable=False),  # double ?
        Column('low', Float, nullable=False),  # double ?
        Column('close', Float, nullable=False),  # double ?
        Column('volume', Float, nullable=True),  # double ?
        Column('turnover', Float, nullable=True),  # double ?
    )


def get_orderbook_columns() -> Iterable[Column]:
    return (
        Column('time', Double, primary_key=True),
        Column('price', Float, primary_key=True),
        Column('side', Boolean, primary_key=True, comment='"is_ask" flag: 0 - bid, 1 - ask'),
        Column('volume', Float, nullable=False),
        Column('index', SmallInteger, nullable=True),
    )


def get_trades_columns() -> Iterable[Column]:
    return (
        Column('time', Double, nullable=False, index=True),
        Column('side', Boolean, nullable=False, comment='"is_buy" flag: 0 - sell, 1 - buy'),
        Column('size', Float, nullable=False),
        Column('price', Float, nullable=False),
        Column('id', String(36), primary_key=True),
    )
