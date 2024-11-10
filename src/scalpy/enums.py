from enum import Enum


class DataType(Enum):
    TICK = 1
    TRADE = 2
    ORDERBOOK = 3
    KLINE = 4


class MessageType(Enum):
    SNAPSHOT = 1
    DELTA = 2
