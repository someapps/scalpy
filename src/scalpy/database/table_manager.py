from loguru import logger
from sqlalchemy import Table

from scalpy import EventInfo, DataType
from .db import Database
from .orm import get_kline_columns, get_trades_columns, get_orderbook_columns


class TableManager:

    def __init__(self, database: Database):
        self.database = database

    def get_table(self, info: EventInfo, ext: str = None) -> Table:
        metadata = self.database.get_metadata()
        table_name = self.get_table_name(info, ext)

        if table_name not in metadata.tables:
            if info.type == DataType.KLINE:
                columns = get_kline_columns()
            elif info.type == DataType.TRADE:
                columns = get_trades_columns()
            elif info.type == DataType.ORDERBOOK:
                columns = get_orderbook_columns()
            else:
                raise NotImplementedError

            logger.info(f'Creating table {table_name}...')
            Table(table_name, metadata, *columns)
            metadata.create_all(bind=Database.get_engine())
            logger.info(f'Created table {table_name}')

        return metadata.tables[table_name]

    def is_table_exists(self, info: EventInfo) -> bool:
        return self.get_table_name(info) in self.database.get_metadata().tables

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
