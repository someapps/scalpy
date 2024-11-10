from .. import EventInfo, DataType


def get_table_name(info: EventInfo):
    if info.type == DataType.KLINE:
        return f'kline_{info.interval}_{info.symbol}'
