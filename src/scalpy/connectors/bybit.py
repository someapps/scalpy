import json
from pathlib import Path
from typing import List, Iterable

import requests
from loguru import logger
from pendulum import Interval, Date
from pybit.unified_trading import HTTP

from scalpy import (
    Connector,
    DataType,
    EventInfo,
    MessageType,
    OHLC,
    Orderbook,
    PriceVolume,
    Trade,
)
from scalpy.constants import PERIOD_TO_MS
from scalpy.utils import download_file, get_lines_from_archive


class BybitConnector(Connector):

    def __init__(self):
        self.http = HTTP(testnet=False)

    def can_batch_download(self, data_type: DataType) -> bool:
        match data_type:
            case DataType.KLINE:
                return True
            case _:
                return False

    def get_day(self, info: EventInfo, day: Date) -> Iterable[Trade | Orderbook]:
        logger.info(f'Downloading {info} for {day}...')

        match info.type:
            case DataType.TRADE | DataType.ORDERBOOK:
                yield from self.download(info, day)
            case _:
                raise NotImplementedError

        logger.info(f'Downloaded {info} for {day}')

    def get_days(self, info: EventInfo, interval: Interval) -> Iterable[OHLC]:
        logger.info(f'Downloading {info} for {interval}...')

        match info.type:
            case DataType.KLINE:
                yield from self.get_kline(info.symbol, info.period, interval)
            case _:
                raise NotImplementedError

        logger.info(f'Downloaded {info} for {interval}')

    def get_kline(self, symbol: str, period: int, interval: Interval) -> Iterable[OHLC]:
        start = interval.start.int_timestamp * 1000
        end = interval.end.int_timestamp * 1000 - 1

        while start <= end:
            candles = self._get_kline(symbol, period, start=start, end=end)

            if len(candles) == 0:
                return

            yield from candles
            end = int(candles[-1].start_timestamp) - 1

    @staticmethod
    def _convert_period(period: int) -> str:
        match period:
            case 1 | 3 | 5 | 15 | 30 | 60 | 120 | 240 | 360 | 720:
                return str(period)
            case 1440:
                return 'D'
            case 10080:
                return 'W'
            case 43200:
                return 'M'
            case _:
                raise ValueError(f'Unsupported period {period}')

    def _get_kline(self, symbol: str, period: int, **kwargs) -> List[OHLC]:
        bybit_period = self._convert_period(period)

        logger.info(f'Downloading {symbol} candlesticks with period '
                    f'{bybit_period}, args={kwargs}...')
        result = self.http.get_kline(
            symbol=symbol,
            interval=bybit_period,
            limit=1000,
            **kwargs
        )['result']['list']
        return [
            OHLC(
                timestamp=float(item[0]) + period * PERIOD_TO_MS,  # close time
                producer_id=id(self),
                start_timestamp=float(item[0]),  # open time
                open=float(item[1]),
                high=float(item[2]),
                low=float(item[3]),
                close=float(item[4]),
                volume=float(item[5]) if item[5] else None,
                turnover=float(item[6]) if item[6] else None,
            )
            for item in result
        ]

    def download(self, info: EventInfo, day: Date) -> Iterable[Trade | Orderbook]:
        match info.type:
            case DataType.TRADE:
                product_id = 'trade'
                fetch_func = self.fetch_trade
                skip_title = True
            case DataType.ORDERBOOK:
                product_id = 'orderbook'
                fetch_func = self.fetch_orderbook
                skip_title = False
            case _:
                raise NotImplementedError

        file_info = self.get_download_info(info.symbol, product_id, day)
        path = Path(f'./downloads/{product_id}/{info.symbol}/')
        path.mkdir(parents=True, exist_ok=True)
        filepath = path / file_info['filename']
        filename = str(filepath.absolute())

        if not filepath.exists():
            download_file(file_info['url'], filename)
        else:
            logger.info(f'File {filename} already exists, skip downloading...')

        lines = get_lines_from_archive(filename, skip_title)

        for line in lines:
            yield fetch_func(line)

    @staticmethod
    def get_download_info(symbol: str, product_id: str, day: Date) -> dict:
        day_str = day.strftime("%Y-%m-%d")
        url = ('https://api2.bybit.com/quote/public/support/download/list-files'
               '?bizType=contract&interval=daily&periods='
               f'&productId={product_id}'
               f'&symbols={symbol}'
               f'&startDay={day_str}'
               f'&endDay={day_str}')

        try:
            result = requests.get(url)
            return result.json()['result']['list'][0]
        except Exception as e:
            logger.error(e)
            raise ValueError

    def fetch_trade(self, line: str) -> Trade:
        ts, symbol, side, size, price, tick_dir, trade_id, *_ = line.split(',')
        return Trade(
            timestamp=float(ts) * 1000,
            producer_id=id(self),
            is_buy=side[0] == 'B',
            size=float(size),
            price=float(price),
            trade_id=trade_id,
        )

    def fetch_orderbook(self, line: str) -> Orderbook:
        def to_price_volume(list_):
            return PriceVolume(
                price=float(list_[0]),
                volume=float(list_[1])
            )

        data = json.loads(line)
        return Orderbook(
            timestamp=float(data['cts']),
            producer_id=id(self),
            type=MessageType[data['type'].upper()],
            asks=list(map(to_price_volume, data['data']['a'])),
            bids=list(map(to_price_volume, data['data']['b'])),
        )
