import json
from datetime import date
from pathlib import Path
from typing import List, Iterable, Sequence

import requests
from loguru import logger
from pybit.unified_trading import HTTP

from scalpy.utils import get_day_start_end, download_file, get_lines_from_archive
from .. import DataType, Connector, EventInfo, MessageType, PriceVolume, OrderbookEvent


class BybitConnector(Connector):

    def __init__(self):
        self.http = HTTP(testnet=False)

    def can_batch_download(self, data_type: DataType) -> bool:
        match data_type:
            case DataType.KLINE:
                return True
            case _:
                return False

    def get_day(self, info: EventInfo, day: date) -> Iterable[Sequence]:
        logger.info(f'Downloading {info} for {day}...')

        match info.type:
            case DataType.TRADE | DataType.ORDERBOOK:
                yield from self.download(info, day)
            case _:
                raise NotImplementedError

        logger.info(f'Downloaded {info} for {day}')

    def get_days(self, info: EventInfo, day_from: date, day_to: date) -> Iterable[Sequence]:
        logger.info(f'Downloading {info} from {day_from} to {day_to}...')

        match info.type:
            case DataType.KLINE:
                start, _ = get_day_start_end(day_from)
                _, end = get_day_start_end(day_to)
                yield from self.get_kline(info.symbol, info.period, start=start, end=end)
            case _:
                raise NotImplementedError

    def get_kline(self, symbol: str, period: int, start: int, end: int) -> Iterable[Sequence]:
        while start <= end:
            candles = self._get_kline(symbol, period, start=start, end=end)

            if len(candles) == 0:
                return

            yield from candles
            end = int(candles[-1][1]) - 1

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

    def _get_kline(self, symbol: str, period: int, **kwargs) -> List[Sequence]:
        bybit_period = self._convert_period(period)

        logger.info(f'Downloading {symbol} candles with period {bybit_period}, args={kwargs}...')
        result = self.http.get_kline(
            symbol=symbol,
            interval=bybit_period,
            limit=1000,
            **kwargs
        )['result']['list']
        return [
            (
                int(item[0]) / 1000 + period * 60,  # close time
                int(item[0]) / 1000,  # open time
                float(item[1]),
                float(item[2]),
                float(item[3]),
                float(item[4]),
                float(item[5]) if item[5] else None,
                float(item[6]) if item[6] else None,
            )
            for item in result
        ]

    def download(self, info: EventInfo, day: date) -> Iterable[Sequence]:
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
            logger.info(f'File {filename} already exists, skip download...')

        lines = get_lines_from_archive(filename, skip_title)

        for line in lines:
            yield fetch_func(line)

    @staticmethod
    def get_download_info(symbol: str, product_id: str, day: date) -> dict:
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

    @staticmethod
    def fetch_trade(line: str):
        ts, symbol, side, size, price, tick_dir, id_, *_ = line.split(',')
        return float(ts), side[0] == 'B', float(size), float(price), id_
    
    @staticmethod
    def fetch_orderbook(line: str):
        def to_price_volume(list_):
            return PriceVolume(float(list_[0]), float(list_[1]))

        data = json.loads(line)
        return OrderbookEvent(
            timestamp=data['cts'],
            type=MessageType[data['type'].upper()],
            asks=list(map(to_price_volume, data['data']['a'])),
            bids=list(map(to_price_volume, data['data']['b'])),
            producer_id=0
        )
