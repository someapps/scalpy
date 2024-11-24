from typing import Iterable

from pendulum import Interval

from scalpy import EventInfo, History, Event
from scalpy.connectors.bybit import BybitConnector
from scalpy.database.db import Database
from scalpy.database.downloaded_service import DownloadedService
from scalpy.database.market_service import MarketService


class HistoryProvider(History):

    def __init__(self):
        database = Database()
        database.init()

        self.downloaded_service = DownloadedService(database)
        self.market_service = MarketService(database)
        self.connector = BybitConnector()

    def get(self, info: EventInfo, interval: Interval) -> Iterable[Event]:
        interval.in_days()

        if not self.connector.can_batch_download(info.type):
            for day in interval:
                if not self.downloaded_service.is_downloaded(info, day.date()):
                    data = list(self.connector.get_day(info, day))
                    self.market_service.save(info, data)
                    self.downloaded_service.set_downloaded(info, day, True)
            yield from self.market_service.get(info, interval)
            return

        intervals_for_download = self._intervals_for_download(info, interval)

        if intervals_for_download:
            for sub_interval in intervals_for_download:
                sub_interval.in_days()
                data = list(self.connector.get_days(info,  sub_interval))
                self.market_service.save(info, data)

                for day in sub_interval:
                    self.downloaded_service.set_downloaded(info, day, True)

        yield from self.market_service.get(info, interval)

    def _intervals_for_download(self, info: EventInfo, interval: Interval) -> list:
        intervals_for_download = []
        day_row = []
        day_skipped = False

        def add_day_row(days):
            start = days[0].replace(hour=0, minute=0, second=0, microsecond=0)
            end = (days[-1]
                   .replace(hour=0, minute=0, second=0, microsecond=0)
                   .add(days=1)
                   .add(microseconds=-1))
            sub_interval = Interval(start, end)
            intervals_for_download.append(sub_interval)

        for day in interval:
            if self.downloaded_service.is_downloaded(info, day.date()):
                day_skipped = True
            else:
                if day_skipped:
                    if day_row:
                        add_day_row(day_row)
                        day_row = []
                        day_skipped = False
                day_row.append(day)

        if day_row:
            add_day_row(day_row)
        return intervals_for_download
