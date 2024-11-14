from datetime import datetime
from typing import Iterable, Sequence

from scalpy import EventInfo, DataType
from scalpy.connectors.bybit import BybitConnector
from scalpy.database.db import Database
from scalpy.database.downloaded_service import DownloadedService
from scalpy.database.service import Service
from scalpy.utils import normalize_ts, get_days, to_timestamp


class HistoryProvider:

    def __init__(self):
        database = Database()
        database.init()

        self.downloaded_service = DownloadedService(database)
        self.service = Service(database)
        self.connector = BybitConnector()

    def get(self, info: EventInfo, start: int, end: int) -> Iterable[Sequence]:
        if start > end:
            raise ValueError('Start must be less than end')

        start = normalize_ts(start)
        end = normalize_ts(end)
        days = list(get_days(start, end))

        if not self.connector.can_batch_download(info.type):
            for day in days:
                if not self.downloaded_service.is_downloaded(info, day):
                    data = list(self.connector.get_day(info, day))
                    self.service.save(info, data)
                    self.downloaded_service.set_downloaded(info, day, True)
            yield from self.service.get(info, start, end)
            return

        day_rows_for_download = []
        day_row = []
        day_skipped = False

        for day in days:
            if self.downloaded_service.is_downloaded(info, day):
                day_skipped = True
            else:
                if day_skipped:
                    if day_row:
                        day_rows_for_download.append(day_row)
                        day_row = []
                        day_skipped = False
                day_row.append(day)

        if day_row:
            day_rows_for_download.append(day_row)

        if day_rows_for_download:
            for day_row in day_rows_for_download:
                data = list(self.connector.get_days(info, day_row[0], day_row[-1]))
                self.service.save(info, data)

                for day in day_row:
                    self.downloaded_service.set_downloaded(info, day, True)

        yield from self.service.get(info, start, end)
