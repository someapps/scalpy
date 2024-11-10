import gzip
import io
import zipfile
from datetime import datetime, date, timedelta, UTC
from typing import Iterable

import requests


def to_datetime(timestamp: int | float) -> datetime:
    return datetime.utcfromtimestamp(float(timestamp) / 1000)


def to_timestamp(value: date | datetime) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)

    if isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time())
        return int(dt.timestamp() * 1000)

    raise NotImplementedError


def normalize_ts(value: int) -> int:
    now = to_timestamp(datetime.utcnow())
    return max(0, min(now, value))


def get_days(start: int, end: int) -> Iterable[date]:
    day = to_datetime(start).date()
    end_date = to_datetime(end).date()

    while day <= end_date:
        yield day
        day += timedelta(days=1)


def get_day_start_end(day: date) -> tuple[int, int]:
    day_datetime = datetime.combine(day, datetime.min.time(), UTC)
    next_day_datetime = day_datetime + timedelta(days=1)
    return to_timestamp(day_datetime), to_timestamp(next_day_datetime) - 1


# TODO del
def get_expected_candle_count(period: int, start: int, end: int) -> int:
    range_ = end - start
    period = 1000 * 60 * period
    remainder = int(range_ % period == 0)
    count = max(0, range_ // period - remainder)

    if start % period == 0:
        count += 1

    if start != end and end % period == 0:
        count += 1

    return count


# TODO del
def get_expected_candles(period: int, start: int, end: int) -> Iterable[int]:
    period_ms = 1000 * 60 * period
    ts = start
    reminder = start % period_ms

    if reminder != 0:
        ts += period_ms - reminder

    yield from range(ts, end + 1, period_ms)


def download_file(url: str, filename: str) -> bool:
    response = requests.get(url)

    if response.status_code != 200:
        print("Произошла ошибка при скачивании файла")
        return False

    with open(filename, 'wb') as file:
        file.write(response.content)

    return True


def get_lines_from_archive(filename: str, skip_title: bool) -> Iterable[str]:
    def read_lines(lines_):
        lines_ = iter(lines_)

        if skip_title:
            line = next(lines_).decode('utf-8')

            if len(line) > 0 and line[0].isdigit():
                yield line

        for line in lines_:
            yield line.decode('utf-8')

    def unpack_gz():
        with gzip.GzipFile(filename=filename) as f:
            yield from read_lines(f)

    def unpack_zip():
        with zipfile.ZipFile(file=filename) as af:
            file_list = af.namelist()

            assert len(file_list) == 1

            for file in file_list:
                with af.open(file) as f:
                    yield from read_lines(f)

    if filename.endswith('.zip'):
        yield from unpack_zip()
    elif filename.endswith('.gz'):
        yield from unpack_gz()
    else:
        raise NotImplementedError


# TODO del
def download_gz_and_get_lines(url: str) -> Iterable[str]:
    response = requests.get(url)

    if response.status_code != 200:
        print("Произошла ошибка при скачивании файла")
        return

    # with gzip.GzipFile(filename='BTCUSDT2024-10-01.csv.gz') as file:
    # with zipfile.ZipFile(filename='C:\\Users\\Roman\\Downloads\\2024-09-20_1000PEPEUSDT_ob500.data.zip') as file:
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as file:
        for line in file:
            yield line.decode('utf-8')


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
