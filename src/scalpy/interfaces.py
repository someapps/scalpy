from abc import ABC, abstractmethod
from typing import AsyncGenerator, List

from .items import EventInfo, MarketRequest, Event


class History(ABC):

    @abstractmethod
    async def get(self, info: EventInfo, start: int, end: int) -> AsyncGenerator[Event]:
        ...


class MarketIterator(ABC):

    def __init__(self):
        self._requests = set()
        self._sources = set()

    def __aiter__(self):
        return self

    def subscribe(self, request: MarketRequest):
        self._requests.add(request)
        self._sources.add(request.info)

    @abstractmethod
    async def __anext__(self) -> Event:
        ...

    @abstractmethod
    async def run(self):
        ...


class Handler(ABC):

    def __init__(self, requests: List[MarketRequest]):
        self._requests = requests

    @property
    def requests(self) -> List[MarketRequest]:
        return self._requests
