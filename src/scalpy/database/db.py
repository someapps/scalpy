from sqlalchemy import create_engine, Engine, MetaData
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker, Session

from .. import config
from .orm import Base


class Database:
    metadata = None
    engine = None
    session_maker = None
    async_engine = None
    async_session_maker = None

    @classmethod
    def init(cls):
        cls.get_metadata().create_all(cls.get_engine())

    @classmethod
    def get_metadata(cls) -> MetaData:
        if cls.metadata is None:
            cls.metadata = Base.metadata
            cls.metadata.reflect(bind=cls.get_engine())
        return cls.metadata

    @classmethod
    def get_engine(cls) -> Engine:
        if cls.engine is None:
            cls.engine = create_engine(
                url=config.SQLALCHEMY_DATABASE_URI,
                # echo=True,
            )
        return cls.engine

    @classmethod
    def get_session_maker(cls) -> sessionmaker:
        if cls.session_maker is None:
            cls.session_maker = sessionmaker(bind=cls.get_engine())
        return cls.session_maker

    @classmethod
    def new_session(cls) -> Session:
        return cls.get_session_maker()()

    @classmethod
    def get_async_engine(cls) -> AsyncEngine:
        if cls.async_engine is None:
            cls.async_engine = create_engine(config.SQLALCHEMY_ASYNC_DATABASE_URI)
        return cls.async_engine

    @classmethod
    def get_async_session_maker(cls) -> async_sessionmaker:
        if cls.async_session_maker is None:
            cls.async_session_maker = async_sessionmaker(bind=cls.get_async_engine())
        return cls.async_session_maker

    @classmethod
    def new_async_session(cls) -> AsyncSession:
        return cls.get_async_session_maker()()

    @classmethod
    def dispose(cls):
        if cls.async_engine is not None:
            cls.async_engine.dispose()
        if cls.engine is not None:
            cls.engine.dispose()

        cls.metadata = None
        cls.engine = None
        cls.session_maker = None
        cls.async_engine = None
        cls.async_session_maker = None
