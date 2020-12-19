from datetime import datetime
import logging
import os

from sqlalchemy import (
    create_engine, Column, inspect,
    Integer, DateTime, Float, String
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import exc as sqlaException


DB_USER = os.environ.get('DB_USER_IMI', 'postgres')
DB_PASS = os.environ.get('DB_PASS_IMI', '')
DB_HOST = os.environ.get('DB_HOST_IMI', '127.0.0.1')
DB_PORT = os.environ.get('DB_PORT', 5432)
DB_NAME = os.environ.get('DB_NAME_IMI', 'postgres')
DB_SCHEMA = os.environ.get('DB_SCHEMA_IMI', 'imi')

pg_config = {
    'username': DB_USER,
    'password': DB_PASS,
    'host': DB_HOST,
    'port': DB_PORT,
    'database': DB_NAME,
    'schema': DB_SCHEMA
}

logger = logging.getLogger(__name__)
pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/postgres".format(**pg_config)
Base = declarative_base()
db_engine = create_engine(
    pg_dsn,
    connect_args={"application_name": 'imi:' + str(__name__)},
    pool_size=200,
    pool_recycle=600,
    max_overflow=0,
    encoding='utf-8'
    )
try:
    conn = db_engine.connect()
    conn.execute("commit")
    conn.execute("create database {}".format(pg_config['database']))
    conn.close()
except sqlaException.ProgrammingError as Exception:
    pass


class ImiTexts(Base):
    __tablename__ = 'imi_texts'
    __table_args__ = {"schema": pg_config['schema']}

    gan = Column(Integer, autoincrement=False, primary_key=True)
    text = Column(String)


class Analysis(Base):
    __tablename__ = 'analysis'
    __table_args__ = {"schema": pg_config['schema']}

    gan = Column(Integer, autoincrement=False, primary_key=True)
    word_count = Column(Integer)
    positive = Column(Float)
    negative = Column(Float)
    uncertainty = Column(Float)
    litigious = Column(Float)
    modal_weak = Column(Float)
    modal_moderate = Column(Float)
    modal_strong = Column(Float)
    constraining = Column(Float)
    alphabetic = Column(Integer)
    digits = Column(Integer)
    numbers = Column(Integer)
    avg_syllables_per_word = Column(Float)
    avg_word_length = Column(Float)
    vocabulary = Column(Integer)
    created = Column(DateTime, default=datetime.utcnow)

    def _asdict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}


class SentimentHarvard(Base):
    __tablename__ = 'sentiment_harvard'
    __table_args__ = {"schema": pg_config['schema']}

    gan = Column(Integer, autoincrement=False, primary_key=True)
    word_count = Column(Integer)
    positiv = Column(Float)
    negativ = Column(Float)
    pstv = Column(Float)
    affil = Column(Float)
    ngtv = Column(Float)
    hostile = Column(Float)
    strong = Column(Float)
    power = Column(Float)
    weak = Column(Float)
    submit = Column(Float)
    active = Column(Float)
    passive = Column(Float)
    created = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(db_engine)
