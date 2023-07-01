import configparser
import datetime
import functools
import logging
import logging.config
from typing import Dict, List, Protocol

from pymongo import MongoClient, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from logging_config import LOGGING_CONFIG


CONFIG_FILE = '.\\src\\config.ini'

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def read_config() -> Dict[str, str]:
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config['mongodb']['url']


class OriginalFunc(Protocol):
    def __call__(self, db: Database, collection_name: str) -> Collection:
        ...


class DecoratedFunc(Protocol):
    def __call__(self, db_name: str, collection_name: str) -> None:
        ...


def get_database(client: MongoClient, db_name: str) -> Database:
    db_names = client.list_database_names()
    if db_name not in db_names:
        return Database(client, db_name)
    else:
        return client.get_database(db_name)


def connect_mongodb(url: str = read_config(), tls: bool = True, tls_allow_invalid_certificates: bool = True):
    def decorator(func: OriginalFunc) -> DecoratedFunc:
        @functools.wraps(func)
        def wrapper(db_name: str, collection_name: str):
            logger.info(f'Connecting to \"{db_name}\"..')
            client = MongoClient(
                url,
                tls=tls,
                tlsAllowInvalidCertificates=tls_allow_invalid_certificates
            )
            db = get_database(client, db_name)
            return func(db, collection_name)
        return wrapper
    return decorator


@connect_mongodb()
def get_general_collection(db: Database, collection_name: str) -> Collection:
    collection_names = db.list_collection_names()
    if collection_name not in collection_names:
        return db.create_collection(collection_name)
    else:
        return db.get_collection(collection_name)


def close_client(func):
    def wrapper(collection: Collection, docs: List[Dict]):
        func(collection, docs)
        collection.database.client.close()
    return wrapper


def generate_queries(docs: List[Dict], is_timeseries: bool) -> List[Dict]:
    '''
    If it is a collection of time series, each document has a subdocument
    called `metadata`. Using the `find_one` method to query any document
    might fail because the order of keys in the subdocument is important.
    Therefore, an alternative approach is used where each field is matched
    individually to query the subdocument.
    '''
    if is_timeseries:
        queries = []
        for doc in docs:
            query = doc.copy()
            for key, val in doc['metadata'].items():
                query[f'metadata.{key}'] = val
            del query['metadata']
            queries.append(query)
    else:
        queries = docs
    return queries


@close_client
def insert_documents(collection: Collection, docs: List[Dict]):
    logger.info(f'Updating \"{collection.name}\"..')
    is_timeseries = 'timeseries' in collection.options()
    queries = generate_queries(docs, is_timeseries)
    for query, doc in zip(queries, docs):
        if not collection.find_one(query):
            collection.insert_one(doc)


def connect_and_insert_general(db_name: str, collection_name: str, docs: List[Dict]):
    collection = get_general_collection(
        db_name=db_name,
        collection_name=collection_name,
    )
    insert_documents(collection, docs)


@connect_mongodb()
def get_timeseries_collection(db: Database, collection_name: str) -> Collection:
    collection_names = db.list_collection_names()
    if collection_name not in collection_names:
        timeseries = {
            'timeField': 'timestamp',
            'metaField': 'metadata',
            'granularity': 'hours'
        }
        return db.create_collection(collection_name, timeseries=timeseries)
    else:
        return db.get_collection(collection_name)


def connect_and_insert_timeseries(db_name: str, collection_name: str, docs: List[Dict]):
    collection = get_timeseries_collection(
        db_name=db_name,
        collection_name=collection_name,
    )
    insert_documents(collection, docs)


def get_latest_timestamp(db_name: str, collection_name: str) -> datetime.date:
    collection = get_timeseries_collection(
        db_name=db_name,
        collection_name=collection_name,
    )
    logger.info(f'Searching the latest date of \"{collection_name}\"..')
    latest_doc = collection.find().sort('timestamp', DESCENDING)[0]
    return latest_doc['timestamp'].date()


if __name__ == '__main__':
    general_docs = [
        {'name': 'blender', 'price': 340, 'category': 'kitchen appliance'},
        {'name': 'egg', 'price': 36, 'category': 'food'}
    ]

    connect_and_insert_general(
        db_name='test_db',
        collection_name='kitchen_collection',
        docs=general_docs
    )

    timeseries_docs = [
        {
            'metadata': {'patient': 'wyatt', 'gender': 'male'},
            'timestamp': datetime.datetime(2021, 5, 18),
            'weight': 70.1,
            'body temperature': 37.4
        },
        {
            'metadata': {'patient': 'wyatt', 'gender': 'male'},
            'timestamp': datetime.datetime(2021, 5, 19),
            'weight': 70.6,
            'body temperature': 37.0
        },
        {
            'metadata': {'patient': 'wyatt', 'gender': 'male'},
            'timestamp': datetime.datetime(2021, 5, 20),
            'weight': 70.2,
            'body temperature': 36.8
        },
    ]

    connect_and_insert_timeseries(
        db_name='test_db',
        collection_name='patient_condition',
        docs=timeseries_docs
    )

    latest_timestamp = get_latest_timestamp(
        db_name='test_db',
        collection_name='patient_condition',
    )
