import configparser
import datetime
import logging
import logging.config
from typing import Dict, List, Tuple

from pymongo import MongoClient, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from logging_config import LOGGING_CONFIG


CONFIG_FILE = './src/config.ini'

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def get_config_url() -> str:
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config['mongodb']['url']


def connect_initial(db_name: str, url: str = get_config_url()) -> Tuple[MongoClient, Database]:
    client = MongoClient(
        host=url,
        tls=True,
        tlsAllowInvalidCertificates=True
    )
    db = client[db_name]
    return client, db


def generate_queries(docs: List[Dict], with_metadata: bool) -> List[Dict]:
    '''
    If it is a collection of time series, each document has a subdocument
    called `metadata`. Using the `find_one` method to query any document
    might fail because the order of keys in the subdocument is important.
    Therefore, an alternative approach is used where each field is matched
    individually to query the subdocument.
    '''
    if with_metadata:
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


def update_collection(collection: Collection, docs: List[Dict], with_metadata: bool):
    logger.info(f'Updating {collection.name}..')
    queries = generate_queries(docs, with_metadata)
    for query, doc in zip(queries, docs):
        if not collection.find_one(query):
            collection.insert_one(doc)


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


def get_latest_timestamp(collection: Collection) -> datetime.datetime:
    latest_doc = collection.find().sort('timestamp', DESCENDING)[0]
    return latest_doc['timestamp']


def get_daily_document(collection: Collection, datetime: datetime.datetime):
    return collection.find_one({'timestamp': datetime})


def count_documents(collection: Collection) -> int:
    return collection.count_documents({})


if __name__ == '__main__':
    general_docs = [
        {'name': 'blender', 'price': 340, 'category': 'kitchen appliance'},
        {'name': 'egg', 'price': 36, 'category': 'food'}
    ]
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
    client, db = connect_initial(
        db_name='test_db'
    )
    update_collection(
        collection=db['kitchen_collection'],
        docs=general_docs,
        with_metadata=False
    )
    collection = get_timeseries_collection(
        db=db,
        collection_name='patient_condition',
    )
    update_collection(
        collection=collection,
        docs=timeseries_docs,
        with_metadata=True
    )
    latest_timestamp = get_latest_timestamp(
        collection=collection
    )
    doc = get_daily_document(
        collection=collection,
        datetime=datetime.datetime(2021, 5, 20)
    )
    client.close()
