import configparser
import datetime
from typing import Dict, List

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


# read config.ini
config = configparser.ConfigParser()
config.read('.\\src\\config.ini')


# connect to mongodb
url = config['mongodb']['url']
client = MongoClient(url, tls=True, tlsAllowInvalidCertificates=True)


def get_database(client: MongoClient, db_name: str) -> Database:
    db_names = client.list_database_names()
    if db_name not in db_names:
        return Database(client, db_name)
    else:
        return client.get_database(db_name)


def get_collection(db: Database, collection_name: str, istimeseries: bool) -> Collection:
    collection_names = db.list_collection_names()
    if collection_name not in collection_names:
        if istimeseries:
            timeseries = {
                'timeField': 'timestamp',
                'metaField': 'metadata',
                'granularity': 'hours'
            }
            return db.create_collection(collection_name, timeseries=timeseries)
        else:
            return db.create_collection(collection_name)
    else:
        return db.get_collection(collection_name)


def update_docs(db_name: str, collection_name: str, istimeseries: bool, docs: List[Dict]):
    db = get_database(client, db_name=db_name)
    collection = get_collection(db, collection_name, istimeseries)
    for doc in docs:
        if not collection.find_one(doc):
            collection.insert_one(doc)


if __name__ == '__main__':
    general_docs = [
        {'name': 'blender', 'price': 340, 'category': 'kitchen appliance'},
        {'name': 'egg', 'price': 36, 'category': 'food'}
    ]

    update_docs(
        db_name='test_db',
        collection_name='kitchen_collection',
        istimeseries=False,
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

    update_docs(
        db_name='test_db',
        collection_name='patient_condition',
        istimeseries=True,
        docs=timeseries_docs
    )
