import configparser
from typing import Dict, List

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


# read config.ini
config = configparser.ConfigParser()
config.read('.\\src\\config.ini')


# connect to mongodb
url = config['mongodb']['url']
client = MongoClient(url)


def get_init_db(client: MongoClient, db_name: str) -> Database:
    db_names = client.list_database_names()
    if db_name not in db_names:
        return Database(client, db_name)
    else:
        return client.get_database(db_name)


def get_init_collection(db: Database, collection_name: str) -> Collection:
    collection_names = db.list_collection_names()
    if collection_name not in collection_names:
        return db.create_collection(collection_name)
    else:
        return db.get_collection(collection_name)


def create_docs(db_name: str, collection_name: str, items: List[Dict]):
    db = get_init_db(client, db_name=db_name)
    collection = get_init_collection(db, collection_name=collection_name)
    collection.insert_many(items)


if __name__ == '__main__':
    items = [
        {'name': 'Blender', 'price': 340, 'category': 'kitchen appliance'},
        {'name': 'Egg', 'price': 36, 'category': 'food'}
    ]

    create_docs(
        db_name='test_db',
        collection_name='test_collection',
        items=items
    )
