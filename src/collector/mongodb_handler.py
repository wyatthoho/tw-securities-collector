import datetime
import logging
import sys
from typing import Any, Optional, Mapping

from pymongo import MongoClient, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database


logger = logging.getLogger(__name__)


class CollectionHandler:
    def __init__(self, url: str, db_name: str, collection_name: str):
        self.client = MongoClient(url)
        logger.info(f"Connected to MongoDB (nodes: {self.client.nodes})")

        self.db = self.client.get_database(db_name)
        logger.info(f"Using database: '{self.db.name}'")

        self.collection = self.db.get_collection(collection_name)
        logger.info(f"Using collection: '{self.collection.name}'")

    def _trans_dot_notation(self, doc: dict) -> dict:
        _doc = {k: v for k, v in doc.items() if k != "metadata"}
        for key, val in doc["metadata"].items():
            _doc[f"metadata.{key}"] = val
        return _doc

    def upload_docs(self, docs: list[dict]):
        for doc in docs:
            _doc = self._trans_dot_notation(doc) if "metadata" in doc else doc

            queried = self.collection.find_one(_doc)
            if queried:
                logger.info(f"Document already exists. ID: {queried['_id']}")
            else:
                result = self.collection.insert_one(doc)
                logger.info(f"Document inserted. ID: {result.inserted_id}")

    def close(self):
        self.client.close()
        logger.info("MongoDB connection closed.")


# def get_timeseries_collection(
#     db: Database[dict[str, Any]], collection_name: str
# ) -> Collection[dict[str, Any]]:
#     """
#     Ensures a time-series collection exists or creates it with default parameters.[cite: 1]
#     """
#     collection_names = db.list_collection_names()
#     if collection_name not in collection_names:
#         # Mapping is used for read-only configuration dictionaries[cite: 1]
#         timeseries: Mapping[str, Any] = {
#             "timeField": "timestamp",
#             "metaField": "metadata",
#             "granularity": "hours",
#         }
#         return db.create_collection(collection_name, timeseries=timeseries)
#     else:
#         return db.get_collection(collection_name)


# def get_latest_timestamp(
#     collection: Collection[dict[str, Any]],
# ) -> Optional[datetime.datetime]:
#     """
#     Retrieves the most recent timestamp. Returns None if collection is empty.[cite: 1]
#     """
#     try:
#         # Sort and limit to 1 for efficiency[cite: 1]
#         latest_doc = collection.find().sort("timestamp", DESCENDING).limit(1)
#         doc = next(latest_doc, None)
#         return doc["timestamp"] if doc else None
#     except (StopIteration, KeyError):
#         return None


# def get_daily_document(
#     collection: Collection[dict[str, Any]], dt: datetime.datetime
# ) -> Optional[dict[str, Any]]:
#     """
#     Finds a document matching a specific timestamp.[cite: 1]
#     """
#     return collection.find_one({"timestamp": dt})


# def count_documents(collection: Collection[dict[str, Any]]) -> int:
#     """
#     Returns total document count in collection.[cite: 1]
#     """
#     return collection.count_documents({})


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    MONGODB_URL = os.environ.get("MONGODB_URL")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    handler_cooking = CollectionHandler(MONGODB_URL, "testing", "cooking")
    handler_cooking.upload_docs(
        [
            {"name": "Beef Wellington", "difficulty": "hard", "cost": 50},
            {"name": "Chicken Carbonara", "difficulty": "medium", "cost": 20},
            {"name": "Boiled Egg", "difficulty": "easy", "cost": 5},
        ]
    )

    handler_inbody = CollectionHandler(MONGODB_URL, "testing", "inbody")
    handler_inbody.upload_docs(
        [
            {
                "metadata": {"name": "wyatt", "gender": "male"},
                "timestamp": datetime.datetime(2021, 5, 18),
                "weight": 70.1,
                "fat_mass": 17.3,
                "muscle_mass": 52.8,
            },
            {
                "metadata": {"name": "wyatt", "gender": "male"},
                "timestamp": datetime.datetime(2021, 5, 19),
                "weight": 70.6,
                "fat_mass": 17.8,
                "muscle_mass": 52.8,
            },
            {
                "metadata": {"name": "wyatt", "gender": "male"},
                "timestamp": datetime.datetime(2021, 5, 20),
                "weight": 70.2,
                "fat_mass": 17.4,
                "muscle_mass": 52.8,
            },
        ]
    )
