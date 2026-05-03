import datetime
import logging
import os
import sys
from typing import Any, Optional, Mapping

from dotenv import load_dotenv
from pymongo import MongoClient, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

# Load environment variables
load_dotenv()
MONGODB_URL = os.environ.get("MONGODB_URL", "mongodb://localhost:27017")
logger = logging.getLogger(__name__)


def connect_initial(
    db_name: str, url: str = MONGODB_URL
) -> tuple[MongoClient[dict[str, Any]], Database[dict[str, Any]]]:
    """
    Establishes the initial connection to MongoDB with modernized type hints.
    """
    logger.info(f"Initiating MongoDB connection to database: '{db_name}'")

    try:
        # MongoClient is generic; we hint that it handles dictionaries[cite: 1]
        client: MongoClient[dict[str, Any]] = MongoClient(
            host=url, tls=True, tlsAllowInvalidCertificates=True
        )

        # Clean logging: show cluster nodes without dumping the whole object
        nodes = client.nodes or url.split("@")[-1].split("/")[0]
        logger.info(f"Successfully created MongoClient (Cluster nodes: {nodes})")

        db = client.get_database(db_name)
        logger.info(f"Connected to database instance: '{db.name}'")

        return client, db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


def generate_queries(
    docs: list[dict[str, Any]], with_metadata: bool
) -> list[dict[str, Any]]:
    """
    Generates queries for time-series documents by matching metadata fields individually.[cite: 1]
    """
    if with_metadata:
        queries: list[dict[str, Any]] = []
        for doc in docs:
            query = doc.copy()
            # Safety check for metadata existence[cite: 1]
            metadata = doc.get("metadata", {})
            for key, val in metadata.items():
                query[f"metadata.{key}"] = val

            if "metadata" in query:
                del query["metadata"]
            queries.append(query)
    else:
        queries = docs
    return queries


def update_collection(
    collection: Collection[dict[str, Any]],
    docs: list[dict[str, Any]],
    with_metadata: bool,
) -> None:
    """
    Updates a collection by inserting documents if they do not already exist.[cite: 1]
    """
    logger.info(f"Updating {collection.name}...")
    queries = generate_queries(docs, with_metadata)

    for query, doc in zip(queries, docs):
        # find_one returns Optional[dict], which is handled by the 'if not' check[cite: 1]
        if not collection.find_one(query):
            collection.insert_one(doc)


def get_timeseries_collection(
    db: Database[dict[str, Any]], collection_name: str
) -> Collection[dict[str, Any]]:
    """
    Ensures a time-series collection exists or creates it with default parameters.[cite: 1]
    """
    collection_names = db.list_collection_names()
    if collection_name not in collection_names:
        # Mapping is used for read-only configuration dictionaries[cite: 1]
        timeseries: Mapping[str, Any] = {
            "timeField": "timestamp",
            "metaField": "metadata",
            "granularity": "hours",
        }
        return db.create_collection(collection_name, timeseries=timeseries)
    else:
        return db.get_collection(collection_name)


def get_latest_timestamp(
    collection: Collection[dict[str, Any]],
) -> Optional[datetime.datetime]:
    """
    Retrieves the most recent timestamp. Returns None if collection is empty.[cite: 1]
    """
    try:
        # Sort and limit to 1 for efficiency[cite: 1]
        latest_doc = collection.find().sort("timestamp", DESCENDING).limit(1)
        doc = next(latest_doc, None)
        return doc["timestamp"] if doc else None
    except (StopIteration, KeyError):
        return None


def get_daily_document(
    collection: Collection[dict[str, Any]], dt: datetime.datetime
) -> Optional[dict[str, Any]]:
    """
    Finds a document matching a specific timestamp.[cite: 1]
    """
    return collection.find_one({"timestamp": dt})


def count_documents(collection: Collection[dict[str, Any]]) -> int:
    """
    Returns total document count in collection.[cite: 1]
    """
    return collection.count_documents({})


if __name__ == "__main__":
    # Standard logging configuration for script execution[cite: 1]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    # Example usage
    client, db = connect_initial(db_name="test_db")

    # Close connection when finished
    client.close()
    logger.info("Connection closed.")
