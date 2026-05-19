import logging
from datetime import date, datetime

from pymongo import MongoClient


logger = logging.getLogger(__name__)


DB_STOCK = "tw_stock"
CL_LISTINGS = "listings"
CL_DAILY = "daily_prices"


class MongoHandler:
    def __init__(self, url: str):
        self.client = MongoClient(url)
        logger.info(f"Connected to MongoDB (nodes: {self.client.nodes})")

        self.db = self.client.get_database(DB_STOCK)
        logger.info(f"Using database: '{self.db.name}'")

        self.cl_listings = self.db.get_collection(CL_LISTINGS)
        logger.info(f"Using collection: '{self.cl_listings.name}'")

        self.cl_daily = self.db.get_collection(CL_DAILY)
        logger.info(f"Using collection: '{self.cl_daily.name}'")

    def upload_listings(self, docs: list[dict]):
        for doc in docs:
            queried = self.cl_listings.find_one(doc)
            if queried:
                logger.info(f"Document already exists. ID: {queried['_id']}")
            else:
                result = self.cl_listings.insert_one(doc)
                logger.info(f"Document inserted. ID: {result.inserted_id}")

    def upload_daily(self, docs: list[dict]):
        for doc in docs:
            queried = self.cl_daily.find_one(doc)
            if queried:
                logger.info(f"Document already exists. ID: {queried['_id']}")
            else:
                result = self.cl_daily.insert_one(doc)
                logger.info(f"Document inserted. ID: {result.inserted_id}")

    def get_birth_date(self, code: str) -> date:
        doc = self.cl_listings.find_one({"有價證券代號": code})
        birth_date_str: str = doc["公開發行/上市(櫃)/發行日"]
        return datetime.strptime(birth_date_str, "%Y/%m/%d").date()

    def get_record_date(self, code: str) -> date | None:
        result = self.cl_daily.find_one(
            {"code": code},
            sort=[("timestamp", -1)],
            projection={"timestamp": 1, "_id": 0},
        )
        return result["timestamp"] if result else None

    def close(self):
        self.client.close()
        logger.info("MongoDB connection closed.")
