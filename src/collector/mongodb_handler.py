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
        self.cl_listings = self.db.get_collection(CL_LISTINGS)
        self.cl_daily = self.db.get_collection(CL_DAILY)

    def upload_listings(self, docs: list[dict]):
        inserted_count = 0
        for doc in docs:
            queried = self.cl_listings.find_one(doc)
            if not queried:
                self.cl_listings.insert_one(doc)
                inserted_count += 1
        if inserted_count > 0:
            logger.info(f"Uploaded {inserted_count} new security listings to MongoDB.")

    def upload_daily(self, docs: list[dict]):
        inserted_count = 0
        for doc in docs:
            queried = self.cl_daily.find_one(doc)
            if not queried:
                self.cl_daily.insert_one(doc)
                inserted_count += 1
        if inserted_count > 0:
            logger.info(f"Saved {inserted_count} new daily price records.")

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
