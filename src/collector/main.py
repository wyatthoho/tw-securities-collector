import logging
import os
import sys
import time
from datetime import date, datetime
from typing import TypedDict

import pandas as pd
from dotenv import load_dotenv

from collector.mongodb_handler import MongoHandler
from collector.security_crawler import SecurityCrawler


class ListingDoc(TypedDict):
    有價證券代號: str
    有價證券名稱: str
    市場別: str
    有價證券別: str
    產業別: str
    CFICode: str
    備註: str


class TimeseriesDoc(TypedDict):
    code: str
    opening_price: float
    closing_price: float
    lowest_price: float
    highest_price: float
    price_change: float
    trade_count: int
    trade_shares: int
    trade_value: int
    timestamp: datetime
    note: str


load_dotenv()
MONGODB_URL = os.environ.get("MONGODB_URL")
TRACEABLE_DATE = date(2010, 1, 4)
TODAY = date.today()
MIN_CYCLE_SECONDS = 5


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class DataFrameConverter:
    def to_documents(self, df: pd.DataFrame) -> list[ListingDoc]:
        return [row.to_dict() for _, row in df.iterrows()]

    def to_timeseries(self, df: pd.DataFrame) -> list[TimeseriesDoc]:
        docs = []
        for _, row in df.iterrows():
            docs.append(
                {
                    "code": row["code"],
                    "opening_price": float(row["開盤價"]),
                    "closing_price": float(row["收盤價"]),
                    "lowest_price": float(row["最低價"]),
                    "highest_price": float(row["最高價"]),
                    "price_change": float(row["漲跌價差"]),
                    "trade_count": self._parse_int(row["成交筆數"]),
                    "trade_shares": self._parse_int(row["成交股數"]),
                    "trade_value": self._parse_int(row["成交金額"]),
                    "timestamp": self._roc_date_to_datetime(row["日期"]),
                    "note": row["註記"],
                }
            )
        return docs

    @staticmethod
    def _parse_int(value: str) -> int:
        return int(value.replace(",", ""))

    @staticmethod
    def _roc_date_to_datetime(roc_date: str) -> datetime:
        year, month, day = map(int, roc_date.split("/"))
        return datetime(year + 1911, month, day)


def next_month(_date: date) -> date:
    if _date.month < 12:
        return date(_date.year, _date.month + 1, 1)
    return date(_date.year + 1, 1, 1)


def throttle(elapsed: float) -> None:
    remaining = MIN_CYCLE_SECONDS - elapsed
    if remaining > 0:
        time.sleep(remaining)


def main():
    logger.info("Start!")

    mongo = MongoHandler(url=MONGODB_URL)
    crawler = SecurityCrawler()
    converter = DataFrameConverter()

    mongo.upload_listings(converter.to_documents(crawler.fetch_listings()))

    for doc in mongo.cl_listings.find():
        code = doc["有價證券代號"]
        birth_date = mongo.get_birth_date(code)
        record_date = mongo.get_record_date(code) or TRACEABLE_DATE
        start_date = max(birth_date, record_date, TRACEABLE_DATE)

        while start_date < TODAY:
            t0 = time.time()

            prices = crawler.fetch_monthly_prices(code=code, date_tgt=start_date)
            mongo.upload_daily(docs=converter.to_timeseries(prices))

            start_date = next_month(start_date)
            throttle(elapsed=time.time() - t0)

    mongo.close()
    logger.info("Done!")


if __name__ == "__main__":
    main()
