import logging
import os
import sys
import time
import datetime
from typing import TypedDict

import pandas as pd
from dotenv import load_dotenv

from collector.mongodb_handler import MongoHandler
from collector.security_crawler import SecurityCrawler


class ListingDocument(TypedDict):
    有價證券代號: str
    有價證券名稱: str
    市場別: str
    有價證券別: str
    產業別: str
    CFICode: str
    備註: str


class TimeseriesDocument(TypedDict):
    code: str
    opening_price: float
    closing_price: float
    lowest_price: float
    highest_price: float
    price_change: float
    trade_count: int
    trade_shares: int
    trade_value: int
    timestamp: datetime.datetime
    note: str


load_dotenv()
MONGODB_URL = os.environ.get("MONGODB_URL")
TRACEABLE_DATE = datetime.date(2010, 1, 4)
TODAY = datetime.date.today()
MIN_CYCLE_SECONDS = 5


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class DataFrameConverter:
    def to_documents(self, df: pd.DataFrame) -> list[ListingDocument]:
        return [row.to_dict() for _, row in df.iterrows()]

    def to_timeseries(self, df: pd.DataFrame) -> list[TimeseriesDocument]:
        """
        Converts TWSE raw DataFrame into a list of TimeseriesDocuments.

        Notes on excluded fields:
        - '漲跌價差' (Price Change): Excluded due to non-numeric indicators (+, -, X).
        'X' denotes ex-dividend/ex-rights days, which breaks direct numeric parsing.
        Derive from 'closing_price' if historical changes are required.
        - '註記' (Notes): Omitted because it is missing from certain TWSE API
        responses, causing KeyErrors. It also holds low quantitative value
        (used mainly for rare events like stock splits or par value changes).
        """
        docs = []
        for _, row in df.iterrows():
            # Skip non-trading days (e.g. 0051) where all price fields are "--":
            # ["日期",       "成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌價差", "成交筆數", "註記"]
            # ["107/03/31",        "0",       "0",     "--",    "--",    "--",     "--",   " 0.00",       "0",     ""]
            if row["開盤價"] == row["收盤價"] == row["最低價"] == row["最高價"] == "--":
                date = self._roc_date_to_datetime(row["日期"]).date()
                msg = f"Skipping non-trading day for {row['code']} on {date}"
                logger.warning(msg)
                continue

            docs.append(
                {
                    "code": row["code"],
                    "opening_price": float(row["開盤價"]),
                    "closing_price": float(row["收盤價"]),
                    "lowest_price": float(row["最低價"]),
                    "highest_price": float(row["最高價"]),
                    "price_change": row["漲跌價差"],
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
    def _roc_date_to_datetime(roc_date: str) -> datetime.datetime:
        year, month, day = map(int, roc_date.split("/"))
        return datetime.datetime(year + 1911, month, day)


def next_month(_date: datetime.date) -> datetime.date:
    if _date.month < 12:
        return datetime.date(_date.year, _date.month + 1, 1)
    return datetime.date(_date.year + 1, 1, 1)


def throttle(elapsed: float) -> None:
    remaining = MIN_CYCLE_SECONDS - elapsed
    if remaining > 0:
        time.sleep(remaining)


def main():
    logger.info("Initializing Taiwan stock crawling pipeline...")

    mongo = MongoHandler(url=MONGODB_URL)
    crawler = SecurityCrawler()
    converter = DataFrameConverter()

    # Fetch and sync listings
    listings_df = crawler.fetch_listings()
    listings_count = len(listings_df)
    mongo.upload_listings(converter.to_documents(listings_df))
    logger.info(f"Synchronized {listings_count} security listings.")

    for idx, doc in enumerate(mongo.cl_listings.find(), 1):
        code = doc["有價證券代號"]
        birth_date = mongo.get_birth_date(code)
        record_date = mongo.get_record_date(code) or TRACEABLE_DATE
        start_date = max(birth_date, record_date, TRACEABLE_DATE)

        if start_date >= TODAY:
            continue

        logger.info(f"[{idx}/{listings_count}] Processing {code}")

        while start_date < TODAY:
            t0 = time.time()
            date_str = start_date.strftime("%Y-%m")

            try:
                prices = crawler.fetch_monthly_prices(code=code, date_tgt=start_date)

                if not prices.empty:
                    count = mongo.insert_absent_docs(converter.to_timeseries(prices))
                    if count:
                        logger.info(
                            f"Uploaded {count} daily prices for {code} in {date_str}"
                        )
                    else:
                        logger.info(f"No new daily prices for {code} in {date_str}")
                else:
                    logger.warning(f"No data returned for {code} in {date_str}")

                start_date = next_month(start_date)

            except Exception as e:
                logger.error(f"Failed to fetch/save data for {code} in {date_str}: {e}")
                sys.exit()

            finally:
                throttle(elapsed=time.time() - t0)

    mongo.close()
    logger.info("Pipeline execution completed successfully.")


if __name__ == "__main__":
    main()
