import datetime
import logging
import os
import random
import sys
import time
from typing import TypedDict

import pandas as pd
from dotenv import load_dotenv

from collector.postgres_handler import PostgresHandler, COLUMNS_LISTINGS
from collector.security_crawler import HTTPError, ResponseError, SecurityCrawler


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
    price_change: str
    trade_count: int
    trade_shares: int
    trade_value: int
    timestamp: datetime.datetime
    note: str


load_dotenv()
POSTGRES_URL = os.environ.get("POSTGRES_URL")
TRACEABLE_DATE = datetime.date(2010, 1, 4)
TODAY = datetime.date.today()
MIN_CYCLE_SECONDS = 7
MAX_CYCLE_SECONDS = 14
FETCH_RETRY_BACKOFF = [360, 360, 360, 360, 360]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    encoding="utf-8",
)

logger = logging.getLogger(__name__)


class DataFrameConverter:
    def to_listing_rows(self, df: pd.DataFrame) -> list[tuple]:
        return list(df[COLUMNS_LISTINGS].itertuples(index=False, name=None))

    # def to_timeseries(self, df: pd.DataFrame) -> list[TimeseriesDocument]:
    #     """
    #     Converts TWSE raw DataFrame into a list of TimeseriesDocuments.

    #     Notes on excluded fields:
    #     - '漲跌價差' (Price Change): Excluded due to non-numeric indicators (+, -, X).
    #     'X' denotes ex-dividend/ex-rights days, which breaks direct numeric parsing.
    #     Derive from 'closing_price' if historical changes are required.
    #     - '註記' (Notes): Omitted because it is missing from certain TWSE API
    #     responses, causing KeyErrors. It also holds low quantitative value
    #     (used mainly for rare events like stock splits or par value changes).
    #     """
    #     docs = []
    #     for _, row in df.iterrows():
    #         # Skip non-trading days (e.g. 0051) where all price fields are "--":
    #         # ["日期",       "成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌價差", "成交筆數", "註記"]
    #         # ["107/03/31",        "0",       "0",     "--",    "--",    "--",     "--",   " 0.00",       "0",     ""]
    #         if row["開盤價"] == row["收盤價"] == row["最低價"] == row["最高價"] == "--":
    #             date = self._roc_date_to_datetime(row["日期"]).date()
    #             msg = f"Skipping non-trading day for {row['code']} on {date}"
    #             logger.warning(msg)
    #             continue

    #         docs.append(
    #             {
    #                 "code": row["code"],
    #                 "opening_price": float(self._remove_separator(row["開盤價"])),
    #                 "closing_price": float(self._remove_separator(row["收盤價"])),
    #                 "lowest_price": float(self._remove_separator(row["最低價"])),
    #                 "highest_price": float(self._remove_separator(row["最高價"])),
    #                 "price_change": row["漲跌價差"],
    #                 "trade_count": int(self._remove_separator(row["成交筆數"])),
    #                 "trade_shares": int(self._remove_separator(row["成交股數"])),
    #                 "trade_value": int(self._remove_separator(row["成交金額"])),
    #                 "timestamp": self._roc_date_to_datetime(row["日期"]),
    #                 "note": row["註記"],
    #             }
    #         )
    #     return docs

    # @staticmethod
    # def _remove_separator(value: str) -> str:
    #     return value.replace(",", "")

    # @staticmethod
    # def _roc_date_to_datetime(roc_date: str) -> datetime.datetime:
    #     year, month, day = map(int, roc_date.split("/"))
    #     return datetime.datetime(year + 1911, month, day)


# def next_month(_date: datetime.date) -> datetime.date:
#     if _date.month < 12:
#         return datetime.date(_date.year, _date.month + 1, 1)
#     return datetime.date(_date.year + 1, 1, 1)


# def throttle(elapsed: float) -> None:
#     cycle_seconds = random.randint(MIN_CYCLE_SECONDS, MAX_CYCLE_SECONDS)
#     remaining = cycle_seconds - elapsed
#     if remaining > 0:
#         time.sleep(remaining)


def main():
    logger.info("Initializing Taiwan stock crawling pipeline...")

    db_handler = PostgresHandler(url=POSTGRES_URL)
    crawler = SecurityCrawler()
    converter = DataFrameConverter()

    # Fetch and sync listings
    listings_df = crawler.fetch_listings()
    listings_count = len(listings_df)
    db_handler.upload_listings(converter.to_listing_rows(listings_df))
    logger.info(f"Synchronized {listings_count} security listings.")

    # listings = db_handler.cl_listings.find().to_list()

    # for idx, doc in enumerate(listings, 1):
    #     code = doc["有價證券代號"]
    #     birth_date = db_handler.get_birth_date(code)
    #     record_date = db_handler.get_record_date(code) or TRACEABLE_DATE
    #     fetch_date = max(birth_date, record_date, TRACEABLE_DATE)

    #     if fetch_date >= TODAY:
    #         continue

    #     logger.info(f"[{idx}/{listings_count}] Processing {code}")

    #     while fetch_date < TODAY:
    #         date_str = fetch_date.strftime("%Y-%m")
    #         success = False

    #         for backoff in FETCH_RETRY_BACKOFF:
    #             t0 = time.time()
    #             try:
    #                 prices = crawler.fetch_monthly_prices(code=code, date_tgt=fetch_date)
    #                 if prices.empty:
    #                     logger.warning(f"No data returned for {code} in {date_str}")
    #                     success = True
    #                     break

    #                 count = db_handler.insert_absent_docs(converter.to_timeseries(prices))
    #                 logger.info(f"Uploaded {count} daily prices for {code} in {date_str}")
    #                 success = True
    #                 break
    #             except (ResponseError, HTTPError) as e:
    #                 logger.warning(f"Failed attempt for {code} in {date_str}. Backoff: {backoff}.\n\n{str(e)}\n")
    #                 time.sleep(backoff)
    #                 continue
    #             except Exception as e:
    #                 logger.exception(f"Unexpected error for {code} in {date_str}")
    #                 break

    #         if not success:
    #             logger.error(f"Stopped for {code} in {date_str}")
    #             sys.exit(1)

    #         fetch_date = next_month(fetch_date)
    #         throttle(elapsed=time.time() - t0)

    # db_handler.close()
    # logger.info("Pipeline execution completed successfully.")


if __name__ == "__main__":
    main()
