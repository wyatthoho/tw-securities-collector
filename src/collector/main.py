import datetime
import logging
import os
import random
import sys
import time
import zoneinfo

from dotenv import load_dotenv

from collector.postgres_handler import PostgresHandler
from collector.security_crawler import ResponseError, SecurityCrawler, TWSEHTTPError

load_dotenv()
POSTGRES_URL = os.environ.get("POSTGRES_URL")
TRACEABLE_DATE = datetime.date(2010, 1, 4)
TIMEZONE = zoneinfo.ZoneInfo("Asia/Taipei")
TODAY = datetime.datetime.now(tz=TIMEZONE).date()
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
    def to_daily_rows(self, rows: list[dict]) -> list[dict]:
        """
        Converts TWSE raw rows into daily price records keyed by DB column names.

        Notes on excluded fields:
        - '漲跌價差' (Price Change): Excluded due to non-numeric indicators (+, -, X).
        'X' denotes ex-dividend/ex-rights days, which breaks direct numeric parsing.
        Derive from 'closing_price' if historical changes are required.
        - '註記' (Notes): Omitted because it is missing from certain TWSE API
        responses, causing KeyErrors. It also holds low quantitative value
        (used mainly for rare events like stock splits or par value changes).
        """
        docs = []
        for row in rows:
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
                    "trade_date": self._roc_date_to_datetime(row["日期"]),
                    "opening_price": float(self._remove_separator(row["開盤價"])),
                    "closing_price": float(self._remove_separator(row["收盤價"])),
                    "lowest_price": float(self._remove_separator(row["最低價"])),
                    "highest_price": float(self._remove_separator(row["最高價"])),
                    "price_change": row["漲跌價差"],
                    "trade_count": int(self._remove_separator(row["成交筆數"])),
                    "trade_shares": int(self._remove_separator(row["成交股數"])),
                    "trade_value": int(self._remove_separator(row["成交金額"])),
                    "note": row["註記"],
                }
            )
        return docs

    @staticmethod
    def _remove_separator(value: str) -> str:
        return value.replace(",", "")

    @staticmethod
    def _roc_date_to_datetime(roc_date: str) -> datetime.datetime:
        year, month, day = map(int, roc_date.split("/"))
        return datetime.datetime(year + 1911, month, day, tzinfo=TIMEZONE)


def next_month(_date: datetime.date) -> datetime.date:
    if _date.month < 12:
        return datetime.date(_date.year, _date.month + 1, 1)
    return datetime.date(_date.year + 1, 1, 1)


def throttle(elapsed: float) -> None:
    cycle_seconds = random.randint(MIN_CYCLE_SECONDS, MAX_CYCLE_SECONDS)
    remaining = cycle_seconds - elapsed
    if remaining > 0:
        time.sleep(remaining)


def main():
    logger.info("Initializing Taiwan stock crawling pipeline...")

    postgres = PostgresHandler(url=POSTGRES_URL)
    crawler = SecurityCrawler()
    converter = DataFrameConverter()

    # Fetch and sync listings
    listings = crawler.fetch_listings()
    postgres.upload_listings(listings)

    # listings = postgres.fetch_listings()
    # listings_count = len(listings)

    # for idx, listing in enumerate(listings, 1):
    #     code = listing["有價證券代號"]
    #     birth_date = (
    #         datetime.datetime.strptime(
    #             listing["公開發行/上市(櫃)/發行日"],
    #             "%Y/%m/%d",
    #         )
    #         .replace(tzinfo=TIMEZONE)
    #         .date()
    #     )
    #     record_date = postgres.get_record_date(code) or TRACEABLE_DATE
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
    #                 prices = crawler.fetch_daily_prices_by_month(
    #                     code=code, date_tgt=fetch_date
    #                 )
    #                 if not prices:
    #                     logger.warning(f"No data returned for {code} in {date_str}")
    #                     success = True
    #                     break

    #                 postgres.upload_daily_prices(converter.to_daily_rows(prices))
    #                 success = True
    #                 break
    #             except (ResponseError, TWSEHTTPError) as e:
    #                 logger.warning(
    #                     f"Failed attempt for {code} in {date_str}. Backoff: {backoff}.\n\n{e}\n"
    #                 )
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

    # postgres.close()
    # logger.info("Pipeline execution completed successfully.")


if __name__ == "__main__":
    main()
