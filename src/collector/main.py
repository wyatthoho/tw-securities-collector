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

    # Fetch and sync listings
    listings = crawler.fetch_listings()
    postgres.upload_listings(listings)

    listings = postgres.fetch_listings()
    listings_count = len(listings)

    for idx, listing in enumerate(listings, 1):
        security_code = listing["security_code"]
        listing_date = listing["listing_date"]
        record_date = postgres.get_record_date(security_code) or TRACEABLE_DATE
        fetch_date = max(listing_date, record_date, TRACEABLE_DATE)

        if fetch_date >= TODAY:
            continue

        logger.info(f"[{idx}/{listings_count}] Processing {security_code}")

        while fetch_date < TODAY:
            date_str = fetch_date.strftime("%Y-%m")
            success = False

            for backoff in FETCH_RETRY_BACKOFF:
                t0 = time.time()
                try:
                    prices = crawler.fetch_daily_prices(security_code, fetch_date)
                    if not prices:
                        logger.warning(
                            f"No data returned for {security_code} in {date_str}"
                        )
                        success = True
                        break

                    postgres.upload_daily_prices(prices)
                    success = True
                    break
                except (ResponseError, TWSEHTTPError) as e:
                    logger.warning(
                        f"Failed attempt for {security_code} in {date_str}. Backoff: {backoff}.\n\n{e}\n"
                    )
                    time.sleep(backoff)
                    continue
                except Exception as e:
                    logger.exception(
                        f"Unexpected error for {security_code} in {date_str}"
                    )
                    break

            if not success:
                logger.error(f"Stopped for {security_code} in {date_str}")
                sys.exit(1)

            fetch_date = next_month(fetch_date)
            throttle(elapsed=time.time() - t0)

    postgres.close()
    logger.info("Pipeline execution completed successfully.")


if __name__ == "__main__":
    main()
