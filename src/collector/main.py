import datetime
import logging
import os
import random
import sys
import time
import zoneinfo

from dotenv import load_dotenv

from collector.postgres_handler import PostgresConnectionError, PostgresHandler
from collector.security_crawler import FetchDailyBarsError, SecurityCrawler

load_dotenv()
POSTGRES_URL = os.environ.get("POSTGRES_URL")
TRACEABLE_DATE = datetime.date(2010, 1, 4)
TIMEZONE = zoneinfo.ZoneInfo("Asia/Taipei")
TODAY = datetime.datetime.now(tz=TIMEZONE).date()
MIN_CYCLE_SECONDS = 7
MAX_CYCLE_SECONDS = 14


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("run.log", encoding="utf-8"),
    ],
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
    crawler = SecurityCrawler()
    postgres = PostgresHandler(url=POSTGRES_URL)

    # Fetch and upload securities
    securities = crawler.fetch_securities()
    postgres.upload_securities(securities)

    # Fetch and upload daily bars
    securities = postgres.fetch_securities()
    securities_count = len(securities)
    for idx, security in enumerate(securities, 1):
        security_code = security["security_code"]
        listing_date = security["listing_date"]
        record_date = postgres.get_record_date(security_code) or TRACEABLE_DATE
        fetch_date = max(listing_date, record_date, TRACEABLE_DATE)

        if fetch_date >= TODAY:
            continue

        logger.info(f"[{idx}/{securities_count}] Processing {security_code}")

        while fetch_date < TODAY:
            fetch_date_str = fetch_date.strftime("%Y-%m")
            t0 = time.time()
            success = False

            try:
                daily_bars = crawler.fetch_daily_bars(security_code, fetch_date)
                if not daily_bars:
                    logger.warning(
                        f"No data returned for {security_code} in {fetch_date_str}"
                    )
                    success = True
                    break

                postgres.upload_daily_bars(security_code, fetch_date_str, daily_bars)
                success = True
            except FetchDailyBarsError as e:
                logger.error(
                    f"Failed for {security_code} in {fetch_date_str}.\n\n{e}\n"
                )
                break
            except PostgresConnectionError as e:
                logger.error(f"Postgres connection error.\n\n{e}\n")
                break

            fetch_date = next_month(fetch_date)
            throttle(elapsed=time.time() - t0)

        if not success:
            logger.info("Aborting pipeline...")
            postgres.close()
            sys.exit(1)

    postgres.close()
    logger.info("Pipeline execution completed successfully.")


if __name__ == "__main__":
    main()
