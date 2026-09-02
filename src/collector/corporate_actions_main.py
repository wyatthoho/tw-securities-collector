import datetime
import logging
import os
import sys
import zoneinfo

from dotenv import load_dotenv

from collector.ex_rights_crawler import ExRightsCrawler, FetchExRightsEventsError
from collector.postgres_handler import PostgresConnectionError, PostgresHandler
from collector.split_crawler import FetchSplitEventsError, SplitCrawler

load_dotenv()
POSTGRES_URL = os.environ.get("POSTGRES_URL")
TIMEZONE = zoneinfo.ZoneInfo("Asia/Taipei")
TODAY = datetime.datetime.now(tz=TIMEZONE).date()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("run_corporate_actions.log", encoding="utf-8"),
    ],
)

logger = logging.getLogger(__name__)


def run_ex_rights(crawler: ExRightsCrawler, postgres: PostgresHandler) -> None:
    logger.info("Fetching ex-rights events...")
    try:
        events = crawler.fetch_ex_rights_events(TODAY)
        inserted_count = postgres.upload_ex_rights_events(events)
        logger.info(f"Uploaded {inserted_count} ex-rights events.")
    except FetchExRightsEventsError as e:
        logger.error(f"Failed to fetch ex-rights events.\n\n{e}\n")


def run_split(crawler: SplitCrawler, postgres: PostgresHandler) -> None:
    logger.info("Fetching split events...")
    try:
        events = crawler.fetch_split_events(TODAY)
        inserted_count = postgres.upload_split_events(events)
        logger.info(f"Uploaded {inserted_count} split events.")
    except FetchSplitEventsError as e:
        logger.error(f"Failed to fetch split events.\n\n{e}\n")


def main():
    logger.info("Initializing corporate actions crawling pipeline...")
    ex_rights_crawler = ExRightsCrawler()
    split_crawler = SplitCrawler()
    postgres = PostgresHandler(url=POSTGRES_URL)

    try:
        run_ex_rights(ex_rights_crawler, postgres)
        run_split(split_crawler, postgres)
    except PostgresConnectionError as e:
        logger.error(f"Postgres connection error.\n\n{e}\n")
        postgres.close()
        sys.exit(1)

    postgres.close()
    logger.info("Pipeline execution completed successfully.")


if __name__ == "__main__":
    main()
