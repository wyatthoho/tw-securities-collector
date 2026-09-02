import datetime
import functools
import logging
import time
from collections.abc import Callable
from typing import TypedDict

URL_FETCH_SPLIT = "..."
HEADERS_SPLIT = {
    "user-agent": "...",
}

COLUMN_MAPPING_SPLIT = {
    "...": "security_code",
    "...": "event_date",
    "...": "prior_closing_price",
    "...": "reference_price",
}

MAX_ATTEMPTS = 5
BACKOFF_SECONDS = 360

logger = logging.getLogger(__name__)


def with_retry[T](func: Callable[..., T]) -> Callable[..., T]:
    @functools.wraps(func)
    def wrapper(self: "SplitCrawler", *args, **kwargs) -> T:
        last_error: Exception | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return func(self, *args, **kwargs)
            except SplitResponseError as e:
                last_error = e
                logger.warning(f"Got invalid split response: {e}")
            except SplitHTTPError as e:
                last_error = e
                logger.warning(f"Fetch split events failed: {e}")

            if attempt == MAX_ATTEMPTS:
                break

            logger.info(f"Sleeping for {BACKOFF_SECONDS} seconds...")
            time.sleep(BACKOFF_SECONDS)
            logger.info(f"Retrying (attempt {attempt}/{MAX_ATTEMPTS})...")

        raise FetchSplitEventsError(f"{last_error}") from last_error

    return wrapper


class SplitHTTPError(Exception):
    """Raised when a HTTP error occurs while fetching split events."""


class SplitResponseError(Exception):
    """Raised when the response content fails schema/data validation."""


class FetchSplitEventsError(Exception):
    """Raised when fetching split events fails after exhausting all retry attempts."""


class SplitEvent(TypedDict):
    security_code: str
    event_date: datetime.date
    prior_closing_price: float
    reference_price: float


class SplitCrawler:
    def __init__(self):
        logger.info("Initialized SplitCrawler.")

    @staticmethod
    def _examine_response_content(content) -> None:
        """Validate that the response contains the fields required to assemble events."""

    def _assemble_split_events(self, content) -> list[SplitEvent]:
        """Parse the raw response content into a list of SplitEvent."""

    @with_retry
    def fetch_split_events(self, date_tgt: datetime.date) -> list[SplitEvent]:
        """Fetch split events for the given date."""
