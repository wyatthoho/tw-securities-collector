import datetime
import functools
import logging
import time
from collections.abc import Callable
from typing import TypedDict

URL_FETCH_EX_RIGHTS = "..."
HEADERS_EX_RIGHTS = {
    "user-agent": "...",
}

COLUMN_MAPPING_EX_RIGHTS = {
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
    def wrapper(self: "ExRightsCrawler", *args, **kwargs) -> T:
        last_error: Exception | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return func(self, *args, **kwargs)
            except ExRightsResponseError as e:
                last_error = e
                logger.warning(f"Got invalid ex-rights response: {e}")
            except ExRightsHTTPError as e:
                last_error = e
                logger.warning(f"Fetch ex-rights events failed: {e}")

            if attempt == MAX_ATTEMPTS:
                break

            logger.info(f"Sleeping for {BACKOFF_SECONDS} seconds...")
            time.sleep(BACKOFF_SECONDS)
            logger.info(f"Retrying (attempt {attempt}/{MAX_ATTEMPTS})...")

        raise FetchExRightsEventsError(f"{last_error}") from last_error

    return wrapper


class ExRightsHTTPError(Exception):
    """Raised when a HTTP error occurs while fetching ex-rights events."""


class ExRightsResponseError(Exception):
    """Raised when the response content fails schema/data validation."""


class FetchExRightsEventsError(Exception):
    """Raised when fetching ex-rights events fails after exhausting all retry attempts."""


class ExRightsEvent(TypedDict):
    security_code: str
    event_date: datetime.date
    prior_closing_price: float
    reference_price: float


class ExRightsCrawler:
    def __init__(self):
        logger.info("Initialized ExRightsCrawler.")

    @staticmethod
    def _examine_response_content(content) -> None:
        """Validate that the response contains the fields required to assemble events."""

    def _assemble_ex_rights_events(self, content) -> list[ExRightsEvent]:
        """Parse the raw response content into a list of ExRightsEvent."""

    @with_retry
    def fetch_ex_rights_events(self, date_tgt: datetime.date) -> list[ExRightsEvent]:
        """Fetch ex-rights events for the given date."""
