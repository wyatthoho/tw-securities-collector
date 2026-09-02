import datetime
import functools
import logging
import time
from collections.abc import Callable

import psycopg2
import psycopg2.extras
from psycopg2 import InterfaceError, OperationalError

from collector.ex_rights_crawler import ExRightsEvent
from collector.security_crawler import DailyBar, Security
from collector.split_crawler import SplitEvent

TABLE_SECURITIES = "securities"
TABLE_DAILY_BARS = "daily_bars"
TABLE_EX_RIGHTS_EVENTS = "ex_rights_events"
TABLE_SPLIT_EVENTS = "split_events"
MAX_ATTEMPTS = 5
BACKOFF_SECONDS = 3

logger = logging.getLogger(__name__)


class PostgresConnectionError(Exception):
    """Raised when the PostgreSQL connection drops and cannot serve the request."""


def with_retry[T](func: Callable[..., T]) -> Callable[..., T]:
    @functools.wraps(func)
    def wrapper(self: "PostgresHandler", *args, **kwargs) -> T:
        last_error: Exception | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return func(self, *args, **kwargs)
            except (OperationalError, InterfaceError) as e:
                last_error = e
                logger.warning(f"Postgres connection error: {e}")

            if attempt == MAX_ATTEMPTS:
                break

            time.sleep(BACKOFF_SECONDS)
            logger.info(
                f"Reconnecting to PostgreSQL (attempt {attempt}/{MAX_ATTEMPTS})..."
            )
            try:
                self.reconnect()
            except (OperationalError, InterfaceError) as e:
                last_error = e

        raise PostgresConnectionError(f"{last_error}") from last_error

    return wrapper


class PostgresHandler:
    def __init__(self, url: str):
        self.url = url
        self.conn = psycopg2.connect(url)
        self.conn.autocommit = True
        logger.info("Connected to PostgreSQL.")

    def reconnect(self) -> None:
        try:
            self.conn.close()
        except psycopg2.Error as e:
            logger.debug(f"Ignoring error while closing broken connection: {e}")

        self.conn = psycopg2.connect(self.url)
        self.conn.autocommit = True
        logger.info("Reconnected to PostgreSQL.")

    @with_retry
    def upload_securities(self, securities: list[Security]) -> None:
        if not securities:
            return

        columns = ", ".join(f'"{col}"' for col in Security.__annotations__)
        sql = f"""
            INSERT INTO {TABLE_SECURITIES} ({columns})
            VALUES %s
            ON CONFLICT (security_code) DO NOTHING
        """
        values = [tuple(security[col] for col in security) for security in securities]

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)

        logger.info(f"Synchronized {len(securities)} securities.")

    @with_retry
    def fetch_securities(self) -> list[Security]:
        logger.info("Fetching securities from databse...")
        columns = ", ".join(f'"{col}"' for col in Security.__annotations__)
        sql = f"""
            SELECT {columns}
            FROM {TABLE_SECURITIES}
            ORDER BY security_code
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()

    @with_retry
    def get_record_date(self, security_code: str) -> datetime.date | None:
        sql = f"""
            SELECT MAX(trade_date)
            FROM {TABLE_DAILY_BARS}
            WHERE security_code = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (security_code,))
            row = cur.fetchone()
        return row[0] if row and row[0] else None

    @with_retry
    def upload_daily_bars(self, daily_bars: list[DailyBar]) -> int:
        if not daily_bars:
            return 0

        columns = ", ".join(DailyBar.__annotations__)
        sql = f"""
            INSERT INTO {TABLE_DAILY_BARS} ({columns})
            VALUES %s
            ON CONFLICT (security_code, trade_date) DO NOTHING
            RETURNING security_code
        """
        values = [
            tuple(daily_bar[col] for col in daily_bar) for daily_bar in daily_bars
        ]

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)
            inserted = cur.fetchall()

        return len(inserted)

    @with_retry
    def upload_ex_rights_events(self, events: list[ExRightsEvent]) -> int:
        if not events:
            return 0

        columns = ", ".join(ExRightsEvent.__annotations__)
        sql = f"""
            INSERT INTO {TABLE_EX_RIGHTS_EVENTS} ({columns})
            VALUES %s
            ON CONFLICT (security_code, event_date) DO NOTHING
            RETURNING security_code
        """
        values = [tuple(event[col] for col in event) for event in events]

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)
            inserted = cur.fetchall()

        return len(inserted)

    @with_retry
    def get_last_ex_rights_date(self, security_code: str) -> datetime.date | None:
        sql = f"""
            SELECT MAX(event_date)
            FROM {TABLE_EX_RIGHTS_EVENTS}
            WHERE security_code = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (security_code,))
            row = cur.fetchone()
        return row[0] if row and row[0] else None

    @with_retry
    def upload_split_events(self, events: list[SplitEvent]) -> int:
        if not events:
            return 0

        columns = ", ".join(SplitEvent.__annotations__)
        sql = f"""
            INSERT INTO {TABLE_SPLIT_EVENTS} ({columns})
            VALUES %s
            ON CONFLICT (security_code, event_date) DO NOTHING
            RETURNING security_code
        """
        values = [tuple(event[col] for col in event) for event in events]

        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)
            inserted = cur.fetchall()

        return len(inserted)

    @with_retry
    def get_last_split_date(self, security_code: str) -> datetime.date | None:
        sql = f"""
            SELECT MAX(event_date)
            FROM {TABLE_SPLIT_EVENTS}
            WHERE security_code = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (security_code,))
            row = cur.fetchone()
        return row[0] if row and row[0] else None

    def close(self) -> None:
        self.conn.close()
        logger.info("PostgreSQL connection closed.")
