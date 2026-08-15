import datetime
import logging

import psycopg2
import psycopg2.extras

from collector.security_crawler import DailyBar, Security

TABLE_SECURITIES = "securities"
TABLE_DAILY_BARS = "daily_bars"

logger = logging.getLogger(__name__)


class PostgresHandler:
    def __init__(self, url: str):
        self.conn = psycopg2.connect(url)
        self.conn.autocommit = True
        logger.info("Connected to PostgreSQL.")

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

    def fetch_securities(self) -> list[Security]:
        columns = ", ".join(f'"{col}"' for col in Security.__annotations__)
        sql = f"""
            SELECT {columns}
            FROM {TABLE_SECURITIES}
            ORDER BY security_code
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()

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

    def upload_daily_bars(
        self, security_code: str, fetch_date_str: str, daily_bars: list[DailyBar]
    ) -> int:
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

        logger.info(
            f"Uploaded {len(inserted)} daily bars for {security_code} in {fetch_date_str}."
        )
        return len(inserted)

    def close(self) -> None:
        self.conn.close()
        logger.info("PostgreSQL connection closed.")
