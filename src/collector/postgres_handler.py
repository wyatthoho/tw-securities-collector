import datetime
import logging

import psycopg2
import psycopg2.extras

from collector.security_crawler import Daily, Listing

TABLE_LISTINGS = "listings"
TABLE_DAILY = "daily_prices"

logger = logging.getLogger(__name__)


class PostgresHandler:
    def __init__(self, url: str):
        self.conn = psycopg2.connect(url)
        self.conn.autocommit = True
        logger.info("Connected to PostgreSQL.")

    def upload_listings(self, listings: list[Listing]) -> None:
        if not listings:
            return

        columns = ", ".join(f'"{col}"' for col in Listing.__annotations__)
        sql = f"""
            INSERT INTO {TABLE_LISTINGS} ({columns})
            VALUES %s
            ON CONFLICT (security_code) DO NOTHING
        """
        values = [tuple(listing[col] for col in listing) for listing in listings]
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)

        logger.info(f"Synchronized {len(listings)} security listings.")

    def fetch_listings(self) -> list[Listing]:
        columns = ", ".join(f'"{col}"' for col in Listing.__annotations__)
        sql = f"""
            SELECT {columns}
            FROM {TABLE_LISTINGS}
            ORDER BY security_code
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()

    def get_record_date(self, security_code: str) -> datetime.date | None:
        sql = f"""
            SELECT MAX(trade_date)
            FROM {TABLE_DAILY}
            WHERE security_code = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (security_code,))
            row = cur.fetchone()
        return row[0] if row and row[0] else None

    def upload_daily_prices(self, security_code: str, fetch_date_str: str, rows: list[Daily]) -> int:
        if not rows:
            return 0

        columns = ", ".join(Daily.__annotations__)
        sql = f"""
            INSERT INTO {TABLE_DAILY} ({columns})
            VALUES %s
            ON CONFLICT (security_code, trade_date) DO NOTHING
            RETURNING security_code
        """
        values = [tuple(daily[col] for col in daily) for daily in rows]
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)
            inserted = cur.fetchall()

        logger.info(f"Uploaded {len(inserted)} daily price records for {security_code} in {fetch_date_str}.")
        return len(inserted)

    def close(self) -> None:
        self.conn.close()
        logger.info("PostgreSQL connection closed.")
