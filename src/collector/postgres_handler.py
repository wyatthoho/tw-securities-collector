import datetime
import logging

import psycopg2
import psycopg2.extras

TABLE_NAME_LISTINGS = "listings"
TABLE_NAME_DAILY = "daily_prices"
COLUMNS_LISTINGS = [
    "國際證券編碼",
    "有價證券代號",
    "有價證券名稱",
    "市場別",
    "有價證券別",
    "產業別",
    "公開發行/上市(櫃)/發行日",
    "CFICode",
    "備 註",
]

logger = logging.getLogger(__name__)


class PostgresHandler:
    def __init__(self, url: str):
        self.conn = psycopg2.connect(url)
        self.conn.autocommit = True
        logger.info("Connected to PostgreSQL.")

    def upload_listings(self, rows: list[tuple]) -> None:
        if not rows:
            return

        columns_sql = ", ".join(f'"{col}"' for col in COLUMNS_LISTINGS)
        sql = f"""
            INSERT INTO {TABLE_NAME_LISTINGS} ({columns_sql})
            VALUES %s
            ON CONFLICT ("有價證券代號") DO NOTHING
        """
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows)

        logger.info(f"Synchronized {len(rows)} security listings.")

    def fetch_listings(self) -> list[dict]:
        columns_sql = ", ".join(f'"{col}"' for col in COLUMNS_LISTINGS)
        sql = f"""
            SELECT {columns_sql}
            FROM {TABLE_NAME_LISTINGS}
            ORDER BY "有價證券代號"
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return cur.fetchall()

    def get_record_date(self, code: str) -> datetime.date | None:
        sql = f"""
            SELECT MAX(trade_date)
            FROM {TABLE_NAME_DAILY}
            WHERE code = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (code,))
            row = cur.fetchone()
        return row[0] if row and row[0] else None
