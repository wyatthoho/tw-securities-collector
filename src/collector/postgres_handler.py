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
COLUMNS_DAILY = [
    "code",
    "trade_date",
    "opening_price",
    "closing_price",
    "lowest_price",
    "highest_price",
    "price_change",
    "trade_count",
    "trade_shares",
    "trade_value",
    "note",
]

logger = logging.getLogger(__name__)


class PostgresHandler:
    def __init__(self, url: str):
        self.conn = psycopg2.connect(url)
        self.conn.autocommit = True
        logger.info("Connected to PostgreSQL.")

    def upload_listings(self, rows: list[dict]) -> None:
        if not rows:
            return

        columns = ", ".join(f'"{col}"' for col in COLUMNS_LISTINGS)
        sql = f"""
            INSERT INTO {TABLE_NAME_LISTINGS} ({columns})
            VALUES %s
            ON CONFLICT ("有價證券代號") DO NOTHING
        """
        values = [tuple(row[col] for col in COLUMNS_LISTINGS) for row in rows]
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)

        logger.info(f"Synchronized {len(rows)} security listings.")

    def fetch_listings(self) -> list[dict]:
        columns = ", ".join(f'"{col}"' for col in COLUMNS_LISTINGS)
        sql = f"""
            SELECT {columns}
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

    def upload_daily_prices(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        columns = ", ".join(COLUMNS_DAILY)
        sql = f"""
            INSERT INTO {TABLE_NAME_DAILY} ({columns})
            VALUES %s
            ON CONFLICT (code, trade_date) DO NOTHING
            RETURNING code
        """
        values = [tuple(row[col] for col in COLUMNS_DAILY) for row in rows]
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values)
            inserted = cur.fetchall()

        logger.info(f"Uploaded {len(inserted)} daily price records.")
        return len(inserted)

    def close(self) -> None:
        self.conn.close()
        logger.info("PostgreSQL connection closed.")
