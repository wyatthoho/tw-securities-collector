import logging
import os
import sys
import time
from datetime import date, datetime
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv

from collector import mongodb_handler as mongo
from collector import security_crawler as crawl


load_dotenv()
MONGODB_URL = os.environ.get("MONGODB_URL")
TRACEABLE_DATE = date(2010, 1, 4)
TODAY = date.today()
MIN_TIME_INC = 5


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def convert_dataframe_to_documents(df: pd.DataFrame) -> List[Dict]:
    return [row.to_dict() for _, row in df.iterrows()]


def convert_rocdate_to_utcdate(rocdate: str) -> datetime:
    year, month, day = map(int, rocdate.split("/"))
    return datetime(year + 1911, month, day)


def convert_dataframe_to_timeseries(df: pd.DataFrame) -> List[Dict]:
    docs = []
    for _, row in df.iterrows():
        doc = {
            "code": row["code"],
            "opening_price": float(row["開盤價"]),
            "closing_price": float(row["收盤價"]),
            "lowest_price": float(row["最低價"]),
            "highest_price": float(row["最高價"]),
            "price_change": float(row["漲跌價差"]),
            "trade_count": int(row["成交筆數"].replace(",", "")),
            "trade_shares": int(row["成交股數"].replace(",", "")),
            "trade_value": int(row["成交金額"].replace(",", "")),
            "timestamp": convert_rocdate_to_utcdate(row["日期"]),
            "note": row["註記"],
        }
        docs.append(doc)
    return docs


def get_next_month(_date: date) -> date:
    if _date.month < 12:
        return date(_date.year, _date.month + 1, 1)
    else:
        return date(_date.year + 1, 1, 1)


def main():
    logger.info("Start!")

    m_hdl = mongo.MongoHandler(url=MONGODB_URL)

    listing_df = crawl.fetch_listings()
    listing_docs = convert_dataframe_to_documents(listing_df)
    m_hdl.upload_listings(listing_docs)

    for doc in m_hdl.cl_listings.find():
        code = doc["有價證券代號"]
        birth_date = m_hdl.get_birth_date(code)
        record_date = m_hdl.get_record_date(code) or TRACEABLE_DATE
        start_date = max(birth_date, record_date, TRACEABLE_DATE)

        while start_date < TODAY:
            t1 = time.time()

            prices = crawl.fetch_monthly_prices(code=code, date_tgt=start_date)
            docs = convert_dataframe_to_timeseries(prices)
            m_hdl.upload_daily(docs=docs)

            start_date = get_next_month(start_date)
            t_inc = time.time() - t1
            if t_inc < MIN_TIME_INC:
                time.sleep(MIN_TIME_INC - t_inc)

    m_hdl.close()
    logger.info("Done!")


if __name__ == "__main__":
    main()
