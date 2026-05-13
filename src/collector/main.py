# import datetime
import logging
import os
import sys
# import time
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv
# from pymongo.collection import Collection

from collector import mongodb_handler as mongo
from collector import security_crawler as crawl


load_dotenv()
MONGODB_URL = os.environ.get("MONGODB_URL")
# DB_NAME = "taiwan_securities"
# DATE_TRACEABLE = datetime.date(2010, 1, 1)
# DATE_TODAY = datetime.date.today()
# MIN_TIME_INC = 5


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def convert_dataframe_to_documents(df: pd.DataFrame) -> List[Dict]:
    return [row.to_dict() for _, row in df.iterrows()]


# def convert_rocdate_to_utcdate(rocdate: str) -> datetime.datetime:
#     year, month, day = map(int, rocdate.split("/"))
#     return datetime.datetime(year + 1911, month, day)


# def convert_dataframe_to_timeseries(
#     df: pd.DataFrame, collection: Collection
# ) -> List[Dict]:
#     docs = []
#     for idx, row in df.iterrows():
#         try:
#             doc = {
#                 "timestamp": convert_rocdate_to_utcdate(row["日期"]),
#                 "opening_price": float(row["開盤價"]),
#                 "closing_price": float(row["收盤價"]),
#                 "lowest_price": float(row["最低價"]),
#                 "highest_price": float(row["最高價"]),
#                 "number_trades": int(row["成交筆數"].replace(",", "")),
#                 "traded_volume": int(row["成交股數"].replace(",", "")),
#                 "traded_value": int(row["成交金額"].replace(",", "")),
#             }
#         except ValueError:
#             doc_num = mongo.count_documents(collection)
#             if doc_num == 0:
#                 continue
#             if idx == 0:
#                 pre_date = mongo.get_latest_timestamp(collection)
#                 pre_doc = mongo.get_daily_document(collection, pre_date)
#             else:
#                 pre_doc = docs[idx - 1]
#             doc = pre_doc
#             doc["timestamp"] = convert_rocdate_to_utcdate(row["日期"])
#             doc["number_trades"] = 0
#             doc["traded_volume"] = 0
#             doc["traded_value"] = 0
#         docs.append(doc)
#     return docs


# def get_start_date(collection: Collection, security_code: str) -> datetime.date:
#     try:
#         latest_timestamp = mongo.get_latest_timestamp(collection=collection)
#         return latest_timestamp.date() + datetime.timedelta(days=1)
#     except IndexError:  # this is a brand new collection
#         date_listed = crawl.search_listed_date(security_code)
#         return max(DATE_TRACEABLE, date_listed)


# def get_next_month(date: datetime.date) -> datetime.date:
#     if date.month < 12:
#         return datetime.date(date.year, date.month + 1, 1)
#     else:
#         return datetime.date(date.year + 1, 1, 1)


# def iter_monthly(collection: Collection, security_code: str, date_tgt: datetime.date):
#     while date_tgt <= DATE_TODAY:
#         t1 = time.time()
#         try:
#             security_prices = crawl.fetch_monthly_prices(
#                 security_code=security_code, date_tgt=date_tgt
#             )
#         except Exception as e:
#             logger.warning(e, exc_info=True)
#             break
#         docs = convert_dataframe_to_timeseries(security_prices, collection)
#         mongo.update_collection(collection=collection, docs=docs, with_metadata=False)
#         date_tgt = get_next_month(date_tgt)

#         t_inc = time.time() - t1
#         if t_inc < MIN_TIME_INC:
#             time.sleep(MIN_TIME_INC - t_inc)


def main():
    logger.info("Start!")

    listing_df = crawl.fetch_listings()
    listing_docs = convert_dataframe_to_documents(listing_df)

    mongo_handler = mongo.MongoHandler(url=MONGODB_URL)
    mongo_handler.upload_listing(listing_docs)

    # for _, security in securities.iterrows():
    #     security_name = security['有價證券名稱']
    #     security_code = security['有價證券代號']

    #     collection_name = f'{security_name} ({security_code})'
    #     collection = mongo.get_timeseries_collection(db, collection_name)

    #     date_tgt = get_start_date(collection, security_code)
    #     iter_monthly(collection, security_code, date_tgt)

    mongo_handler.close()
    logger.info("Done!")


if __name__ == "__main__":
    main()
