import datetime
import logging
import logging.config
from typing import Dict, List

import pandas

import mongodb_handler
import security_crawler
from logging_config import LOGGING_CONFIG


DB_NAME = 'taiwan_securities'

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger()


def convert_dataframe_to_documents(df: pandas.DataFrame) -> List[Dict]:
    return [row.to_dict() for idx, row in df.iterrows()]


def fetch_newest_prices(security_name: str, security_code: str):
    try:
        latest_timestamp = mongodb_handler.get_latest_timestamp(
            db_name=DB_NAME,
            collection_name=security_name
        )
        security_prices = security_crawler.fetch_monthly_prices(
            security_code=security_code,
            date_tgt=latest_timestamp,
            date_end=datetime.date.today()
        )
    except IndexError:  # this is a brand new collection
        security_prices = security_crawler.fetch_monthly_prices(
            security_code=security_code,
            date_end=datetime.date.today()
        )
    return security_prices


def convert_rocdate_to_utcdate(rocdate: str) -> datetime.datetime:
    year, month, day = map(int, rocdate.split('/'))
    return datetime.datetime(year+1911, month, day)


def convert_dataframe_to_timeseries(df: pandas.DataFrame, metadata: Dict) -> List[Dict]:
    docs = []
    for idx, row in df.iterrows():
        doc = {
            'metadata': metadata,
            'timestamp': convert_rocdate_to_utcdate(row['日期']),
            'opening_price': float(row['開盤價']),
            'closing_price': float(row['收盤價']),
            'lowest_price': float(row['最低價']),
            'highest_price': float(row['最高價']),
            # 'price_difference': float(row['漲跌價差'].replace('X', '')),
            # 'X' 表示該日除息
            'number_trades': int(row['成交筆數'].replace(',', '')),
            'traded_volume': int(row['成交股數'].replace(',', '')),
            'traded_value': int(row['成交金額'].replace(',', '')),
        }
        docs.append(doc)
    return docs


def main():
    logger.info('Start!')
    securities = security_crawler.fetch_security_table()
    docs = convert_dataframe_to_documents(securities)
    mongodb_handler.connect_and_insert_general(
        db_name=DB_NAME,
        collection_name='security_info',
        docs=docs
    )

    for idx, security in securities.iterrows():
        security_name = security['有價證券名稱']
        security_code = security['有價證券代號']
        metadata = security.to_dict()
        security_prices = fetch_newest_prices(security_name, security_code)
        docs = convert_dataframe_to_timeseries(security_prices, metadata)
        mongodb_handler.connect_and_insert_timeseries(
            db_name=DB_NAME,
            collection_name=security_name,
            docs=docs
        )
    logger.info('Done!')


if __name__ == '__main__':
    main()
