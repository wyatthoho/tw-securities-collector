import datetime
import logging
import logging.config
from typing import Dict, List

import pandas

import mongodb_handler
import security_crawler
from logging_config import LOGGING_CONFIG


DB_NAME = 'taiwan_securities'
DATE_TRACEABLE = datetime.date(2010, 1, 1)
DATE_TODAY = datetime.date.today()

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger()


def convert_dataframe_to_documents(df: pandas.DataFrame) -> List[Dict]:
    return [row.to_dict() for idx, row in df.iterrows()]


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


def get_start_date(security_name: str, security_code: str) -> datetime.date:
    try:
        latest_timestamp = mongodb_handler.get_latest_timestamp(
            db_name=DB_NAME,
            collection_name=security_name
        )
        return latest_timestamp + datetime.timedelta(days=1)
    except IndexError:  # this is a brand new collection
        date_listed = security_crawler.search_listed_date(security_code)
        return max(DATE_TRACEABLE, date_listed)


def get_next_month(date: datetime.date) -> datetime.date:
    if date.month < 12:
        return datetime.date(date.year, date.month+1, 1)
    else:
        return datetime.date(date.year+1, 1, 1)


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
        date_tgt = get_start_date(security_name, security_code)
        while date_tgt <= DATE_TODAY:
            security_prices = security_crawler.fetch_monthly_prices(
                security_code=security_code,
                date_tgt=date_tgt
            )
            docs = convert_dataframe_to_timeseries(security_prices, metadata)
            mongodb_handler.connect_and_insert_timeseries(
                db_name=DB_NAME,
                collection_name=security_name,
                docs=docs
            )
            date_tgt = get_next_month(date_tgt)
    logger.info('Done!')


if __name__ == '__main__':
    main()
