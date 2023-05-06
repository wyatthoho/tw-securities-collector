import datetime
from typing import Dict, List

import pandas

import crawler
import crud_mongo


def dataframe_to_docs(df: pandas.DataFrame) -> List[Dict]:
    return [row.to_dict() for idx, row in df.iterrows()]


def dataframe_to_timeseries(df: pandas.DataFrame) -> List[Dict]:
    docs = []
    for idx, row in df.iterrows():
        doc = {
            'timestamp': rocdate_to_utc(row['日期']),
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


def rocdate_to_utc(rocdate: str) -> datetime.datetime:
    year, month, day = map(int, rocdate.split('/'))
    return datetime.datetime(year+1911, month, day)


def main():
    DB_NAME = 'taiwan_securities'
    securities = crawler.get_security_table()
    docs = dataframe_to_docs(securities)
    crud_mongo.connect_and_insert_general(
        db_name=DB_NAME,
        collection_name='security_info',
        docs=docs
    )

    for idx, row in securities.iterrows():
        security_name = row['有價證券名稱']
        security_code = row['有價證券代號']
        security_prices = crawler.get_security_prices(security_code)
        docs = dataframe_to_timeseries(security_prices)
        crud_mongo.connect_and_insert_timeseries(
            db_name=DB_NAME,
            collection_name=security_name,
            docs=docs
        )


if __name__ == '__main__':
    main()
