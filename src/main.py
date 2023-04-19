import pandas
from typing import Dict, List

import crawler
import crud_mongo


def transform_df(df: pandas.DataFrame) -> List[Dict]:
    return [row.to_dict() for idx, row in df.iterrows()]


def main():
    companies_etfs = crawler.get_companies_etfs()
    docs = transform_df(companies_etfs)
    crud_mongo.update_docs(
        db_name='taiwan_stocks',
        collection_name='companies_etfs',
        docs=docs
    )

    for idx, row in companies_etfs.iterrows():
        stock_name = row['有價證券名稱']
        stock_idx = row['有價證券代號']
        stock_history = crawler.get_stock_prices(stock_idx)
        docs = transform_df(stock_history)
        crud_mongo.update_docs(
            db_name='taiwan_stocks',
            collection_name=stock_name,
            docs=docs
        )


if __name__ == '__main__':
    main()
