import pandas

import crawler
import crud_mongo


def get_docs(df: pandas.DataFrame):
    return [row.to_dict() for idx, row in df.iterrows()]


def main():
    companies_etfs = crawler.get_companies_etfs()
    docs = get_docs(companies_etfs)
    crud_mongo.create_docs(
        db_name='taiwan_stocks',
        collection_name='companies_etfs',
        docs=docs
    )


if __name__ == '__main__':
    main()
