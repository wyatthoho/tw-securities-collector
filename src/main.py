import crawler
import crud_mongo


def main():
    companies_etfs = crawler.get_companies_etfs()
    items = [row.to_dict() for idx, row in companies_etfs.iterrows()]
    crud_mongo.create_docs(
        db_name='taiwan_stocks',
        collection_name='companies_etfs',
        items=items
    )


if __name__ == '__main__':
    main()
