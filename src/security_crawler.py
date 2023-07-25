import datetime
import json
import logging
import logging.config
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup, ResultSet


this_dir = os.path.dirname(__file__)
config_path = os.path.join(this_dir, 'config.json')
with open(config_path, 'r', encoding='utf-8') as f:
    config = json.load(f)


logging.config.dictConfig(config['logging'])
logger = logging.getLogger(__name__)


USER_AGENT = config['crawler']['user_agent']


def security_filter(data: dict) -> bool:
    cond1 = not data['有價證券代號'][-1].isalpha()
    cond2 = not data['有價證券代號'][0].isalpha()
    cond3 = data['市場別'] in ['上市', ]
    cond4 = data['有價證券別'] in ['ETF', '股票']
    return all([cond1, cond2, cond3, cond4])


def collect_securities(columns: list, rows: ResultSet):
    df = pd.DataFrame()
    for row in rows:
        data_dirty = row.text.split('\n')
        data_cleaned = {
            column: content
            for column, content in zip(columns, data_dirty)
            if column not in config['crawler']['columns_skip']
        }
        if security_filter(data_cleaned):
            df_data = pd.DataFrame([data_cleaned])
            df = pd.concat([df, df_data], ignore_index=True)
    return df


def fetch_security_table() -> pd.DataFrame:
    '''
    Collect the table of securities from Taiwan Stock Exchange.
    '''
    # Get the soup
    logger.info('Fetching securities data from Taiwan Stock Exchange website..')
    url = "https://isin.twse.com.tw/isin/single_main.jsp?"
    headers = {'user-agent': USER_AGENT}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Analyze the soup
    logger.info('Cleaning and filtering data..')
    table = soup.find('table', class_='h4')
    first_row = table.find('tr')
    columns = first_row.text.split('\n')
    other_rows = first_row.find_next_siblings('tr')
    return collect_securities(columns, other_rows)


def search_listed_date(security_code: str) -> datetime.date:
    url = "https://isin.twse.com.tw/isin/single_main.jsp?"
    payload = {'owncode': security_code, 'stockname': ''}
    headers = {'user-agent': USER_AGENT}
    response = requests.get(url, params=payload, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    td_all = soup.find_all('td')
    for element in td_all:
        text = element.get_text()
        cond1 = len(text.split('/')) == 3
        cond2 = text.replace('/', '').isdigit()
        if cond1 and cond2:
            year, month, day = [int(digit) for digit in text.split('/')]
            return datetime.date(year, month, day)


def fetch_monthly_prices(security_code: str, date_tgt: datetime.date) -> pd.DataFrame:
    date_show = date_tgt.strftime('%Y-%m')
    msg = f'Collecting the prices of {security_code} in {date_show}..'
    logger.info(msg)

    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    payload = {
        'response': 'json',
        'date': str(date_tgt).replace('-', ''),
        'stockNo': security_code
    }
    headers = {'user-agent': USER_AGENT}
    response = requests.get(url, params=payload, headers=headers)
    content = eval(response.text)
    if content['stat'] != 'OK':
        raise Exception(f'Fetch failed for {date_tgt}')
    else:
        return pd.DataFrame(content['data'], columns=content['fields'])


if __name__ == '__main__':
    securities = fetch_security_table()
    date_listed = search_listed_date('00639')
    security_prices = fetch_monthly_prices(
        security_code='00639',
        date_tgt=datetime.date(2015, 12, 1)
    )
