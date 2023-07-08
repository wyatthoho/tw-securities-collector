import datetime
import logging
import logging.config
import time
from typing import Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup, ResultSet

from logging_config import LOGGING_CONFIG

USER_AGENT = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36'
DATE_TRACEABLE = datetime.date(2010, 1, 1)
DATE_TODAY = datetime.date.today()

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def security_filter(data: dict) -> bool:
    cond1 = not data['有價證券代號'][-1].isalpha()
    cond2 = not data['有價證券代號'][0].isalpha()
    cond3 = data['市場別'] not in ['上櫃', '期貨及選擇權', '興櫃一般板', '公開發行', '創櫃版']
    cond4 = data['有價證券別'] in ['ETF', '股票']
    return all([cond1, cond2, cond3, cond4])


def collect_securities(columns: list, rows: ResultSet):
    df = pd.DataFrame()
    for row in rows:
        data_dirty = row.text.split('\n')
        data_cleaned = {
            column: content
            for column, content in zip(columns, data_dirty)
            if column
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


def check_time_range(date_listed: datetime.date, date_tgt: datetime.date):
    date_earliest = max(DATE_TRACEABLE, date_listed)
    if date_tgt < date_earliest:
        raise Exception(
            f'The start date can not be earlier than {date_earliest}.'
        )
    elif date_tgt > DATE_TODAY:
        raise Exception(f'The target date can not be later than {DATE_TODAY}.')


def fetch_monthly_prices(security_code: str, date_tgt: datetime.date) -> pd.DataFrame:
    try:
        date_listed = search_listed_date(security_code)
        check_time_range(date_listed, date_tgt)
    except Exception as e:
        logger.error(e)
        quit()

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
    df_data = pd.DataFrame(content['data'], columns=content['fields'])
    return df_data


if __name__ == '__main__':
    securities = fetch_security_table()
    security_prices = fetch_monthly_prices(
        security_code='2330',
        date_tgt=datetime.date(2022, 11, 4)
    )
