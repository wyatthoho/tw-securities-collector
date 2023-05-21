import datetime
import time
from typing import Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup, ResultSet

AGENT = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36'
DATE_TRACEABLE = datetime.date(2010, 1, 1)
DATE_TODAY = datetime.date.today()


def get_security_table() -> pd.DataFrame:
    '''
    Collect the table of securities from Taiwan Stock Exchange.
    '''
    # Get the soup
    url = "https://isin.twse.com.tw/isin/single_main.jsp?"
    headers = {'user-agent': AGENT}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Analyze the soup
    table = soup.find('table', class_='h4')
    first_row = table.find('tr')
    columns = first_row.text.split('\n')
    other_rows = first_row.find_next_siblings('tr')
    return collect_securities(columns, other_rows)


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


def security_filter(data: dict) -> bool:
    cond1 = not data['有價證券代號'][-1].isalpha()
    cond2 = not data['有價證券代號'][0].isalpha()
    cond3 = data['市場別'] not in ['上櫃', '期貨及選擇權', '興櫃一般板', '公開發行', '創櫃版']
    cond4 = data['有價證券別'] in ['ETF', '股票']
    return all([cond1, cond2, cond3, cond4])


def get_security_prices(
        security_code: str,
        date_str: datetime.date = DATE_TRACEABLE,
        date_end: datetime.date = DATE_TODAY) -> pd.DataFrame:
    '''
    Collect the prices for the specific stock index for all time.
    '''
    url = "https://isin.twse.com.tw/isin/single_main.jsp?"
    payload = {'owncode': security_code, 'stockname': ''}
    headers = {'user-agent': AGENT}
    response = requests.get(url, params=payload, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    date_listed = search_listed_date(soup)
    try:
        check_time_range(date_listed, date_str, date_end)
    except Exception as e:
        print(e)
        quit()
    date_str, date_end = correct_time_range(date_listed, date_str, date_end)
    return iter_time_range(security_code, date_str, date_end)


def search_listed_date(soup: BeautifulSoup) -> datetime.date:
    td_all = soup.find_all('td')
    for element in td_all:
        text = element.get_text()
        cond1 = len(text.split('/')) == 3
        cond2 = text.replace('/', '').isdigit()
        if cond1 and cond2:
            year, month, day = [int(digit) for digit in text.split('/')]
            return datetime.date(year, month, day)


def check_time_range(date_listed: datetime.date, date_str: datetime.date, date_end: datetime.date):
    date_earliest = max(DATE_TRACEABLE, date_listed)
    if date_str > date_end:
        raise Exception('The start date is later than the end date.')
    elif date_str < date_earliest:
        raise Exception(
            f'The start date can not be earlier than {date_earliest}.'
        )
    elif date_end > DATE_TODAY:
        raise Exception(f'The end date can not be later than {DATE_TODAY}.')


def correct_time_range(date_listed: datetime.date, date_str: datetime.date, date_end: datetime.date) -> Tuple[datetime.date, datetime.date]:
    date_str = max(DATE_TRACEABLE, date_listed, date_str)
    date_end = min(DATE_TODAY, date_end)
    return date_str, date_end


def iter_time_range(security_code: str, date_str: datetime.date, date_end: datetime.date) -> pd.DataFrame:
    sleep_time = 5
    df_main = pd.DataFrame()
    date = date_str
    while date <= date_end:
        content = crawl_month_prices(date, security_code)
        df_data = pd.DataFrame(content['data'], columns=content['fields'])
        df_main = pd.concat([df_main, df_data], ignore_index=True)
        date = get_next_month(date)
        time.sleep(sleep_time)
    return df_main


def crawl_month_prices(date: datetime.date, security_code: str) -> dict:
    date_input = str(date).replace('-', '')
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    payload = {
        'response': 'json',
        'date': date_input,
        'stockNo': security_code
    }
    headers = {'user-agent': AGENT}
    response = requests.get(url, params=payload, headers=headers)
    date_show = date.strftime('%Y/%m')
    msg = f'Collecting the prices of {security_code} in {date_show}..'
    print(msg)
    return eval(response.text)


def get_next_month(date: datetime.date) -> datetime.date:
    if date.month < 12:
        return datetime.date(date.year, date.month+1, 1)
    else:
        return datetime.date(date.year+1, 1, 1)


if __name__ == '__main__':
    securities = get_security_table()
    security_prices = get_security_prices(
        security_code='2330',
        date_str=datetime.date(2022, 11, 1),
        date_end=datetime.date(2023, 4, 1)
    )
