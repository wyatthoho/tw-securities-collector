import datetime
import time
from typing import Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

AGENT = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36'

def crawl_companies_etfs() -> pd.DataFrame:
    '''
    Collect the List of Taiwan Stock Exchange Listed Companies and ETFs.
    '''
    
    # Get the soup
    url = "https://isin.twse.com.tw/isin/single_main.jsp?"
    headers = {'user-agent': AGENT}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Initialize data frame
    table = soup.find('table', class_='h4')
    first_row = table.find('tr')
    cols_all = first_row.text.split('\n')
    cols_tgt = [text for text in cols_all if text]
    df = pd.DataFrame(columns=cols_tgt)
    
    # Collect row data
    rows = first_row.find_next_siblings('tr')
    for row in rows:
        data_all = row.text.split('\n')
        data_tgt = {name: cont for name, cont in zip(cols_all, data_all) if name}
        df = append_stock_data(df, data_tgt)
    return df


def append_stock_data(df_main: pd.DataFrame, data: dict) -> pd.DataFrame:
    cond1 = not data['有價證券代號'][-1].isalpha()
    cond2 = not data['有價證券代號'][0].isalpha()
    cond3 = data['市場別'] not in ['上櫃', '期貨及選擇權', '興櫃一般板', '公開發行', '創櫃版']
    cond4 = data['有價證券別'] in ['ETF', '股票']
    conds = [cond1, cond2, cond3, cond4]
    if all(conds):
        df_data = pd.DataFrame([data])
        return pd.concat([df_main, df_data], ignore_index=True)
    else:
        return df_main


def crawl_stock_price(stock_idx: int) -> pd.DataFrame:
    url = "https://isin.twse.com.tw/isin/single_main.jsp?"
    payload = {'owncode': str(stock_idx), 'stockname': ''}
    headers = {'user-agent': AGENT}
    response = requests.get(url, params=payload, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    date_listed = get_listed_date(soup)
    date_str, date_end = get_time_range(date_listed)
    return collect_price(stock_idx, date_str, date_end)


def get_listed_date(soup: BeautifulSoup) -> datetime.date:
    td_all = soup.find_all('td')
    for element in td_all:
        text = element.get_text()
        cond1 = len(text.split('/')) == 3
        cond2 = text.replace('/', '').isdigit()
        if cond1 and cond2:
            year, month, day = [int(digit) for digit in text.split('/')]
            return datetime.date(year, month, day)


def get_time_range(date_listed: datetime.date) -> Tuple[datetime.date, datetime.date]:
    date_traceable = datetime.date(2010, 1, 1)
    if date_listed < date_traceable:
        date_str = date_traceable
    else:
        date_str = date_listed
    return date_str, datetime.date.today()


def collect_price(stock_idx: int, date_str: datetime.date, date_end: datetime.date) -> pd.DataFrame:
    sleep_time = 5
    df = pd.DataFrame()
    date = date_str
    while True:
        if date <= date_end:
            content = crawl_stock(date, stock_idx)
            df_data = pd.DataFrame(content['data'], columns=content['fields'])
            df = pd.concat([df, df_data], ignore_index=True)
            date = get_next_month(date)
            time.sleep(sleep_time)
        else:
            break
    return pd.DataFrame(df)


def crawl_stock(date: datetime.date, stock_idx: int):
    date_input = str(date).replace('-', '')
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    payload = {'response': 'json', 'date': date_input, 'stockNo': str(stock_idx)}
    headers = {'user-agent': AGENT}
    response = requests.get(url, params=payload, headers=headers)
    date_show = date.strftime('%Y/%m')
    msg = f'Collecting stock id: {stock_idx:d}, region: {date_show:s}'
    print(msg)
    return eval(response.text)


def get_next_month(date: datetime.date):
    if date.month < 12:
        return datetime.date(date.year, date.month + 1, 1)
    else:
        return datetime.date(date.year + 1, 1, 1)


if __name__ == '__main__':
    companies_etfs = crawl_companies_etfs()
    stock_price = crawl_stock_price(stock_idx=2330)
