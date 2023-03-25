import datetime
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

AGENT = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36'

def crawl_companies_etfs():
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


def append_stock_data(df_main, data: dict):
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


def crawl_stock_price(stock_idx):
    listed_date = crawl_listed_date(stock_idx)
    traceable_date = datetime.date(2010, 1, 1)
    if listed_date < traceable_date:
        yearStr, monthStr = traceable_date.year, traceable_date.month
    else:
        yearStr, monthStr = listed_date.year, listed_date.month
    
    today = datetime.date.today()
    yearEnd, monthEnd = today.year, today.month

    try:
        check_time_range(yearStr, monthStr, yearEnd, monthEnd)
    except Exception as e:
        print(e.args[0])
        quit()

    table = {}
    year, month = yearStr, monthStr
    dateEnd = datetime.date(yearEnd, monthEnd, 1)
    
    while True:
        date = datetime.date(year, month, 1)
        dateStr = date.isoformat().replace('-', '')
        content = crawl_stock(dateStr, stock_idx)
        table = append_history(content, table)
        suspendDuration = 5
        time.sleep(suspendDuration)
        if month < 12:
            month += 1
        else:
            month = 1
            year += 1
        if date >= dateEnd:
            break
    return pd.DataFrame(table)


def crawl_listed_date(stockNo):
    url = "https://isin.twse.com.tw/isin/single_main.jsp?"
    payload = {'owncode': str(stockNo), 'stockname': ''}
    headers = {'user-agent': AGENT}
    response = requests.get(url, params=payload, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    for element in soup.find_all('td'):
        text = element.get_text()
        condition1 = len(text.split('/')) == 3
        condition2 = text.replace('/', '').isdigit()
        if condition1 and condition2:
            year, month, day = [int(digit) for digit in text.split('/')]
            listedDate = datetime.date(year, month, day)
    return listedDate


def check_time_range(yearStr, monthStr, yearEnd, monthEnd):
    dateStr = datetime.date(yearStr, monthStr, 1)
    dateEnd = datetime.date(yearEnd, monthEnd, 1)
    if dateStr > dateEnd:
        raise Exception('The start date exceed the end date')


def crawl_stock(date, stockNo):
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    payload = {'response': 'json', 'date': str(date), 'stockNo': str(stockNo)}
    headers = {'user-agent': AGENT}
    
    try:
        check_date_fmt(date)
    except Exception as e:
        print(e.args[0])
        quit()

    response = requests.get(url, params=payload, headers=headers)
    msg = 'Loading: {}'.format(response.url)
    print(msg)
    return eval(response.text)


def check_date_fmt(date):
    if type(date) != str:
        raise Exception('Date should be input as string')
    if len(date) != 8:
        raise Exception('Date should be input as: yyyymmdd')
    if not date.isdigit():
        raise Exception('Date should be characters in digits')


def append_history(content, table):
    fields = content['fields']
    data = content['data']
    columnNum = len(fields)
    rowNum = len(data)
    for col in range(columnNum):
        title = fields[col]
        for row in range(rowNum):
            values = data[row][col]
            if title in table.keys():
                table[title].append(values)
            else:
                table[title] = [values]
    return table


if __name__ == '__main__':
    companies_etfs = crawl_companies_etfs()
    # stock_price = crawl_stock_price(stock_idx=2330)

