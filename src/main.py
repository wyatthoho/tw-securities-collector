import datetime
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup


def PutStockListTable(table, stockNo, stockName, listedOrOTC, stockType, industry, stockListedDate):
    condition1 = stockNo[-1].isalpha()
    condition2 = stockNo[0].isalpha()
    
    condition3 = listedOrOTC not in ['上櫃', '期貨及選擇權', '興櫃一般板', '公開發行', '創櫃版']
    condition4 = stockType in ['ETF', '股票']

    if not (condition1 or condition2) and condition3 and condition4:
        getThisStock = True
    else:
        getThisStock = False

    if getThisStock:
        if len(table) == 0:
            table['有價證券代號'] = [stockNo]
            table['有價證券名稱'] = [stockName]
            table['有價證券別'] = [stockType]
            table['產業別'] = [industry]
            table['發行日'] = [stockListedDate]
        else:
            table['有價證券代號'].append(stockNo)
            table['有價證券名稱'].append(stockName)
            table['有價證券別'].append(stockType)
            table['產業別'].append(industry)
            table['發行日'].append(stockListedDate)

    return table


def crawl_companies_etfs():
    '''
    Collect the List of Taiwan Stock Exchange Listed Companies and ETFs.
    '''
    url = "https://isin.twse.com.tw/isin/single_main.jsp?"
    headers = {'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    colIdx = 0
    table = {}
    for element in soup.find_all('td'):
        examCurrentRow = False
        if colIdx == 2:
            stockNo = element.get_text().strip()
        elif colIdx == 3:
            stockName = element.get_text()
        elif colIdx == 4:
            listedOrOTC = element.get_text()
        elif colIdx == 5:
            stockType = element.get_text()
        elif colIdx == 6:
            industry = element.get_text()
        elif colIdx == 7:
            stockListedDate = element.get_text()
        if colIdx == 9:
            colIdx = 0
            examCurrentRow = True
        else:
            colIdx += 1
        if examCurrentRow:
            table = PutStockListTable(table, stockNo, stockName, listedOrOTC, stockType, industry, stockListedDate)
    return pd.DataFrame(table)


def get_tracking_time_range(stockNo):
    url = "https://isin.twse.com.tw/isin/single_main.jsp?"
    payload = {'owncode': str(stockNo), 'stockname': ''}
    headers = {'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36'}
    response = requests.get(url, params=payload, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    for element in soup.find_all('td'):
        text = element.get_text()
        condition1 = len(text.split('/')) == 3
        condition2 = text.replace('/', '').isdigit()
        if condition1 and condition2:
            year, month, day = [int(digit) for digit in text.split('/')]
            listedDate = datetime.date(year, month, day)

    traceableDate = datetime.date(2010, 1, 1)
    if listedDate < traceableDate:
        yearStr, monthStr = traceableDate.year, traceableDate.month
    else:
        yearStr, monthStr = listedDate.year, listedDate.month
    today = datetime.date.today()
    return yearStr, monthStr, today.year, today.month


def CrawlPrice(date, stockNo):
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    payload = {'response': 'json', 'date': str(date), 'stockNo': str(stockNo)}
    headers = {'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Mobile Safari/537.36'}
    
    try:
        CheckDateFormat(date)
    except Exception as e:
        print(e.args[0])
        quit()

    response = requests.get(url, params=payload, headers=headers)
    msg = 'Loading: {}'.format(response.url)
    print(msg)
    return eval(response.text)


def CheckDateFormat(date):
    if type(date) != str:
        raise Exception('Date should be input as string')
    
    if len(date) != 8:
        raise Exception('Date should be input as: yyyymmdd')
    
    if not date.isdigit():
        raise Exception('Date should be characters in digits')


def CheckDateOrder(yearStr, monthStr, yearEnd, monthEnd):
    dateStr = datetime.date(yearStr, monthStr, 1)
    dateEnd = datetime.date(yearEnd, monthEnd, 1)

    if dateStr > dateEnd:
        raise Exception('The start date exceed the end date')


def PutHistoryTable(content, table):
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


def get_stock_price(stockNo, yearStr, monthStr, yearEnd, monthEnd):
    try:
        CheckDateOrder(yearStr, monthStr, yearEnd, monthEnd)
    except Exception as e:
        print(e.args[0])
        quit()

    table = {}
    year, month = yearStr, monthStr
    dateEnd = datetime.date(yearEnd, monthEnd, 1)
    
    while True:
        date = datetime.date(year, month, 1)
        dateStr = date.isoformat().replace('-', '')
        content = CrawlPrice(dateStr, stockNo)
        table = PutHistoryTable(content, table)

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


if __name__ == '__main__':
    companies_etfs = crawl_companies_etfs()
    stock_idx = 2330
    time_range = get_tracking_time_range(stock_idx)    
    stock_price = get_stock_price(stock_idx, *time_range)
    