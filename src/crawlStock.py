import time
import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup


def CrawlListedDate(stockNo):
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

    return listedDate


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


def PutIntoTable(content, table):
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


def GetTaiwanStockPrice(yearStr, monthStr, yearEnd, monthEnd, stockNo):
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
        table = PutIntoTable(content, table)

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

