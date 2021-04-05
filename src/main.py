import time
import datetime
import pandas as pd
import requests
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup


def CrawlTaiwanStockListedDate(stockNo):
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


def CrawlTaiwanStockPrice(date, stockNo):
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


def PutValuesIntoTable(content, table):
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
        content = CrawlTaiwanStockPrice(dateStr, stockNo)
        table = PutValuesIntoTable(content, table)

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


def PlotData(figIdx, stockNo, xData, yData):
    yData = [float(data) for data in yData]

    fig = plt.figure(num=figIdx, figsize=(6,3), tight_layout=True)
    ax = fig.add_subplot(1, 1, 1)
    ax.plot(xData, yData)

    xTickNum = 8
    xDelta = (len(xData)-1) // (xTickNum-1)
    xTick = [xData[itr*xDelta] for itr in range(xTickNum)]

    ax.set_xticks(xTick)
    ax.set_xticklabels(xTick, rotation='vertical')
    ax.set_xlim((xTick[0], xTick[-1]))
    ax.grid(color='0.2', linestyle='-', linewidth=0.1)
    ax.set_title('{}'.format(stockNo), family='Arial', fontsize=14)
    plt.show()


if __name__ == '__main__':
    stockNo = 2330
    listedDate = CrawlTaiwanStockListedDate(stockNo)
    traceableDate = datetime.date(2010, 1, 1)
    today = datetime.date.today()

    if listedDate < traceableDate:
        yearStr, monthStr = traceableDate.year, traceableDate.month
    else:
        yearStr, monthStr = listedDate.year, listedDate.month

    # yearEnd, monthEnd = 2010, 5
    yearEnd, monthEnd = today.year, today.month

    fileName = '{:4d}_{:4d}{:02d}_{:4d}{:02d}.csv'.format(stockNo, yearStr, monthStr, yearEnd, monthEnd)
    myTable = GetTaiwanStockPrice(yearStr, monthStr, yearEnd, monthEnd, stockNo)
    myTable.to_csv(fileName)

    PlotData(figIdx=1, stockNo=stockNo, xData=myTable['日期'], yData=myTable['收盤價'])