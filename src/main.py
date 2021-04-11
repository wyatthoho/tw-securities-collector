import os
import datetime
import pandas as pd
import smartPlot
import crawlStock
import calendarTransform


if __name__ == '__main__':
    print('Start..')
    
    # Find the listed companies and ETF in Taiwan
    cwd = os.getcwd()
    csvPath = os.path.join(cwd, 'data\\TWStockList.csv')

    myTable = crawlStock.CrawlStockNoList()
    myTable.to_csv(csvPath)
    
    getData = False
    plotData = False

    stockNo = 2330
    listedDate = crawlStock.CrawlListedDate(stockNo)
    traceableDate = datetime.date(2010, 1, 1)
    today = datetime.date.today()

    if listedDate < traceableDate:
        yearStr, monthStr = traceableDate.year, traceableDate.month
    else:
        yearStr, monthStr = listedDate.year, listedDate.month

    yearEnd, monthEnd = today.year, today.month

    fileName = '{:4d}_{:4d}{:02d}_{:4d}{:02d}'.format(stockNo, yearStr, monthStr, yearEnd, monthEnd)
    csvPath = os.path.join(cwd, 'data\\{}.csv'.format(fileName))
    pngPath = os.path.join(cwd, 'data\\{}.png'.format(fileName))

    # Crawl and Save csv
    if getData:
        myTable = crawlStock.GetTaiwanStockPrice(yearStr, monthStr, yearEnd, monthEnd, stockNo)
        myTable.to_csv(csvPath)

    # Read csv and Plot png
    if plotData:
        myTable = pd.read_csv(csvPath)
        xData = [calendarTransform.TransRocToGregorian(rocDate, '/') for rocDate in myTable['日期']]
        yData = [float(data) for data in myTable['收盤價']]

        priceFig = smartPlot.BilinearFig(figIdx=1)
        priceFig.plotData(title=stockNo, xData=xData, yData=yData)
        priceFig.saveFig(pngPath)
    
    print('Done ^.< ')