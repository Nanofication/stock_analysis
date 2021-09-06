from utils import stock_utils
import pandas as pd
import datetime

def findPotentialDipAndRipTrades(stockData):
    """
    Scan through list of stocks between 0 - 300 M Market Cap
    Find high volume trade days. Fetch and save.
    We apply Dip and Rip in separate function
    :return:
    """
    dateTimeStrStart = '2019-9-20 9:30'
    dateTimeStrEnd = '2021-9-1 16:00'
    dateTimeStart = datetime.datetime.strptime(dateTimeStrStart, '%Y-%m-%d %H:%M')
    dateTimeEnd = datetime.datetime.strptime(dateTimeStrEnd, '%Y-%m-%d %H:%M')
    stockDatesMap = {}

    df = stockData if not stockData.empty else stock_utils.getStockList()
    df = df[df['Market Cap'] < 300000000]

    tickers = df['Ticker'].to_list()

    for t in tickers:
        try:
            print("Fetching data for {0}".format(t))
            tickerData = stock_utils.getYearlyDataTD(t,dateTimeStart, dateTimeEnd)
            tickerData = tickerData[tickerData['Volume'] > 1000000]
            tickerData = tickerData.sort_values('Volume', ascending=False)

            stockDatesMap[t] = tickerData['Date'].to_list()
        except Exception as e:
            print("Something went wrong for {0}".format(t))
            print(e)

    return stockDatesMap

if __name__ == '__main__':
    data = pd.read_excel('D:\The Fastlane Project\Coding Projects\Stock Analysis\stocks\stock_data_3.xlsx')
    findPotentialDipAndRipTrades(data)