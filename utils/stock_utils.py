from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError
from pandas.tseries.offsets import BDay
from finviz.screener import Screener
from utils import math_calcs
from dask import delayed
from config.configuration import TD_API
import pandas
import datetime
import numpy as np
import requests

EPOCH = datetime.datetime.utcfromtimestamp(0)

HOUR_ADJUSTMENT = 5

### FOR TD API ###

def unixTimeMs(dateAndTime):
    """
    Convert datetime to milliseconds subtract epoch time. Feed back the integers for API
    :param dateAndTime: Datetime of specific date
    :return: Integer representation of milliseconds
    """
    dateAndTime = dateAndTime + datetime.timedelta(hours=HOUR_ADJUSTMENT)
    return int((dateAndTime - EPOCH).total_seconds() * 1000.0)

def convertToEST(timestamp):
    """
    Convert timestamps in json data's candles to EST
    :return: Time in EST format
    """
    newDateTime = datetime.datetime.fromtimestamp(timestamp/1000)
    return newDateTime.date(), newDateTime.time()

def convertData(data):
    """
    Adjust raw json data to usable stock data
    :param data: Json data of candles
    :return: Usable stock data
    """
    for candle in data['candles']:
        candle['date'],candle['time'] = convertToEST(candle['datetime'])

    return data

###

def getDailyDataTD(symbol, startDate, endDate):
    priceHistory = "https://api.tdameritrade.com/v1/marketdata/{symbol}/pricehistory".format(symbol=symbol)
    dailyChartPayLoad = {
        'apikey': TD_API['API_KEY'],
        'periodType': 'day',
        'frequencyType': 'minute',
        'frequency': '1',
        'period': '1',
        'endDate': str(unixTimeMs(endDate)),
        'startDate': str(unixTimeMs(startDate)),
        'needExtendedHoursData': 'true'
    }
    content = requests.get(url=priceHistory, params=dailyChartPayLoad)
    data = content.json()
    data = convertData(data)
    data = pandas.DataFrame(data['candles'])
    data.columns = data.columns.str.capitalize()
    return data

def splitCandles(data):
    """
    Split the daily set of candle for the given stock into premarket, regular market and after hours
    :param data: Json data of candles
    :return: Premarket candles, Regular Market, After hours
    """

    premarket = data[(data['Time'] < datetime.time(9,30))]
    regularMarket = data[(data['Time'] < datetime.time(9, 30)) & (data['Time'] < datetime.time(16,0))]
    afterhour = data[(data['Time'] > datetime.time(16,0))]

    return premarket, regularMarket, afterhour

def getYearlyDataTD(symbol, startDate, endDate):
    priceHistory = "https://api.tdameritrade.com/v1/marketdata/{symbol}/pricehistory".format(symbol=symbol)
    dailyChartPayLoad = {
        'apikey': TD_API['API_KEY'],
        'periodType': 'year',
        'frequencyType': 'daily',
        'frequency': '1',
        'period': '1',
        'endDate': str(unixTimeMs(endDate)),
        'startDate': str(unixTimeMs(startDate)),
        'needExtendedHoursData': 'False'
    }
    content = requests.get(url=priceHistory, params=dailyChartPayLoad)
    data = content.json()
    data = convertData(data)
    data = pandas.DataFrame(data['candles'])
    data.columns = data.columns.str.capitalize()
    return data

def getTDData(ticker,start,end):
    try:
        stockData = getYearlyDataTD(ticker,
                                    datetime.datetime.strptime(start, '%Y-%m-%d'),
                                    datetime.datetime.strptime(end, '%Y-%m-%d'))

        stockData.insert(loc=0, column='Counter', value=np.arange(len(stockData)))
        stockData = stockData.reset_index()
        stockData = stockData.astype({'Date': 'datetime64'})
        stockData = stockData.set_index('Date')
        stockData = stockData.sort_values(by='Date')
        return stockData
    except RemoteDataError:
        print('No Data found for {0}'.format(ticker))

def getData(ticker, start, end):
    try:
        stockData = data.DataReader(ticker,
                                    'stooq',
                                    start,
                                    end)

        stockData.insert(loc=0, column='Counter', value=np.arange(len(stockData)))
        stockData = stockData.reset_index()
        stockData = stockData.astype({'Date': 'datetime64'})
        stockData = stockData.set_index('Date')
        stockData = stockData.sort_values(by='Date')
        return stockData
    except RemoteDataError:
        print('No Data found for {0}'.format(ticker))

# ====== Fetch Statistics Data ======== #

def getPremarketData(data):
    """
    Get Premarket Highest price, Lowest price after Highest price is met, the respective times and cumulative volume
    :param data: Stock Candles Data
    :return: Premarket High, Premarket High Time, Premarket Low after High, Low time and cumulative volume
    """
    premarketData = {
        'Premarket High': 0.0,
        'Premarket High Time': datetime.datetime.now(),
        'Premarket Low (After Top Reached)' : 0.0,
        'Premarket Low Time (After Top Reached)' : datetime.datetime.now(),
        'Premarket Volume':0
    }

    for candle in data:
        if candle['high'] > premarketData['Premarket High']:
            premarketData['Premarket High'] = candle['high']
            premarketData['Premarket High Time'] = candle['time']
            premarketData['Premarket Low (After Top Reached)'] = candle['high']
        elif candle['low'] < premarketData['Premarket Low (After Top Reached)']:
            premarketData['Premarket Low (After Top Reached)'] = candle['low']
            premarketData['Premarket Low Time (After Top Reached)'] = candle['time']

        premarketData['Premarket Volume'] += candle['volume']

    return premarketData

def getPivotPoints(df):
    """
    Get pivot points in stock chart
    Swing lows are reversals when the bars go from red to green
    Swing highs are reversals when bars go from green to red
    :param df: Stock Chart
    :return:
    """
    resistancePivots = []
    supportPivots = []
    prevBarIsGreen = df['Close'].iloc[0] > df['Open'].iloc[0]
    prevClose = df['Close'].iloc[0]
    print(prevBarIsGreen)

    for index, row in df.iloc[1:].iterrows():
        if row['Close'] > row['Open'] and prevBarIsGreen == False:
            num = prevClose if prevClose < row['Open'] else row['Open']
            supportPivots.append((index.to_pydatetime(),num, row['Counter'])) # Note: You could have prev red candle as start of pivot (OR?)
            prevBarIsGreen = True
        elif row['Close'] < row['Open'] and prevBarIsGreen:
            num = prevClose if prevClose > row['Open'] else row['Open']
            resistancePivots.append((index.to_pydatetime(),num, row['Counter']))
            prevBarIsGreen = False
        prevClose = row['Close']

    return supportPivots, resistancePivots

class Trendline():
    def __init__(self, startPivot, endPivot, pivots):
        self.startPivot = startPivot
        self.endPivot = endPivot
        self.pivots = pivots
        self.slope, self.yIntercept = math_calcs.getLineGraph(startPivot[2], startPivot[1], endPivot[2], endPivot[1])
        self.marginOfError = 0.1
        self.score = 0

    def calcTouchPoints(self):
        touchPoint = 0
        for p in self.pivots:
            if abs(self.yIntercept + self.slope * p[2] - p[1]) < ((self.yIntercept + self.slope * p[2]) * self.marginOfError):
                touchPoint += 1

        self.score = touchPoint

        return touchPoint

def generateTrendLine(pivots, startTime=0, endTime=0, reverse=False):
    """
    Sort the pivots by ascending or descending order
    Graph a line between the 2 points and check how many pivot points touch it
    Count how many range exceeds or is close enough
    1. Ignore dates outside of pivot range
    :param pivots:
    :param startTime:
    :param endTime:
    :return:
    """
    sortedPivots = sorted(pivots,key=lambda x:x[1],reverse=reverse)
    p1 = sortedPivots[0]
    prevDate = p1[0]
    highScore = 0
    pair = {}

    for piv in sortedPivots[1:]:
        if not (prevDate > piv[0]):
            trendPoint = Trendline(p1, piv, sortedPivots).calcTouchPoints()
            if highScore < trendPoint:
                highScore = trendPoint
                pair = {
                        'Date1': p1[0], # .strftime("%Y-%m-%d"),
                        'Pivot1': p1[1],
                        'Date2': piv[0], # .strftime("%Y-%m-%d"),
                        'Pivot2': piv[1]
                    }
                prevDate = pair['Date2']

    return pair

def getAllStocks():
    """
    Get complete list of stocks from excel. Excel sheet saved a list of all stocks to read from Nasdaq, NYSE
    :return: Dataframe of Stocks
    """
    return pandas.read_excel('D:\The Fastlane Project\Coding Projects\Stock Analysis\stocks\stock_data_2.xlsx')

def getLastBDay(date):
    return date if np.is_busday(date) else getLastBDay(date - datetime.timedelta(1))

def parallelUpdate(df, date):
    data = []
    rangelimit = 99

    for i in range(0, len(df), rangelimit):
        maxLen = i + rangelimit
        if maxLen > len(df):
            maxLen = len(df)
        print('{0} to {1}'.format(i, maxLen))
        partition = delayed(updateStockInfo)(df[i:maxLen], date)
        data.append(partition)
    return delayed(sum)(data,[])

def updateStockInfo(df, date):
    """
    Update all stock's market cap and float once day ends in excel sheet
    Save excel sheet
    :return: Updated Dataframe
    """
    date = getLastBDay(date)

    for index, row in df.iterrows():
        try:
            stockData = data.DataReader(row['Symbol'],
                                        'yahoo',
                                        date,
                                        date)
            floatNum = df.loc[df['Symbol']==row['Symbol'], ['Float']].values[0]

            df.at[index,'Last Sale'] = stockData['Close']
            df.at[index,'Market Cap'] = stockData['Close'] * float(floatNum) if floatNum else 0
            df.at[index, 'Has Data'] = 1
        except (RemoteDataError, KeyError):
            df.at[index, 'Has Data'] = 0
            print('No Data found for {0}'.format(index))
    return [df.to_dict()]

def getStockList(storeExcel=False, path=None):
    """
    Get Stock information from Finviz
    :param storeExcel: Store data into excel or not
    :param path: location to store excel file
    :return:
    """
    stockList = Screener()
    stockList = pandas.DataFrame(stockList)
    if storeExcel:
        stockList.to_excel(path)

    return stockList


def readStockData(symbol):
    df = pandas.read_excel('D:/The Fastlane Project/Coding Projects/Stock Analysis/results/stock_data/{0}_Data.xlsx'.format(symbol), index_col=0)
    return df

if __name__ == '__main__':
    print("Getting all stocks")

    dateTimeStrStart = '2021-8-20 9:30'
    dateTimeStrEnd = '2021-8-20 16:00'
    dateTimeStart = datetime.datetime.strptime(dateTimeStrStart, '%Y-%m-%d %H:%M')
    dateTimeEnd = datetime.datetime.strptime(dateTimeStrEnd, '%Y-%m-%d %H:%M')

    data = getDailyDataTD('NIO', dateTimeStart, dateTimeEnd)
    print(data)
    pre, reg, aft=splitCandles(data)
    print(pre)

    ### PARALLEL PROCESSING

    # df = pandas.read_excel('D:\The Fastlane Project\Coding Projects\Stock Analysis\stocks\stock_data.xlsx')
    #
    # # print(df)
    # df2 = parallelUpdate(df, datetime.date(2021,6,20))
    # df2 = df2.compute()
    #
    # data = []
    # for x in df2:
    #     data.append(pandas.DataFrame(x))
    #
    # # print(df.loc[0])
    # # print(df2.loc[0])
    # finalDf = pandas.concat(data)
    # print("out")
    # finalDf.to_excel('D:\The Fastlane Project\Coding Projects\Stock Analysis\stocks\stock_data_2.xlsx', engine='xlsxwriter', index=False)