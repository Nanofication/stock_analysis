from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError
from pandas.tseries.offsets import BDay
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

def getStockDataTD(symbol, startDate, endDate):
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
    return pandas.DataFrame(data['candles'])

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

if __name__ == '__main__':
    # print("Getting all stocks")
    dateTimeStrStart = '2021-1-2 16:00'
    dateTimeStrEnd = '2021-7-8 16:00'
    dateTimeStart = datetime.datetime.strptime(dateTimeStrStart, '%Y-%m-%d %H:%M')
    dateTimeEnd = datetime.datetime.strptime(dateTimeStrEnd, '%Y-%m-%d %H:%M')

    print(getStockDataTD('NIO', dateTimeStart, dateTimeEnd))


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