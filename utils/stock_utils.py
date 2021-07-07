from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError
from pandas.tseries.offsets import BDay
from dask import delayed
import pandas
import datetime
import numpy as np

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
    df = pandas.read_excel('D:\The Fastlane Project\Coding Projects\Stock Analysis\stocks\stock_data.xlsx')

    # print(df)
    df2 = parallelUpdate(df, datetime.date(2021,6,20))
    df2 = df2.compute()

    data = []
    for x in df2:
        data.append(pandas.DataFrame(x))

    # print(df.loc[0])
    # print(df2.loc[0])
    finalDf = pandas.concat(data)
    print("out")
    finalDf.to_excel('D:\The Fastlane Project\Coding Projects\Stock Analysis\stocks\stock_data_2.xlsx', engine='xlsxwriter', index=False)