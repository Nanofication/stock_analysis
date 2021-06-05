from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError
from pandas.tseries.offsets import BDay
import pandas as pd
import numpy as np
import datetime

def getData(ticker, start, end):
    try:
        stockData = data.DataReader(ticker,
                                    'yahoo',
                                    start,
                                    end)

        stockData.insert(loc=0, column='Counter', value=np.arange(len(stockData)))
        stockData = stockData.reset_index()
        stockData = stockData.astype({'Date': 'datetime64'})
        stockData = stockData.set_index('Date')
        return stockData
    except RemoteDataError:
        print('No Data found for {0}'.format(ticker))

class MATrading:
    def __init__(self, ticker, startDate, endDate, ma=50):
        self.ticker = ticker
        self.ma = ma
        self.startDate = startDate
        self.endDate = endDate

    def generateStockDate(self):
        start = self.startDate - BDay(self.ma)
        start = start.strftime('%Y-%m-%d')
        end = self.endDate.strftime('%Y-%m-%d')

        return getData(self.ticker, start, end)

    def generateMAData(self):
        df = self.generateStockDate()
        df['MA'] = df.rolling(window=self.ma)['Close'].mean()
        print(df)

class EMACrossoverTrading:
    def __init__(self, ticker, startDate, endDate, ema1=5, ema2=20):
        self.ticker = ticker
        self.ema1 = ema1
        self.ema2 = ema2
        self.startDate = startDate
        self.endDate = endDate
        self.lookback = self.ema2 if self.ema2 > self.ema1 else self.ema1

    def generateStockDate(self):
        start = self.startDate - BDay(self.lookback)
        start = start.strftime('%Y-%m-%d')
        end = self.endDate.strftime('%Y-%m-%d')

        return getData(self.ticker, start, end)

    def generateEMAData(self):
        df = self.generateStockDate()
        df['EMA_{0}'.format(self.ema1)] = df['Close'].ewm(span=self.ema1).mean()
        df['EMA_{0}'.format(self.ema2)] = df['Close'].ewm(span=self.ema2).mean()
        return df

    def backTest(self, df, moneySpent):
        """
        Back test this strategy against this stock and generate df of number of success failures and profit and losses
        :return: df
        """
        backTestData = pd.DataFrame(columns=['Start Date', 'End Date', 'Entry Price', 'Shares Bought', 'Exit Price', 'PnL', 'Win or Loss'])

        enterPrice = 0
        enterDate = datetime.date.today()
        sharesBought = 0

        prevIsNegative = df['EMA_{0}'.format(self.ema1)].iloc[0] < df['EMA_{0}'.format(self.ema2)].iloc[0]

        for index, row in df.iloc[1:].iterrows():
            isCrossedOverPositive = prevIsNegative and (row['EMA_{0}'.format(self.ema1)] > row['EMA_{0}'.format(self.ema2)])
            isCrossedOverNegative = not prevIsNegative and (
                row['EMA_{0}'.format(self.ema1)] < row['EMA_{0}'.format(self.ema2)])

            if isCrossedOverPositive:
                enterPrice = row['Open']
                enterDate = index.to_pydatetime()
                sharesBought = moneySpent//enterPrice
                prevIsNegative = False

            if isCrossedOverNegative:
                exitPrice = row['Close']
                exitDate = index.to_pydatetime()
                backTestData = backTestData.append({'Start Date':enterDate,
                                     'End Date': exitDate,
                                     'Entry Price': enterPrice,
                                     'Exit Price': exitPrice,
                                     'Shares Bought': sharesBought,
                                     'PnL': (exitPrice - enterPrice) * sharesBought,
                                     'Win or Loss': exitPrice - enterPrice > 0}, ignore_index=True)

                prevIsNegative = True


        return backTestData

if __name__ == '__main__':
    stock = 'AAPL'
    ma = EMACrossoverTrading('DIS', datetime.date(2020,1,2), datetime.date(2021,6,3))
    df = ma.generateEMAData()
    print(ma.backTest(df, 10000))