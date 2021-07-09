from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError
from pandas.tseries.offsets import BDay
from utils import math_calcs, stock_utils
import pandas as pd
import numpy as np
import datetime

def getTDData(ticker, start, end):
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

class MATrading:
    def __init__(self, ticker, startDate, endDate, ma=50):
        self.ticker = ticker
        self.ma = ma
        self.startDate = startDate
        self.endDate = endDate
        self.marginOfError = 0.05

    def generateStockDate(self):
        start = self.startDate - BDay(self.ma)
        start = start.strftime('%Y-%m-%d')
        end = self.endDate.strftime('%Y-%m-%d')

        return getData(self.ticker, start, end)

    def generateMAData(self):
        df = self.generateStockDate()
        df['MA'] = df.rolling(window=self.ma)['Close'].mean()
        return df

    def findPivotsCloseToMa(self, df):
        up, down = getPivotPoints(df)
        allPivots = up.extend(down)
        for pv in allPivots:
            # Is PV below
            print(pv)
            # distance formula, get closest pivots - maybe have a margin of error?

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

    def backTest(self, df, moneySpent=0, shareCount = 0):
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
                sharesBought = shareCount if shareCount else moneySpent//enterPrice
                prevIsNegative = False

            if isCrossedOverNegative:
                exitPrice = row['Close']
                exitDate = index.to_pydatetime()
                backTestData = backTestData.append(
                    {'Start Date':enterDate,
                     'End Date': exitDate,
                     'Entry Price': enterPrice,
                     'Exit Price': exitPrice,
                     'Shares Bought': sharesBought,
                     'PnL': (exitPrice - enterPrice) * sharesBought,
                     'Win or Loss': 'Win' if exitPrice - enterPrice > 0 else 'Loss'}, ignore_index=True)

                prevIsNegative = True


        return backTestData.iloc[1:]

if __name__ == '__main__':
    # stock = 'AAPL' #TODO: API Crashed, check results tomorrow (We may need to figure out a way to pull and calculate data faster
    # ma = EMACrossoverTrading('CWH', datetime.date(2020,1,2), datetime.date(2021,7,5))
    # df = ma.generateEMAData()
    # print(ma.backTest(df, 10000))

    df = stock_utils.getAllStocks()
    df = df[df['Has Data']==1]
    df = df[df['Market Cap']> 3000000000]

    winLossEma = pd.DataFrame(columns=['Symbol','Start Date', 'End Date', 'Entry Price', 'Shares Bought', 'Exit Price', 'PnL', 'Win or Loss', 'Win Loss Percent'])

    count = 0

    for index, row in df.iterrows():
        ma = EMACrossoverTrading(row['Symbol'], datetime.date(2020, 1, 2), datetime.date(2021, 6, 23))
        try:
            emaData = ma.generateEMAData()
            ma = ma.backTest(emaData,10000)

            winLoss = len(ma[ma['Win or Loss']== 'Win'])/len(ma)
            result = pd.concat([pd.DataFrame({'Symbol': [row['Symbol']] * len(ma)}), ma,
                                pd.DataFrame({'Win Loss Percent': [winLoss] * len(ma)})], axis=1)
            winLossEma = winLossEma.append(result)
        except Exception as e:
            print(e)
        count += 1

        if count > 10:
            break

    winLossEma[['Symbol','Win Loss Percent']].drop_duplicates().to_excel("D:/The Fastlane Project/Coding Projects/Stock Analysis/results/win_loss.xlsx")

    # ma = MATrading('ABNB', datetime.date(2020,1,2), datetime.date(2021,6,3))
    # stockData = ma.generateMAData()
    # up, down = getPivotPoints(stockData)
    # print(up)
    # pair = generateTrendLine(down, reverse=True)
    #
    # trendLine = [(pair['Date1'].strftime("%Y-%m-%d"), pair['Pivot1']),
    #              (pair['Date2'].strftime("%Y-%m-%d"), pair['Pivot2'])]