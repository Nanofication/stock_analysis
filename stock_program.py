from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError
from utils import math_calcs
import mplfinance as fplt
import numpy as np

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


def getData(ticker):
    try:
        stockData = data.DataReader(ticker,
                                    'yahoo',
                                    '2021-1-1',
                                    '2021-2-19')

        stockData.insert(loc=0, column='Counter', value=np.arange(len(stockData)))
        stockData = stockData.reset_index()
        stockData = stockData.astype({'Date': 'datetime64'})
        stockData = stockData.set_index('Date')
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
    prevOpen = df['Open'].iloc[0]
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
        prevOpen = row['Open']

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

if __name__ == '__main__':
    stock = 'AAPL'
    stockData = getData(stock)
    print(stockData.head())
    up, down = getPivotPoints(stockData)
    print(up)

    pair = generateTrendLine(down,reverse=True)
    # trendLine = [(up[0][0].strftime("%Y-%m-%d"), up[0][1]), (up[10][0].strftime("%Y-%m-%d"), up[10][1])]
    # trendLine = [('2020-01-02', 73.79), ('2021-03-02', 128.72)]

    trendLine = [(pair['Date1'].strftime("%Y-%m-%d"), pair['Pivot1']), (pair['Date2'].strftime("%Y-%m-%d"), pair['Pivot2'])]

    fplt.plot(
        stockData,
        type='candle',
        style='charles',
        title=stock,
        ylabel='Price',
        alines=trendLine
    )

