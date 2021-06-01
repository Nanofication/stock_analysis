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
        self.marginOfError = 0.05

    def calcTouchPoints(self):
        touchPoint = 0
        for p in self.pivots:
            if self.yIntercept + self.slope * p[2] == p[1] * self.marginOfError:
                touchPoint += 1
        return touchPoint

def getData(ticker):
    try:
        stockData = data.DataReader(ticker,
                                    'yahoo',
                                    '2020-1-1',
                                    '2021-3-8')

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
    print(prevBarIsGreen)

    for index, row in df.iloc[1:].iterrows():
        if row['Close'] > row['Open'] and prevBarIsGreen == False:
            supportPivots.append((index.to_pydatetime(),row['Open'], row['Counter'])) # Note: You could have prev red candle as start of pivot (OR?)
            prevBarIsGreen = True
        elif row['Close'] < row['Open'] and prevBarIsGreen:
            resistancePivots.append((index.to_pydatetime(),row['Open'], row['Counter']))
            prevBarIsGreen = False

    return supportPivots, resistancePivots

def generateTrendLine(pivots, startTime, endTime):
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


    return 0

if __name__ == '__main__':
    stockData = getData('AAPL')
    print(stockData.head())
    up, down = getPivotPoints(stockData)
    print(up)

    print(Trendline(up[0], up[10], up).calcTouchPoints())

    trendLine = [(up[0][0].strftime("%Y-%m-%d"), up[0][1]), (up[10][0].strftime("%Y-%m-%d"), up[10][1])]
    # trendLine = [('2020-01-02', 73.79), ('2021-03-02', 128.72)]

    fplt.plot(
        stockData,
        type='candle',
        style='charles',
        title='Apple',
        ylabel='Price',
        alines=trendLine
    )

