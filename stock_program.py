from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError
import mplfinance as fplt



def getData(ticker):
    try:
        stockData = data.DataReader(ticker,
                                    'yahoo',
                                    '2020-1-1',
                                    '2021-3-8')

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
    uptrendPivots = []
    downtrendPivots = []
    prevBarIsGreen = df['Close'].iloc[0] > df['Open'].iloc[0]
    print(prevBarIsGreen)

    for index, row in df.iloc[1:].iterrows():
        if row['Close'] > row['Open'] and prevBarIsGreen == False:
            uptrendPivots.append((index,row['Open'])) # Note: You could have prev red candle as start of pivot (OR?)
            prevBarIsGreen = True
        elif row['Close'] < row['Open'] and prevBarIsGreen:
            downtrendPivots.append((index,row['Open']))
            prevBarIsGreen = False

    return uptrendPivots, downtrendPivots


if __name__ == '__main__':
    stockData = getData('AAPL')
    print(stockData.head())
    up, down = getPivotPoints(stockData)
    print(up)
    trendLine = [('2020-01-02', 73.79), ('2021-03-02', 128.72)]

    # fplt.plot(
    #     stockData,
    #     type='candle',
    #     style='charles',
    #     title='Apple',
    #     ylabel='Price',
    #     alines=trendLine
    # )

