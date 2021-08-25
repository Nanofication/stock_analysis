from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError
from pandas.tseries.offsets import BDay
from utils import stock_utils
import pandas as pd
import numpy as np
import datetime
import mplfinance as fplt

class MATrading:
    """
    50 MA is a commonly used support level by stock community
    The key is to trade or enter when the stock is near but not breaking down
    past the 50 MA level
    """
    def __init__(self, ticker, startDate, endDate, ma=50):
        self.ticker = ticker
        self.ma = ma
        self.startDate = startDate
        self.endDate = endDate
        self.marginOfError = 0.05
        self.datesCloseToMa = []

    def generateStockDate(self):
        start = self.startDate - BDay(self.ma)
        start = start.strftime('%Y-%m-%d')
        end = self.endDate.strftime('%Y-%m-%d')

        return stock_utils.getTDData(self.ticker, start, end)

    def generateMAData(self,stockData=None):
        df = stockData if stockData else self.generateStockDate()
        df['MA'] = df.rolling(window=self.ma)['Close'].mean()
        df['Date'] = df.index.date
        return df

    def findCloseNearMa(self, df, percentDiff=0.03):
        """
        Find all close price above the MA line
        :param df: Dataframe with MA data
        :param percentDiff: percent difference
        :return: Dates where the close is within the percent Diff and above MA
        """
        df['Within Range'] = ((df['Close'] - df['MA'])/100 >= 0.0) & ((df['Close'] - df['MA'])/100 <= percentDiff) & (df['Close'] - df['Open'] > 0.0)
        self.datesCloseToMa = df[df['Within Range'] == True].index.date.tolist()
        return self.datesCloseToMa

    def findDateHighestPastPoint(self, df, date, lookBackDays=20):
        """
        Find highest stock point in the past x days
        :param df:
        :param lookBackDays:
        :return: Date of when stock hit highest point within lookback days
        """
        counter = df[df['Date']==date]['index'][0]
        if counter - lookBackDays > 0:
            return df[counter - lookBackDays:counter].sort_values(by=['High'], ascending=False)['Date'][0]

    def generateLocalTrendLine(self, df, startDate, endDate):
        """
        Generate a local trend line using the highest amount to the day before the stock is closest to MA
        :param df:
        :param startDate:
        :param endDate:
        :return: Treadline of stock price action
        """
        startCounter = df[df['Date']==startDate]['index'][0]
        endCounter = df[df['Date'] == endDate]['index'][0] - 1

        stockData = df.iloc[startCounter:endCounter]
        up, down = stock_utils.getPivotPoints(stockData)

        pair = stock_utils.generateTrendLine(down, reverse=True)

        trendLine = [(pair['Date1'].strftime("%Y-%m-%d"), pair['Pivot1']),
                     (pair['Date2'].strftime("%Y-%m-%d"), pair['Pivot2'])]
        return trendLine

class MACrossoverTrading:
    def __init__(self, ticker, startDate, endDate, ma1=50, ma2=200):
        self.ticker = ticker
        self.ma1 = ma1
        self.ma2 = ma2
        self.startDate = startDate
        self.endDate = endDate
        self.lookback = self.ma2 if self.ma2 > self.ma1 else self.ma1

class EMACrossoverTrading:
    def __init__(self, ticker, startDate, endDate, ema1=5, ema2=20):
        self.ticker = ticker
        self.ema1 = ema1
        self.ema2 = ema2
        self.startDate = startDate
        self.endDate = endDate
        self.lookback = self.ema2 if self.ema2 > self.ema1 else self.ema1

    def generateStockData(self):
        start = self.startDate - BDay(self.lookback)
        start = start.strftime('%Y-%m-%d')
        end = self.endDate.strftime('%Y-%m-%d')

        return stock_utils.getTDData(self.ticker, start, end)

    def generateEMAData(self, stockData=None):
        df = stockData if not stockData.empty else self.generateStockData()
        ema1 = 'EMA_{0}'.format(self.ema1)
        ema2 = 'EMA_{0}'.format(self.ema2)
        df[ema1] = df['Close'].ewm(span=self.ema1).mean()
        df[ema2] = df['Close'].ewm(span=self.ema2).mean()
        df['Is Positive'] = df[ema1].ge(df[ema2])
        return df[[ema1,ema2,'Is Positive']]

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

        backTestData = backTestData.set_index('Start Date')

        return backTestData.iloc[1:]

class DailyChartBase:

    def __init__(self, data):
        self.premarket, self.regularMarket, self.afterHourMarket = stock_utils.splitCandles(data)

    def getPremarketHigh(self):
        return self.premarket.sort_values(by=['High'], ascending=False).iloc[0]

    def getPremarketVolume(self):
        return self.premarket.sum(by=['High'], ascending=False).iloc[0]

class DipAndRip(DailyChartBase):
    """
    Strategy: Dip and Rip strategy involves a low float high volume premarket ticker gapping up preferably on news
    The stock opens weak and dips, then around 9:45 - 10:15 the stock reclaims the premarket high or HOD and
    rockets up.

    Test: How often does this happen, what does it usually look like
    """
    def __init__(self, data):
        super().__init__(data)

    def backTest(self, df, moneySpent=0, shareCount = 0):
        backTestData = pd.DataFrame(
            columns=['Trade Date', 'Start Time', 'End Time', 'Entry Price', 'Shares Bought', 'Exit Price', 'PnL', 'Win or Loss'])

        tradeDate = datetime.date.today()
        enterPrice = 0
        enterTime = datetime.time(7,0)
        sharesBought = 0


if __name__ == '__main__':
    # stock = 'AAPL' #TODO: API Crashed, check results tomorrow (We may need to figure out a way to pull and calculate data faster
    dateTimeStrStart = '2021-8-20 9:30'
    dateTimeStrEnd = '2021-8-20 16:00'
    dateTimeStart = datetime.datetime.strptime(dateTimeStrStart, '%Y-%m-%d %H:%M')
    dateTimeEnd = datetime.datetime.strptime(dateTimeStrEnd, '%Y-%m-%d %H:%M')

    data = stock_utils.getDailyDataTD('NIO',dateTimeStart, dateTimeEnd)
    dipRip = DipAndRip(data)
    print(dipRip.getPremarketHigh())

    # ma = EMACrossoverTrading('CWH', datetime.date(2020,1,2), datetime.date(2021,7,5))
    # df = ma.generateEMAData()
    # print(ma.backTest(df, 10000))

    # df = stock_utils.getAllStocks()
    # df = df[df['Has Data']==1]
    # df = df[df['Market Cap']> 3000000000]
    #
    # winLossEma = pd.DataFrame(columns=['Symbol','Start Date', 'End Date', 'Entry Price', 'Shares Bought', 'Exit Price', 'PnL', 'Win or Loss', 'Win Loss Percent'])
    #
    # count = 0
    #
    # for index, row in df.iterrows():
    #     ma = EMACrossoverTrading(row['Symbol'], datetime.date(2020, 1, 2), datetime.date(2021, 6, 23))
    #     try:
    #         emaData = ma.generateEMAData()
    #         ma = ma.backTest(emaData,10000)
    #
    #         winLoss = len(ma[ma['Win or Loss']== 'Win'])/len(ma)
    #         result = pd.concat([pd.DataFrame({'Symbol': [row['Symbol']] * len(ma)}), ma,
    #                             pd.DataFrame({'Win Loss Percent': [winLoss] * len(ma)})], axis=1)
    #         winLossEma = winLossEma.append(result)
    #     except Exception as e:
    #         print(e)
    #     count += 1
    #
    #     if count > 10:
    #         break
    #
    # winLossEma[['Symbol','Win Loss Percent']].drop_duplicates().to_excel("D:/The Fastlane Project/Coding Projects/Stock Analysis/results/win_loss.xlsx")

    # EMA SWING TRADING TEST

    # stock = 'NIO'
    # ma = MATrading(stock, datetime.date(2020,1,2), datetime.date(2021,6,3))
    # stockData = ma.generateMAData()
    # up, down = stock_utils.getPivotPoints(stockData)
    #
    # dates = ma.findCloseNearMa(stockData)
    # startDate = ma.findDateHighestPastPoint(stockData,dates[10])
    # trendLine = ma.generateLocalTrendLine(stockData, startDate, dates[10])
    # # print(up)
    # # pair = generateTrendLine(down, reverse=True)
    # #
    # # trendLine = [(pair['Date1'].strftime("%Y-%m-%d"), pair['Pivot1']),
    # #              (pair['Date2'].strftime("%Y-%m-%d"), pair['Pivot2'])]
    # fplt.plot(
    #     stockData,
    #     type='candle',
    #     style='charles',
    #     title=stock,
    #     ylabel='Price',
    #     alines=trendLine,
    #     mav=50
    # )
