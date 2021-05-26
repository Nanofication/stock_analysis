from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError

def getData(ticker):
    try:
        stockData = data.DataReader(ticker,
                                    'yahoo',
                                    '2020-1-1',
                                    '2021-3-8')
        print(stockData)

        stockData = stockData.reset_index()
        stockData = stockData.astype({'Date': 'datetime64'})
        return stockData
    except RemoteDataError:
        print('No Data found for {0}'.format(ticker))


if __name__ == '__main__':
    stockData = getData('DGLY')