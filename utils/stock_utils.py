import pandas

def getAllStocks():
    """
    Get complete list of stocks from excel. Excel sheet saved a list of all stocks to read from Nasdaq, NYSE
    :return: Dataframe of Stocks
    """
    return pandas.read_excel('D:\The Fastlane Project\Coding Projects\Stock Analysis\stocks\stock_data.xlsx')

if __name__ == '__main__':
    print("Getting all stocks")
    df = pandas.read_excel('D:\The Fastlane Project\Coding Projects\Stock Analysis\stocks\stock_data.xlsx')
    print(df)