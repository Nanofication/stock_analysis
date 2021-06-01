
def getLineGraph(x1, y1, x2, y2):
    slope = (y2-y1)/(x2-x1)
    yIntercept = y2 - slope * x2

    return slope, yIntercept

if __name__ == '__main__':
    print(getLineGraph(2,2,1,1))