import math
def maxProfit( prices):
    """
    :type prices: List[int]
    :rtype: int
    """
    if len(prices) == 0 or len(prices) == 1:
        return 0
    minprice = int(1e9)
    maxprice = 0
    for i in prices:
        
        maxprice = max(i - minprice,maxprice)
        minprice = min(minprice,i)
        print i,maxprice,minprice
    return maxprice
        
if __name__ =='__main__':
    nums = [2,4,1,4,6,8,4]
    w = maxProfit(nums)
    print w
