def waysToStep( n):
    """
    :type n: int
    :rtype: int 
    """
   
    if n == 1:
        return 1
    if n == 2:
        return 2
    if n == 3:
        return 4
    dp = [0]*n
    dp[0] = 1
    dp[1] = 2
    dp[2] = 4
    for i in range(3, n):
        dp[i] = (dp[i-1]+dp[i-2]+dp[i-3])
    #print dp
    return dp
dp = waysToStep( random)
