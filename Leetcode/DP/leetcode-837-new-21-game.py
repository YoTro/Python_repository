#coding:utf-8

import random

class Solution():
    '''新21点'''
    def new21game(self, N, K, W):
        '''
        N: int
        K: int
        W: int
        rtype: float
        '''
        print("N = {}\nK = {}\nW = {}".format(N, K, W))
        if K > N:
            return 0.0
        if K == 0:
            return 0
        dp = [0.0]*(N+W+1)
        for i in range(K, N+1):
            dp[i] = 1.0
        dp[K-1] = float(min(N - K, W))/W
        for j in range(K-1, -1, -1):
            dp[j-1] = dp[j] - (dp[j+W] - dp[j])/W
        return dp[0]
if __name__ == '__main__':
    N = random.randrange(100)
    K = random.randrange(21)
    W = random.randrange(1, 100)
    solution = Solution()
    print("从[1,{}]等概率抽取数字,得分超过{}就停止抽取, 得分不超过{}的概率为{}".format(W, K, N, solution.new21game(N,K,W)))
