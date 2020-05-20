#coding:UTF-8
class Solution():
        '''
        def numSquares(self, n):
        
        暴力解法
        :type n: int
        :rtype: int
        
        if n == 0:
            return 0
        res = [i**2 for i in range(n+1)]
        #print res
        if n in res:
            return 1
        an = [1]*(n+1)
        for i in range(1, n+1):
            if i in res:
                continue
            s = []
            for j in range(1,i):
                if j in res:
                    s.append(an[j]+an[i-j])
                #print s
            an[i] = min(s)
        print(an)
        return an[-1]
        '''
    def numSquares_dp(self, n):
        """
        :type n: int
        :rtype: int
        """
        res = [i**2 for i in range(0, int(math.sqrt(n))+1)]
        
        dp = [float('inf')] * (n+1)
        dp[0] = 0
        
        for i in range(1, n+1):
            for j in res:
                if i < j:
                    break
                dp[i] = min(dp[i], dp[i-j] + 1)
        
        return dp[-1]
    def isSquare(self, n: int) -> bool:
        sq = int(math.sqrt(n))
        return sq*sq == n

    def numSquares(self, n):
        '''
        if n=4^k(8m+7): return 4
        '''
        # four-square and three-square theorems
        while (n & 3) == 0:
            n >>= 2      # reducing the 4^k factor from number
        if (n & 7) == 7: # mod 8
            return 4

        if self.isSquare(n):
            return 1
        # check if the number can be decomposed into sum of two squares
        for i in range(1, int(n**(0.5)) + 1):
            if self.isSquare(n - i*i):
                return 2
        # bottom case from the three-square theorem
        return 3




if __name__ == '__main__':
    solution = Solution()
    an = solution.numSquares_dp(23)
