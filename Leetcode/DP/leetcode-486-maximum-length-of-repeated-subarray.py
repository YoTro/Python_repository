class Solution(object):
    def PredictTheWinner(self, nums):
        """
        1. 状态转移方程：dp(x,y) = dp(x+2,y) && dp(x+1,y-1) || dp(x,y-2) && dp(x+1,y-1)
        2. dp（x，y）解释：玩家1第一个从第x到第y个元素中取是否能获胜。
        3. 玩家取x后，下一次再取的时候会有两种情况dp(x+2,y) && dp(x+1,y-1)这两种情况是根据玩家2的两种拿法来的。
        4. 同理，玩家取y后，也有两种情况dp(x,y-2) && dp(x+1,y-1)
        5. 所以，如果取x的两种情况都能赢或者取y的两种情况都能赢玩家1就能赢。
        :type nums: List[int]
        :rtype: bool
        """
        l = len(nums)
        if l == 0:
            return False
        if l == 1:
            return True
        #如果是偶数个的数组,稳赢
        if l % 2 == 0:
            return True
        dp = [[0]*(l) for i in range(l)]
        #从下往上,从左往右填写二维数组
        for i in range(l-2, -1, -1):
            for j in range(i+1, l, 1):
                a = nums[i] - dp[i+1][j]
                b = nums[j] - dp[i][j-1]
                dp[i][j] = max(a, b)
        print dp
        return dp[0][l-1] >= 0

if __name__ == '__main__':
    solution = Solution()
    nums = [3,5,6,2,7,2,8]
    dp = solution.PredictTheWinner(nums)
    print dp