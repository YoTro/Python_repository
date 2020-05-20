class Solution(object):
    def maxProduct(self, nums):
        """
        动态规划解决乘积最大子数组
        :type nums: List[int]
        :rtype: int
        """
        l = len(nums)
        #如果数组为空
        if l == 0:
            return 0
        #如果数组只有一个元素
        if l == 1:
            return nums[0]
        #如果数组中无0,而且无负数或者负数个数为偶数个,直接返回乘积
        sn = 1
        for i in range(l):
            sn *= nums[i]
        if sn > 0:
            return sn
        #第i个数的0位保存最大值, 1位保存最小值
        dp = [[0]*2 for _ in range(l)]
        dp[0] = [nums[0],nums[0]]
        for i in range(1, l):
            #状态转移方程
            dp[i] = [max(dp[i-1][0]*nums[i], max(dp[i-1][1]*nums[i], nums[i])), min(dp[i-1][1]*nums[i], min(dp[i-1][0]*nums[i], nums[i]))]
            #print dp[i]
        #print dp
        return max(max(dp))
