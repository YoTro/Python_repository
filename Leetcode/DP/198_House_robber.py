import numpy as np
class Solution(object):
    '''打家劫舍'''
    def rob(self, nums):
        """
        :type nums: List[int]
        :rtype: int
        """
        if len(nums) == 0:
            return 0
        if len(nums) == 1:
            return nums[0]
        res = np.zeros(len(nums))
        res[0] = nums[0]
        res[1] = max(nums[0], nums[1])
        for i in range(2,len(nums)):
            a = res[i-2]+nums[i]
            b = res[i-1]
            res[i] = max(a,b)
        return int(max(res))
