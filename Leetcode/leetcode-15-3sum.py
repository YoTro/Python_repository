class Solution(object):
    def threeSum(self, nums):
        """
        :type nums: List[int]
        :rtype: List[List[int]]
        """
        nums.sort()
        print nums
        a = nums[0]
        c = nums[-1]
        if a>0 or c<0:
            return []
        p = []
        i = 0
        for i in range(len(nums)):
            if nums[i]>0:
                return p
            if (i > 0 and nums[i] == nums[i-1]):
                continue
            L = i+1
            R = len(nums)-1
            while L<R:
                print L,R,i
                if nums[L]+nums[R]+nums[i] == 0:
                    p.append([nums[L],nums[R],nums[i]])
                    while L<R and nums[L] == nums[L+1]:
                        L += 1
                    while L<R and nums[R] == nums[R-1]:
                        R -= 1
                    L += 1
                    R -= 1
                elif nums[L]+nums[R]+nums[i] >0:
                    R -= 1
                else:
                    L += 1
           
        return p
if __name__ =='__main__':
    asd = Solution()
    nums = [-1,0,1,2,-1,-4]
    w = asd.threeSum(nums)
    print w
