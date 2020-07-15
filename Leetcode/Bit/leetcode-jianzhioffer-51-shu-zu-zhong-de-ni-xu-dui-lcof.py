#coding:utf-8
#视频讲解:BV1pE41197Qj
import bisect
class BIT:
    '''
    树状数组维护区间和
    '''
    def __init__(self, n):
        self.n = n
        self.tree = [0] * (n + 1)

    @staticmethod
    def lowbit(x):
        '''非负整数x在二进制表示下最低位1及其后面0所构成的数值
        x & ~x+1
        其中, ~x+1 = -x
        例如:
        lowbit(4)=lowbit(('0b100'))=('0b100')=4
        '''
        return x & (-x)
    
    def query(self, x):
        '''
        查询区间x内的前缀和、
        时间最坏复杂:O(logn)
        '''
        ret = 0
        while x > 0:
            ret += self.tree[x]
            x -= BIT.lowbit(x)
        return ret

    def update(self, x):
        '''
        更新叶子节点及其父节点的值
        tree[x]的父节点就等于tree[x+lowbit(x)]
        时间最坏复杂度: O(logn)
        '''
        while x <= self.n:
            self.tree[x] += 1
            x += BIT.lowbit(x)

class Solution:
    def reversePairs(self, nums):
        '''时间复杂度:O(nlogn)'''
        n = len(nums)
        # 离散化
        tmp = sorted(nums)
        for i in range(n):
            nums[i] = bisect.bisect_left(tmp, nums[i]) + 1
        # 树状数组统计逆序对
        print nums
        bit = BIT(n)
        ans = 0
        for i in range(n - 1, -1, -1):
            ans += bit.query(nums[i] - 1)
            bit.update(nums[i])
            print ans,  bit.tree
        return ans

if __name__ == '__main__':
    nums = [-3,4,2,-1,0,0,2,7,1000]
    solution = Solution()
    ans = solution.reversePairs(nums)
    print ans
