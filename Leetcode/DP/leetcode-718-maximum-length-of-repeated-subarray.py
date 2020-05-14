#coding: UTF-8
class Solution(object):
    '''
    方法一:动态规划
    方法二:滑动其中一个字符串匹配重合的部分
    '''
    def findLength(self, A, B):
        """
        寻找最长重复子串
        :type A: List[int]
        :type B: List[int]
        :rtype: int
        """
        la = len(A)
        lb = len(B)
        dp = [[0]*(lb+1) for _ in range(la+1)]
        res = 0
        begin = 0
        end = 1
        for i in range(1, la+1):
            for j in range(1, lb+1):
                if A[i-1] == B[j-1]:
                    #状态转移方程
                    dp[i][j] = dp[i-1][j-1]+1
                    if dp[i][j] > res:
                        #print i, j
                        end = i 
                        begin = i - res -1                   
                    res = max(dp[i][j], res)

        
        print("The maximum repeated substring is\n{}").format( A[begin: end])
        return res

if __name__ =='__main__':
    longsubarray = Solution()
    a = 'faassvc'
    b = 'saassvdsfccasvc'
    print("Text1 is {}\nText2 is {}\n").format(a, b)
    res = longsubarray.findLength(a, b)
    print("The length of repeated substring is \n{}").format(res)
