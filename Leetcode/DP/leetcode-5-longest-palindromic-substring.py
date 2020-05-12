class Solution(object):
	'''最长回文子串'''
    def longestPalindrome(self, s):
        """
        :type s: str
        :rtype: str
        时间复杂度：O(N^2)
		空间复杂度：O(N^2)
        """
        r = len(s)
        if r < 2:
            return s
        begin = 0
        end = 1
        #1. 用dp定义初始化状态,如果是单个字符,则一定是回文字符串:True
        dp = [[1]*(r) for _ in range(r)]
        for i in range(1, r):
            for j in range(0, i):
            	#2. 状态转移方程
                if s[i] == s[j]:
                    #print i,j,s[i]
                    dp[i][j] = dp[i-1][j+1]
                    #print dp[i-1][j+1]
                else:
                    dp[i][j] = 0
                #3. 根据状态进行最长回文字符串的更新
                if dp[i][j]:
                    #print i,j
                    c = i-j+1
                    if c > end:
                        begin = j
                        end = c
        print dp
        #3. 有时候输出不是dp最后一项
        return s[begin: end+begin]
