class Solution(object):
	'''最长回文子串'''
    def longestPalindrome(self, s):
        """
        动态规划
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
    def longestPalindrome_Center(self, s):
        """
        中心扩散算法
        :type s: str
        :rtype: str
        """
        r = len(s)
        if r < 2:
            return s
        res = s[0]
        for i in range(r):
            palindromic_odd, odd_len = self.helper(s, r, i, i)
            palindromic_even, even_len = self.helper(s, r, i, i+1)
            maxpalindromic = palindromic_odd if odd_len > even_len else palindromic_even
            if len(maxpalindromic) > len(res):
                res = maxpalindromic
        return res
    def helper(self, s, r, left, right):
        i = left
        j = right
        while i >= 0 and j < r and s[i] == s[j]:
            j += 1
            i -= 1
        return s[i+1:j], j-i-1