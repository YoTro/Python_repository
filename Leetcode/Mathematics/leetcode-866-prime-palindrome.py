class Solution(object):
    #注意到除了11以外，没有其它的两位或四位回文素数。如果我们考虑被11整除的判别法，就可以推出任何偶数位的回文数都能被11整除。所以，除了11以外，所有的回文素数都有奇数个数字
    def isPrime(self, number):
        '''
        :type number: int
        :rtype: bool
        '''
        import math
        if (number < 2):
            return False
        for i in xrange(2, int(math.sqrt(number))+1):
            if number % i == 0:
                return False
        return True
    def isPalindromic(self, number):
        a = str(number)
        if a[::-1] != a:
        	return False
        return True
    def primePalindrome(self, N):
        """
        :type N: int
        :rtype: int
        """
        partprimepalindromic = [2, 3, 5, 7, 11, 101, 131, 151, 181, 191, 313, 353, 373, 383, 727, 757, 787, 797, 919, 929, 10301, 10501, 10601, 11311, 11411, 12421, 12721, 12821, 13331, 13831, 13931, 14341, 14741, 15451, 15551, 16061, 16361, 16561, 16661, 17471, 17971, 18181]
        if(N <= 18181):
            for j in xrange(42):
                if (N <= partprimepalindromic[j]):
                    return partprimepalindromic[j]
        else:
            for i in xrange(N, 10**8):
                if self.isPrime(i):
                    if self.isPalindromic(i):
                        return i
                if 10**7 < N and N < 10**8:
	                return 100030001


class Solution(object):
    def primePalindrome(self, N):
        def is_prime(n):
            return n > 1 and all(n % d for d in xrange(2, int(n**.5) + 1))

        def reverse(x):
            ans = 0
            while x:
                ans = 10 * ans + x % 10
                x /= 10
            return ans

        while True:
            if N == reverse(N) and is_prime(N):
                return N
            N += 1
            if 10**7 < N < 10**8:
                N = 10**8

