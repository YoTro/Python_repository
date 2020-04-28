class Solution(object):
    def isHappy(self, n):
        """
        :type n: int
        :rtype: bool
        """
        tmp = 4
        while n>3:
            l = len(str(n))
            c = 0
            for i in range(l):
                c += (n/10**i%10)**2
            n = c
            if tmp == n:
                return False
            print(n)
        if n == 1:
            return n
        else:
            return False

if __name__ == '__main__':
    happy = Solution()
    n = random.randrange(100)
    #n = 6
    t = happy.isHappy(n)
    print(t)
