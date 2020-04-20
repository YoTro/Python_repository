import random
class Solution(object):
    def tribonacci(self, n):
        """
        :type n: int
        :rtype: int
        类型:递归
        """
        assert n>=0 and n <= 37
        if n == 0:
            return 0
        if n == 1:
            return 1
        if n == 2:
            return 1
        else:
            t0, t1, t2 = 0, 1, 1
            for i in range(n-2):
                t0, t1, t2 = t1, t2, t0 + t1 + t2
            assert t2 <= (1<<31) - 1
            return t2
    def Recursivetribonacci(self,n):
        if n == 0:
            return 0
        elif n == 1 or n == 2:
            return 1
        else:
            return self.Recursivetribonacci(n-1)+self.Recursivetribonacci(n-2)+self.Recursivetribonacci(n-3)
if __name__ == '__main__':
    n = random.randrange(37)
    print('n = {}'.format(n))
    s = Solution()
    tribo1 = s.tribonacci(n)
    tribo2 = s.Recursivetribonacci(n)
    print(tribo1,tribo2)
