import random
class Solution(object):
    def arrangeCoins(self, n):
        """
        :type n: int
        :rtype: int
        """
        c = 0
        k = 0
        while c <n:
            k += 1
            c += k
            #print k,c
            if c>n:
                k -= 1
        
        return k
    def Arithmetic(self, n):
        '''等差数列公式
        i 为层数
        n 为硬币总数
        n = i(2-1+i)/2
        配合一元二次方程组求根公式
        x = -b+-(sqrt(b^2-4ac))/2a

        '''
        s1 = (((1+8*n)**0.5)-1)/2
        print  int(s1)
       
if __name__ =='__main__':
    Coin = Solution()
    n = random.randrange(10000)
    w = Coin.arrangeCoins(n)
    Coin.Arithmetic( n)
    print w
