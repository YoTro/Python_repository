#coding:UTF-8
#Author: Toryun
#Date: 2020-04-17 16:41:00
#https://leetcode-cn.com/problems/sqrtx/

from __future__ import division
import random
#由于2.7中整型间的运算得出的结果只能是整型,所以引入division
import matplotlib.pyplot as plt
import time
import numpy as np

class Solution:
    '''使用牛顿法进行极值运算'''
    def NewtonSqrt(self, x):
        if x < 2:
            return x
        
        x0 = x
        x1 = (x0 + x / x0) / 2
        while abs(x0 - x1) >= 10**-6:
            x0 = x1
            x1 = (x0 + x / x0) / 2        
            #print(x1)
        return int(x1)
    def BinarySqrt(self, x):
        if x < 2:
            return x
        
        left, right = 2, x // 2
        
        while left <= right:
            m = left + (right - left) // 2
            num = m * m
            #print(left, right)
            if num > x:
                right = m -1
            elif num < x:
                left = m + 1
            else:
                return m
            
        return right
    
    def RecursiveSqrt(self, x):
        if x < 2:
            return x
        
        left = self.RecursiveSqrt(x >> 2) << 1
        right = left + 1
        return left if right * right > x else right

    def LogSqrt(self, x):
        if x < 2:
            return x
        
        left = int(np.e**(0.5 * np.log(x)))
        right = left + 1
        return left if right * right > x else right

    def Compare_time(self,t = 100):
        T = []
        for x in range(t):
            t0 = time.time()
            sq_x = self.NewtonSqrt(x)
            t1 = time.time()
            sq_x_1 = self.BinarySqrt(x)
            t2 = time.time()
            sq_x_2 = self.RecursiveSqrt(x)
            t3 = time.time()
            sq_x_3 = self.LogSqrt(x)
            t4 = time.time()
            T.append([t1-t0,t2-t1,t3-t2,t4-t3])
        return T
    def implot(self,T,t = 100):
        fig, ax = plt.subplots()
        ax.autoscale()
        plt.title('Compare time')
        plt.xlabel('x')
        plt.ylabel('y')
        line1 = ax.plot(range(t),T[:,0],'r-',label='Newton')
        line2 = ax.plot(range(t),T[:,1],'b-',label='Binary')
        line3 = ax.plot(range(t),T[:,2],'y-',label='Recursive')
        line4 = ax.plot(range(t),T[:,3],'k-',label='Recursive')
        ax.legend()
        plt.show()
if __name__ == '__main__':
    x = random.randrange(10000)
    print('x的值是:{}'.format(x))
    mysq = Solution()
    sq_x = mysq.NewtonSqrt(x)
    print('牛顿法求的x 的平方根整数位是:{}'.format(sq_x))
    sq_x_1 = mysq.BinarySqrt(x)
    print('二分法求的x 的平方根整数位是:{}'.format(sq_x_1))
    sq_x_2 = mysq.RecursiveSqrt(x)
    print('递归求的x 的平方根整数位是:{}'.format(sq_x_2))
    sq_x_3 = mysq.LogSqrt(x)
    print('对数表求的x 的平方根整数位是:{}'.format(sq_x_3))
    try:
        n = int(raw_input('Please input the number of compare:\n'))
    except Exception as e:
        print(str(e)+'\nPlese input int type,default number of compare is 100')
        n = 100
    T = np.array(mysq.Compare_time(n))
    mysq.implot(T,n)
