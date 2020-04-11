#coding:UTF-8
#Author: Toryun
#Date: 2020-04-05
#Function: Taylor 可以转换成c/c++
#sympy have a function could achieve this method:series(method,n)
#你也可以使用sympy库里的series函数得到Taylor展开式

from sympy import *
import math


class Taylor():
    '''泰勒展开公式，可以获取
    @自定义函数，
    @N的阶乘，
    @函数的N阶导数，
    '''
    
    def __init__(self):
        x = Symbol("x")#自变量x
        n = Symbol("n")#阶乘n
        self.x = x
        self.n = n
        
    def Factorial_N(self,i):
        '''Factorial of N N的阶乘
        你可以使用sympy自带的阶乘计算函数sympy.factorial(n)
        '''
        b = 1
        l = []
        s = 'x'
        t = i
        while t != 0:
            b*=t
            l.append(str(t))
            t-=1
        #s = str(b) + " = " + s.join(l)
        return b

    def f_Nderivative(self,n,f):
        '''N derivative of function函数的N阶导数'''
        l = [f]
        for i in range(n+1):
                f = diff(f)
                l.append(f)
        l.pop()
        return l
        
    def f(self,x):
        '''自定义function'''
        s = input('Please input the function:\n')
        return s
    
if __name__ =='__main__':
    x = Symbol("x")
    Taylor = Taylor()
    n = input("Please input the biggest factorial in this function:\n(We don't suggest you input too big, it will handle much time)\n")
    f = Taylor.f(x)
    Factorial = Taylor.Factorial_N(n)
    Nderivative = Taylor.f_Nderivative(n,f)
    taylor_f = 0.0
    for i in range(0,n+1):
        taylor_f += Taylor.f_Nderivative(i,f)[i]*pow(x,i)/Taylor.Factorial_N(i)
    pprint("{0}'s {1} Factorial Taylor formula is \n{2}".format(f,n, taylor_f))
    
    

