#coding:utf-8

import math
import random
def primeNumber(n):
    '''
    获取1,n(包括n)之间的质数
    Sieve of Eratosthenes
    '''
    isPrime = [1]*(n+1)
    for i in range(2, int(math.sqrt(n))+1):
        if isPrime[i]:
            for j in range(i**2, n+1, i):
                isPrime[j] = 0
                print j
    return [isPrime[i]*i for i in range(n+1) if isPrime[i]*i != 0]

if __name__ == '__main__':
    n = random.randrange(100)
    print("1~{}的素数是:\n{}\n总共个数是:{}\n".format(n, primeNumber(n), len(primeNumber(n))))
