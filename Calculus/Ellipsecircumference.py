﻿'''
椭圆周长公式:
1. p ≈ 2π√[(a^2 + b^2)/2]                                           (a<=3b, error: 5%)
2. p ≈ π{3(a+b) - √[(3a+b)(a+3b)]}                                  (拉马努金, error:1/10^5)
3. p = 2aπ(1 - ∑(∞,i=1){{[(2i)!^2]/[(2^i)(i!)]^4}[(e^2i)/(2i - 1)]} (e: 偏心率)
4. p = π(a+b)∑(∞, n=0)[(0.5, n)^2](h^n)                             ((0.5, n)^2半整数阶乘的二项式系数)


'''
import math
def p(a,b):
    p1 = 2*math.pi*math.sqrt((a**2+b**2)/2.0)
    p2 = math.pi*(3*(a+b) - math.sqrt((3*a+b)*(a+3*b)))
    e = math.sqrt(a**2 - b**2)/a
    p3 = 2*a*math.pi*(1 - (1/4.0)*(e**2) - ((3/8.0)**2)*(e**4)/3.0 - ((15/48.0)**2)*(e**6)/5.0)
    h  = (a-b)**2/(a+b)**2
    p4 = math.pi*(a+b)*(1+(h/4.0)+((h**2)/64.0)+((h**3)/256.0)+(25*(h**4)/16384.0))
    return p1, p2, p3, p4

if __name__ == '__main__':
    t1, t2, t3, t4 = p(10,10)
    print t1, t2, t3, t4