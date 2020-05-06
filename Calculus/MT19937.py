#coding:UTF-8
#Function: generate a random number伪随机发生器产生均匀分布序列
#梅森旋转算法:http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/ARTICLES/mt.pdf
import math
import matplotlib.pyplot as plt
import numpy as np
def _int32(x):
    return int(0xFFFFFFFF & x)

class MT19937:
    def __init__(self, seed):
        self.mt = [0] * 624
        self.mt[0] = seed
        self.mti = 0
        for i in range(1, 624):
            self.mt[i] = _int32(1812433253 * (self.mt[i - 1] ^ self.mt[i - 1] >> 30) + i)


    def extract_number(self):
        if self.mti == 0:
            self.twist()
        y = self.mt[self.mti]
        y = y ^ y >> 11
        y = y ^ y << 7 & 2636928640
        y = y ^ y << 15 & 4022730752
        y = y ^ y >> 18
        self.mti = (self.mti + 1) % 624
        return _int32(y)


    def twist(self):
        for i in range(0, 624):
            y = _int32((self.mt[i] & 0x80000000) + (self.mt[(i + 1) % 624] & 0x7fffffff))
            self.mt[i] = (y >> 1) ^ self.mt[(i + 397) % 624]

            if y % 2 != 0:
                self.mt[i] = self.mt[i] ^ 0x9908b0df

if __name__ == '__main__':
    res = []
    n = 1000
    for i in range(0,n):
        #生成624位0到1的均匀分布数字
        res.append([math.sin(MT19937(i).extract_number()), math.cos(MT19937(i).extract_number())])
    a = np.array(res).reshape(2,n)
    plt.plot(a[0], a[1], 'r.')
    plt.show()
