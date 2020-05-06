import MT19937
import math
import numpy as np
import matplotlib.pyplot as plt

def mt():
    res = []
    n = 10000
    for i in xrange(n):
        #生成624位0到1的均匀分布数字
        res.append([math.sin(MT19937.MT19937(i).extract_number()), math.cos(MT19937.MT19937(i).extract_number())])
    return res
def Marsaglia_polar():
    Gauss = []
    res = mt()
    for i in xrange(len(res)):
        u = res[i][0]
        v = res[i][1]
        s = u**2 + v**2
        if s < 1:
            x = u*math.sqrt(-2*math.log(s)/s)
            y = v*math.sqrt(-2*math.log(s)/s)
            Gauss.append([x,y])
    return Gauss
if __name__ == '__main__':
    r = Marsaglia_polar()
    s = np.array(r).reshape(2,len(r))
    plt.plot(s[0], s[1], 'r.')
    plt.show()
