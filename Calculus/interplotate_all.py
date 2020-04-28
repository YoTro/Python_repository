#coding:utf-8
#Author: Toryun
#Date: 2020-04-29 1:54:00
#Function: all interpolate func

from scipy.interpolate  import interp1d
import numpy as np
import pylab as plt

n = 100
x = np.arange(n)
y = np.sin(x)
kinds = ['linear', 'nearest', 'zero', 'slinear', 'quadratic', 'previous', 'next', 'zero', 'slinear', 'quadratic' ]
x0 = np.linspace(0,10,1010)
for kind in kinds:
    f = interp1d(x,y,kind,fill_value="extrapolate")
    y0 = f(x0)
    plt.plot(x0,y0,label=str(kind))
plt.legend(loc="lower right")
plt.show()

