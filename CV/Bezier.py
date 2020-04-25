#coding:UTF-8

import numpy as np
import random
import matplotlib.pyplot as plt
import matplotlib.animation as animation



class Bezier():
    '''贝塞尔曲线'''
    def Bezier_1(self, p0, p1):
        '''
        一阶贝塞尔曲线
        p0:起始点
        p1:终点
        '''
        b1 = []
        for t in np.arange(0,1,0.001):
            x = p0[0]*(1-t) + t*p1[0]
            y = p0[1]*(1-t) + t*p1[1]
            b1.append([x,y])
        return np.array(b1)
    def Bezier_2(self, p0, p1, p2):
        '''
        二阶贝塞尔曲线
        p0:起始点
        p1:中间点
        p2:终点      
        '''
        b2 = []
        for t in np.arange(0,1,0.001):
            x = (1-t)*(1-t)*p0[0] + 2*t*(1-t)*p1[0] + p2[0]*t*t
            y = (1-t)*(1-t)*p0[1] + 2*t*(1-t)*p1[1] + p2[1]*t*t
            b2.append([x,y])
        return np.array(b2)
    def Bezier_3(self,p0, p1, p2, p3):
        '''
        二阶贝塞尔曲线
        p0:起始点
        p1:中间点
        p2:中间点
        p3:终点      
        '''
        b3 = []
        for t in np.arange(0,1,0.001):
            x = (1-t)*(1-t)*(1-t)*p0[0] + 3*t*(1-t)*(1-t)*p1[0] + 3*t*t*(1-t)*p2[0] + p3[0]*t**3
            y = (1-t)*(1-t)*(1-t)*p0[1] + 3*t*(1-t)*(1-t)*p1[1] + 3*t*t*(1-t)*p2[1] + p3[1]*t**3
            b3.append([x,y])
        return np.array(b3)

  
def init():
    ax.set_xlim(p0[0],2)
    ax.set_ylim(-2,2)
    #x_ticks = np.arange(-4, 4, 0.1)
    #y_ticks = np.arange(-4, 4, 0.1)
    #plt.xticks(x_ticks)
    #plt.xticks(y_ticks)
    line1.set_data([],[])
    return line1,

def animate(frame):
    xdata.append(b1[frame][0])
    ydata.append(b1[frame][1])
    x2data.append(b2[frame][0])
    y2data.append(b2[frame][1])
    x3data.append(b3[frame][0])
    y3data.append(b3[frame][1])
    line1.set_data(xdata,ydata)
    line2.set_data(x2data,y2data)
    line3.set_data(x3data,y3data)
    return line1,

if __name__ == '__main__':
    curve = Bezier()
    '''p0 = np.random.normal(0,1,2)*2
    p1 = np.random.normal(0,1,2)*2
    p2 = np.random.normal(0,1,2)*2
    p3 = np.random.normal(0,1,2)*2'''
    p0 = [-2,0]
    p1 = [1,1]
    p2 = [0,2]
    p3 = [1,0]
    print("贝塞尔曲线的四个点坐标是\n{}\n{}\n{}\n{}\n".format(p0,p1, p2, p3))
    b1 = curve.Bezier_1(p0,p1)
    b2 = curve.Bezier_2(p0,p1, p2)
    b3 = curve.Bezier_3(p0,p1, p2, p3)
    fig, ax = plt.subplots()
    xdata, ydata = [], []
    x2data,y2data = [], []
    x3data,y3data = [], []
    line1, = ax.plot([],[],'ro')
    ani = animation.FuncAnimation(fig, animate,frames=np.arange(len(b1)),init_func = init,blit = True,interval = 2)
    line2, = ax.plot([],[],'b-')
    ani = animation.FuncAnimation(fig, animate,frames=np.arange(len(b1)),init_func = init,blit = True,interval = 2)
    line3, = ax.plot([],[],'y-')
    ani = animation.FuncAnimation(fig, animate,frames=np.arange(len(b1)),init_func = init,blit = True,interval = 2)
    ax.legend()
    plt.show()
