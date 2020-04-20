import numpy as np
import matplotlib.pyplot as plt
import png_to_SVG

class curve_fit_func():
    def curvefunc(self):
        svgpath = png_to_SVG.svgpath('/Users/jin/Desktop/naruto.jpeg','/Users/jin/Desktop/n1.svg')
        X,Y = svgpath.svg_path()
        #定义x、y散点坐标
        x = np.array(X)
        num1 = [0.472,0.469,0.447,0.433,0.418,0.418,0.418,0.418,0.418,0.418]
        num2 = [0.337,0.327,0.325,0.316,0.312,0.311,0.308,0.305,0.295,0.290]
        #y1 = np.array(num1)
        #y2 = np.array(num2)
        y = np.array(Y)
         
        #用3次多项式拟合
        f1 = np.polyfit(x, y, 53)
        p1 = np.poly1d(f1)
        #print(p1)#打印出拟合函数
        yvals1 = p1(x)  #拟合y值
         
        #f2 = np.polyfit(x, y2, 3)
        #p2 = np.poly1d(f2)
        #print(p2)
        #也可使用yvals=np.polyval(f1, x)
        #yvals2 = p2(x)
         
        #绘图
        plot1 = plt.plot(x, y, 'k.',label='original values')
        plot2 = plt.plot(x, yvals1, 'r.',label='polyfit values')
        #plot3 = plt.plot(x, y2, 's',label='original values2')
        #plot4 = plt.plot(x, yvals2, 'r',label='polyfit values2')
         
         
        plt.xlabel('x')
        plt.ylabel('y')
        plt.legend(loc=2, bbox_to_anchor=(1.05,1.0),borderaxespad = 0.)
        plt.title('polyfitting')
        plt.savefig('nihe1.png')
        plt.show()
        plt.close()
        return p1

if __name__ == '__main__':
    f = curve_fit_func()
    func = f.curvefunc()
    print(func)
    t = np.arange(256)
    sp = np.fft.fft(func(t))
    freq = np.fft.fftfreq(t.shape[-1])
    plt.plot(freq, sp.real, freq, sp.imag)
    plt.show()

