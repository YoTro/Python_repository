#coding:UTF-8
#Author: Toryun
#Date: 2020-04-11 15:10:00
#Function: png,jpeg to svg
import sys
import time
from PIL import Image
import matplotlib.pyplot as plt
import tkinter as tk
from tkinter import messagebox
from tkinter.filedialog import (askopenfilename, 
                                    askopenfilenames, 
                                    askdirectory, 
                                    asksaveasfilename)

class svgpath():
    def __init__(self,infile,outfile):
        self.X = []
        self.Y = []
        self.infile = infile
        self.outfile = outfile
        
    def svg_path(self):
        X = []
        Y = []
        plt.figure()
        fig, ax = plt.subplots()
        #infile = askopenfilename(title = "Please choose the filepath in the directory",initialdir='/')
        infile = '/Users/jin/Desktop/naruto.jpeg'
        image = Image.open(infile).convert('RGBA')
        #image.show()
        print image.size[0]
        print image.size[1]
        data = image.load()
        outfile = '/Users/jin/Desktop/n1.svg'
        out = open(outfile, "w")
        out.write('<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n')
        out.write('<svg id="svg2" xmlns="http://www.w3.org/2000/svg" version="1.1" width="%(x)i" height="%(y)i" viewBox="0 0 %(x)i %(y)i">\n' % {'x':image.size[0], 'y':image.size[1]})
        for y in range(image.size[1]):
            for x in range(image.size[0]):
                rgba = data[x, y]
                
                if rgba[2] < 200 and rgba[0] <210 and rgba[1]<200:
                    #print '更改前：{}'.format(rgba)
                    rgba = (0,0,0,255)
                    #print '更改后：{}'.format(rgba)
                if rgba[0] < 200 and rgba[1]<200 and rgba[2]<200:
                    X.append(x)
                    Y.append(y)
                rgb = '#%02x%02x%02x' % rgba[:3]
                if rgba[3] > 0:
                    out.write('<path d="M{0} {1} L{2} {3} Z" stroke="{4}" fill-opacity="{5}" />\n'.format(x, y, x+1, y, rgb, rgba[3]//255.0))
        line, = plt.plot(X, Y, 'k.')
        out.write('</svg>\n') 
        out.close()
        #plt.show()
        return X,Y

if __name__ == '__main__':
    svgpath = svgpath('/Users/jin/Desktop/naruto.jpeg','/Users/jin/Desktop/n1.svg')
    X,Y = svgpath.svg_path()
    



