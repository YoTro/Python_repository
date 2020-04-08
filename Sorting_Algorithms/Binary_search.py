#coding:UTF-8
#Author: Toryun
#Date: 2020-04-05 21:42:00
#Function: Binary search
import math
import time
import numpy
import os

def Binary_search(listdata,value):
    low = 0
    high = len(listdata) - 1
    if len(listdata) == 0:
        print "{} is None".format(listdata)
        return high
    elif not isinstance(listdata,list):
        print "{} is not list type".format(listdata)
    if not isinstance(value,int) or isinstance(value,float):
        print "{} is not int or float type".format(value)
    t0 = time.time()
    mid = (high - low)/2
    while mid>-1:
        if listdata[mid] == value:
            print "{} is in {}th postion".format(value,mid+1)
            break
        elif listdata[mid] < value:
            low = mid + 1
            mid = (high - low) / 2 + low
        else:
            high = mid -1 
            mid = (high - low) / 2
    t1 = time.time()
    T = t1 - t0
    print "Binary_search Total time is {}".format(T)
    return T


def regular_search(listdata,value):
    t0 = time.time()
    for i in range(len(listdata)):
        if listdata[i] == value:
            print "We find this value in {}th postion".format(i+1)
    t1 = time.time()
    T = t1 - t0
    print "Regular_search Total time is {}".format(T)
    return T
if __name__ =='__main__':
    d = [i for i in xrange(1,1000)]
    #v = int(raw_input('Please input the value you want to search:\n'))
    l0 = []
    l1 = [0,0,0]
    for v in range(1,1000):
        p0 = Binary_search(d,v)
        p1 = regular_search(d,v)
        p = p1 - p0
        l1[0],l1[1],l1[2]= p0,p1,p
        l0.append(l1)
    with open(os.getcwd()+'/binary_searchTest.txt','a+') as f:
        for i in range(len(l0)):
            f.write(str(l0[i]))
        f.close()
        
        
        
    
