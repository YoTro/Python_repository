#coding:UTF-8
import math
import time
import numpy

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
            return mid
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
            print "We find this value in {}th postion".format(i)
    t1 = time.time()
    T = t1 - t0
    print "Regular_search Total time is {}".format(T)
    return T
if __name__ =='__main__':
    d = [i for i in xrange(1,1000000000)]
    #v = int(raw_input('Please input the value you want to search:\n'))
    v = d[-1]
    p0 = Binary_search(d,v)
    p1 = regular_search(d,v)
    if p1>p0:
        print "Binary_search is better than regular_search\n faster than {} ".format(p1-p0)
    elif p1<p0:
        print "Regular_search is better than binary_search\n faster than {} ".format(p0-p1)
    else:
        print "Tweedledum and Tweedledee 半斤八两"
    
