#coding:UTF-8
#Auther: Toryun
#Date: 2020-04-07 21:42:00
#Function: Insertion Sort
from random import *
import time

def Insertion_sort(arr):
    for i in range(1,len(arr)):
        j = i
        while j>0:
            
            if arr[j-1]>arr[j]:
                arr[j-1],arr[j] = arr[j],arr[j-1]
                j -= 1
            else:
                j = 0
            
if __name__ == '__main__':
    arr = [i for i in range(100)]
    shuffle(arr)
    print arr
    t0 = time.time()
    Insertion_sort(arr)
    t1 = time.time()
    T = t0 - t1
    print arr
    print "Insertion sort total time is {}".format(T)
