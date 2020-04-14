#coding:UTF-8
#Author: Toryun
#Date: 2020-04-14 23:29:00
#Function: Shell sort

import random

def Shellsort(arr):
    '''Insertion sort 2.0v 插入排序的优化版，原理：把插入排序中只能移动一个位置的步长变成移动较长的step_size,当step_size == 1的时候表示已经成为正常的线性排序了'''
    assert len(arr)>0 and isinstance(arr,list)
    l = len(arr)
    step_size = l/2
    while step_size > 0:
        for i in range(step_size):
            for j in range(i+step_size,l,step_size):
                k = j
                #print k
                while k > 0:
                    if arr[k-step_size] > arr[k]:
                        arr[k-step_size], arr[k] = arr[k],arr[k-step_size]
                        k -= step_size
                    else:
                        k = 0
        step_size /= 2
    return arr
    
if __name__ == '__main__':
    arr = [i for i in range(100)]
    random.shuffle(arr)
    n = Shellsort(arr)
    print(n)
            
    
