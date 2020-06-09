#coding:utf-8
#Author: Toryun
#Date: 2020-06-10 2:00:00
#Function: 堆排序

import time
import numpy as np

class heap():
    '''堆类似于一个完全二叉树'''
    def __init__(self, arr):
        self.arr = arr
        self.size = len(arr)
    def swap(self,arr, i, j):
        '''交换函数'''
        arr[i], arr[j] = arr[j], arr[i]
    def heapify(self, arr, i):
        '''从堆顶开始进行比较,把最大值(或者最小值)放在父节点'''
        lchild = 2*i + 1 #初始化左节点(下标为1)
        rchild = 2*i + 2 #初始化右节点(下标为2)
        largestnode = i  #初始化父节点(下标为0)
        #如果左节点存在(在堆大小范围内),且左节点大于父节点,则临时最大根是左节点
        if lchild < self.size and arr[lchild] > arr[largestnode]:
            largestnode = lchild
        if rchild < self.size and arr[rchild] > arr[largestnode]:
            largestnode = rchild
        #如果进行了最大根下标改变了,则交换它们位置, 并递归地进行下一轮的heapify
        if largestnode != i:
            self.swap(arr, i, largestnode)
            self.heapify(arr, largestnode)
    def buildMaxHeap(self, arr):
        '''构建大根堆'''
        #从数组中间为根出发(向下取整)
        for i in range(int(self.size/2), -1, -1):
            self.heapify(arr, i)
    def MaxHeap(self):
        if self.size == 0:
            return []
        self.buildMaxHeap(self.arr)
        return self.arr

    def heapsort(self):
        '''
        堆排序
        :把最大值放在堆顶,然后和堆尾交换,self.size--来控制循环范围
        
        '''
        #构建大根堆
        self.arr = self.MaxHeap()
        print("构建大根堆:{}\n".format(self.arr))
        #进行深度遍历
        for i in range(self.size - 1, 0, -1):
            self.swap(self.arr, 0, i)
            self.size -= 1
            self.heapify(self.arr, 0)
        return self.arr

if __name__ == '__main__':
    arr = np.arange(10)
    np.random.shuffle(arr)
    print("初始化:{}\n".format(arr))
    t0 = time.time()
    Heap = heap(arr)
    arr = Heap.heapsort()
    t1 = time.time()
    print("排序后:{}\n".format(arr))
    T = t1 - t0
    print("Heapsort totoal time is {}s\n".format(T))
