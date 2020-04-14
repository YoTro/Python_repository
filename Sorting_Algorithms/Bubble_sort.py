#coding:UTF-8
#Author: Toryun
#Date: 2020-04-14 19:41:00
#Function: Bubble sort

import random
import time

def Bubble(arr):
    tmp = 0
    while tmp < len(arr):
        for i in  range(len(arr)-tmp-1):
            if arr[i] > arr[i+1]:
                arr[i], arr[i+1] = arr[i+1], arr[i]
                
            else:
                i += 1
        tmp += 1     
        #print(tmp,arr)
        #time.sleep(3)
        
if __name__ == '__main__':
    arr = [i for i in range(100)]#[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]
    random.shuffle(arr)
    #arr.reverse()
    Bubble(arr)
    print(arr)
