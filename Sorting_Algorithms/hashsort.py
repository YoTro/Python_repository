#coding:UTF-8
#Author: Toryun
#Date: 2020-04-13 02:33:00
#Function: Use hashmap to sort list

def hashsort(arr1):
    """
    :type arr1: List[int]
    :rtype: List[int]
    通过hashmap进行排序O(n+m)
    """
    tmp = -2**31
    for i in range(len(arr1)):
        if arr1[i]>tmp:
            tmp = arr1[i]
    #print tmp
    r = []
    b = [0]*(tmp+1)
    for i in arr1:
        #把所有的存在arr1数组的元素放入hashmap里
        b[i] += 1
    for i in range(len(b)):
        r += [i]*b[i]
    return r

if __name__ == '__main__':
    arr1 = [2, 2, 1, 4, 3, 9, 6, 7, 13, 0, 25]
    s = hashsort(arr1)
    print s
