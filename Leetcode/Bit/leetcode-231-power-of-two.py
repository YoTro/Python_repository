#coding:utf-8
class Solution(object):
    '''位运算,2的幂的规律是所有数在二进制中都只还有一个1其余都是0'''
    def isPowerOfTwo(self, n):
        if n == 0:
            return False
        return n & (-n) == n
