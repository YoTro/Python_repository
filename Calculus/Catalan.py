#coding:UTF-8
#Author: Toryun
#Date: 2020-05-08 16:42:00

def Catalan(n):
    '''求卡特兰数'''
    if n <= 0:
        return None
    if n == 1:
        return 1
    else:
        return Catalan(n-1)*2*(2*n-1)/(n+1)
    
