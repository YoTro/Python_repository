#coding:UTF-8
#Author: Toryun
#Date: 2020-04-14 07:24:00
#Function: finding next[i] of KMP Algorithm 最长公共前后缀长度

def Next(a):
    assert len(a)>0 and len(a)<65
    assert isinstance(a,str)
    next = [0]*len(a)
    for i in range(1,len(a)):
        for j in range(i):
            if a[:j+1] == a[i-j:i+1]:
                next[i] = j
                #print j,i,a[:j+1],a[i-j:i+1],next[i]
            else:
                continue

    next[0] = -1
    return next

if __name__ == '__main__':
    arr = 'AABAAC'
    next = Next(arr)
    print(next)
