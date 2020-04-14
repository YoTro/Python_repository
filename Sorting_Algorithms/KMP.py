#coding:UTF-8
#Author: Toryun
#Date: 2020-04-14 07:24:00
#Function: finding next[i] of KMP Algorithm 最长公共前后缀长度

import time

class KMP():
    def Next(self,a):
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
    def KMP(self,S,a):
        '''
        a:匹配的关键字符串
        S:待匹配的长字符串
        '''
        assert len(a)<=len(S) and len(a)>0 and len(a)<65
        assert isinstance(a,str) and isinstance(S,str)
        next = self.Next(a)
        j = 0
        p = []
        i = 0
        while j < len(S):
            if S[j] != a[i]:
                i = next[i]
                if i == -1:
                    j += 1
                    i = 0
            else:
                j += 1
                i += 1
            if i == len(a)-1 and S[j] == a[i]:
                p.append(j-i)
                i = next[i]
                    
            #print S
            #print a
            #print j,i
        return p
if __name__ == '__main__':
    arr = 'AABA'
    S = 'AABAACAADAABAABAAABAACAADAABAABAAABAACAADAABAABAAABAACAADAABAABA'
    print(S)
    print(arr)
    t0 = time.time()
    kmp = KMP()
    p = kmp.KMP(S,arr)
    t1 = time.time()
    T = t1 - t0
    print("The \'{0}\' is in {1}th position, the total finding time is {2} seconds".format(arr,p,T))
