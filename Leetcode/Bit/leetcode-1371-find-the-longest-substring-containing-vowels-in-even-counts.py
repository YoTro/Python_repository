#coding:utf-8
class Solution:
    '''每个元音包含偶数次的最长子字符串'''
    def findTheLongestSubstring(self, s):
        '''
        用到了状态压缩+动态规划/hashmap
        用'00000'-'11111'表示元音'aeiou'出现的次数为奇数或偶数

        解题思路
        对于代码中一些变量的解释

        这里由于只考虑每个元音奇偶次数，因此考虑用二进制来记录；

        定义特征，aeiou分别对应二进制00001，00010，00100，01000，10000
        其中0表示对应元音出现了偶数次数，1表示奇数
        从左往右遍历字符串，不断更新dp；

        dp[pattern]的作用是用来记录当前索引值下对应的元音奇偶次数组合特征；
        例如：如果pattern为10，也就是对应二进制 01010，dp[pattern] = 8的意思为，当索引值为8的时候，e和o都出现了奇数次，其它元音为偶数次。

        如何找到符合条件最大长度？

        根据异或运算规律，异或本身为0，所以当重复出现偶数次，对应位变为0，否则为1
        由这个规律可以断定，当再次出现这个pattern的时候，一定出现了偶数次
        为了方便解释，pattern如下用二进制表示：
        例如，pattern的值变化为 31-->30-->28-->29-->31
        对应的二进制位[11111]-->[11110]-->[11100]-->[11101]-->[11111]
        一个合理的字符串变化： ‘aeiou’ --> 'aeioua'-->'aeiouae'-->'aeiouaea'-->'aeiouaeae'
        由此可见，从'aeiou'到'aeiouaeae'这个过程中，多余出来的‘aeae’为符合条件的字符串
        所以，在这个过程中，不管中间发生了什么样的变化，这两个状态之间对应的元音为偶数，也就是一定符合题意的字符串
        因此，不断更新res，来获得最大字符串长度
        代码步骤解释

        首先初始化dp长度为32，对应了5个元音每个次数或奇或偶一共32种状态
        异或运算部分：
        遍历字符串，从起始pattern，也就是0开始，不断根据对应情况做异或运算
        如果出现的是辅音，不进行异或运算
        如果出现的是元音，根据元音种类分别对应做异或运算
        答案更新：
        如果当前的pattern没有出现过，那么以这个pattern为键，记录下当前位置，也就是索引的位置
        如果出现过，那么更新目标长度：
        这里有两种情况：
        当前如果不是元音，相当于i变化了1，而dp[pattern]不变，相当于辅音的时候直接增加1的长度
        如果是元音，说明出现了偶数次的元音，那么i-dp[pattern]相当于在原来基础上增加了一部分长度，这一部分长度满足偶数次的元音

        举个例子，s='leetcodeo'

        dp[0]=-1,为了计算长度方便定义，或者理解为，开始计算前，参照点在第一个字符之前，也就是-1的位置
        i=0,l不是元音,pattern不变，
        i=1,e是元音，pattern变化，由0变为2，二进制下为 [00010] dp对应当前pattern对应索引更新为1
        i=2,e是元音，pattern变化，由2变为0，二进制下为 [00000] 即初始状态，更新res为 2-(-1) = 3
        i=3,t不是元音，pattern不变，res更新为 3-(-1) = 4
        i=4,c不是元音，pattern不变，res更新为 4-(-1) = 5
        i=5,o是元音，pattern变化，由0变为8，二进制下为[01000],dp对应当前pattern对应索引更新为5
        i=6,d不是元音，pattern不变，对应cur_len为 6-5=1，没有5大，所以当前res还是5
        i=7,e是元音，pattern变化，由8变为10，二进制下为[01010],dp对应当前pattern对应索引更新为7
        i=8,o是元音，pattern变化，由10变为2，二进制下为[00010],由于这个pattern在i=1出现过，所以用这个对应的i来更新cur_len = i-dp[pattern] = 8-1 = 7, 同时res更新到7
        遍历结束，结果为7

        '''
        dp = [-float('inf')]*32
        dp[0] = -1
        pattern = 0
        res = 0
        print ("i p cn rs")
        for i in range(len(s)):
            if s[i] == 'a':
                pattern^= (1<<0)
            elif s[i] == 'e':
                pattern^= (1<<1)
            elif s[i] == 'i':
                pattern^= (1<<2)
            elif s[i] == 'o':
                pattern^= (1<<3)
            elif s[i] == 'u':
                pattern^= (1<<4)
            if dp[pattern] != -float('inf'):
                cur_len = i-dp[pattern]
                res = max(res,cur_len)
                #print (i, pattern, cur_len, res)
                #print dp
            else:
                dp[pattern] = i
                #print dp[pattern]
                #print (i, pattern, res)
        #print dp
        return res


