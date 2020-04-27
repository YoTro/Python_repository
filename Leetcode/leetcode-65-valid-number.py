class machine():
    '''有限状态机DFS'''
    def __init__(self):
        self.state = [x for x in range(9)]
    def state(self):
        return self.state
    
    def isNumber(self, s):
        """
        :type s: str
        :rtype: bool
        一. 为什么会有9种状态
        -1 其它
        0 空格
        1 +/-
        2 小数点
        3 小数位
        4 E
        5 E后位的数
        6 数字
        7 科学计数法E后的+/-
        8 代表末位的空格
        二. 特别的数字
        '1.' 等于 1.0
        '+.9e8'或者'-.9e8'也是数字
        """
        b = ' '# 32
        positive_symbol = '+'#43
        nagtive_symbol = '-'#45
        number = '9'
        is_numbers = [chr(i) for i in range(48,58)]
        dot = '.'#46
        E = 'e'#101
        other = '-1'
        others = [32,43,45,46,101]
        dfa = { b:              [0,-1,-1,8,-1,8,8,-1,8],
                positive_symbol:[1,-1,-1,-1,7,-1,-1,-1,-1],
                nagtive_symbol: [1,-1,-1,-1,7,-1,-1,-1,-1],
                number:         [6,6,3,3,5,5,6,5,-1],
                dot:            [2,2,-1,-1,-1,-1,3,-1,-1],
                E:              [-1,-1,-1,4,-1,-1,4,-1,-1],
                other:          [-1,-1,-1,-1,-1,-1,-1,-1,-1]        
        }
        start = self.state[0]
        #print dfa
        for i in s:
            if i in is_numbers:
                i = number
            if ord(i) not in range(48,58):
                if ord(i) not in others:
                    i = other
            start = dfa[i][start]
            print i,start
            if start == -1:
                
                return False
        if start == 3 or start == 6 or start == 8 or start == 5:
            return True
        else:
            return False
        
            
            

if __name__ == '__main__':
    machine = machine()
    s = "0"
    t = machine.isNumber(s)
    print t
