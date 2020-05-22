#coding:utf-8
#Author:Toryun
#Date: 2020-05-22 04:02:00
#Function: coin-change

import random

class Solution():
    def coinChange(self, coins, amount):
        '''
        兑换零钱,使用动态规划记录所有面值的纸币到达amount目标值的结果
        coins: list[int]
        amount: int

        1.找子问题/最优子结构: 每次获取一枚某面值(coin)的硬币(+1)之前的状态dp(S-coin), dp(S-coin)也是
        
        1. 明确状态：这里的状态是指原问题和子问题中变化的变量，从问题出发,我们发现要凑成刚好的硬币数, 只有amount是可变的, 它可以当作状态, 因为硬币的个数是无限的，所以唯一的状态就是目标的金额amount。

        2. 定义dp数据/函数的含义：这里和定义递归函数是一样的，递归的一个非常重要的点就是：不去管函数的内部细节是如何处理的，我们只看其函数作用以及输入与输出。这里的含义就是指函数作用：我们定义一个dp数组，dp[n]表示当前目标金额是n，至少需要dp[n]个硬币凑出该金额。

        3. 明确选择：通过我们当前的选择来改变我们的状态，可参考下面的伪代码：

        '''
        print("硬币面值有{}\n,总金额为{}".format(coins, amount))
        dp = [float("inf")]*(amount+1)
        #总金额为0时,币数为0
        dp[0] = 0
        for coin in coins:
            #匹配所有的金额在选取所有面额时的最小个数
            for i in range(coin, amount+1):
                #当总金额为i时,dp[i-coin]表示为上一个金额(i-coin)选取面值coin前的最小币数
                dp[i] = min(dp[i-coin]+1, dp[i])
        return dp[amount] if dp[amount] != float("inf") else -1
    def coinChange_dfs(self, coins, amount):
        """
        dfs+剪枝
        动态显示:http://pythontutor.com/live.html#code=class%20Solution%28object%29%3A%0A%20%20%20%20def%20coinChange%28self,%20coins,%20amount%29%3A%0A%20%20%20%20%20%20%20%20'''%0A%20%20%20%20%20%20%20%20dfs%2B%E5%89%AA%E6%9E%9D%0A%20%20%20%20%20%20%20%20'''%0A%20%20%20%20%20%20%20%20if%20amount%20%3D%3D%200%3A%0A%20%20%20%20%20%20%20%20%20%20%20%20return%200%0A%20%20%20%20%20%20%20%20coins.sort%28reverse%3DTrue%29%0A%20%20%20%20%20%20%20%20self.res%20%3D%20amount%20%2B%201%0A%20%20%20%20%20%20%20%20def%20helper%28remiander,%20index,%20c%29%3A%0A%20%20%20%20%20%20%20%20%20%20%20%20'''dfs%E9%81%8D%E5%8E%86%E6%89%80%E6%9C%89%E5%AD%90%E6%95%B0%E7%BB%84'''%0A%20%20%20%20%20%20%20%20%20%20%20%20if%20remiander%20%3D%3D%200%3A%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20self.res%20%3D%20min%28self.res,%20c%29%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20return%0A%20%20%20%20%20%20%20%20%20%20%20%20if%20index%20%3D%3D%20len%28coins%29%3A%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20return%0A%20%20%20%20%20%20%20%20%20%20%20%20for%20x%20in%20range%28remiander/coins%5Bindex%5D,%20-1,%20-1%29%3A%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20if%20x%20%2B%20c%20%3C%20self.res%3A%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20helper%28remiander%20-%20x*coins%5Bindex%5D,%20index%2B1,%20x%2Bc%29%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20else%3A%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20break%0A%20%20%20%20%20%20%20%20helper%28amount,%200,%200%29%0A%20%20%20%20%20%20%20%20return%20-1%20if%20self.res%20%3D%3D%20amount%20%2B%201%20else%20self.res%0A%0Aif%20__name__%20%3D%3D%20'__main__'%3A%0A%20%20%20%20amount%20%3D%208%0A%20%20%20%20coins%20%3D%20%5B1,2,5%5D%0A%20%20%20%20solution%20%3D%20Solution%28%29%0A%20%20%20%20res%20%3D%20solution.coinChange%28coins,%20amount%29%0A%20%20%20%20print%20res&cumulative=true&curInstr=72&heapPrimitives=true&mode=display&origin=opt-live.js&py=2&rawInputLstJSON=%5B%5D&textReferences=false
        """
        if amount == 0:
            return 0
        coins.sort(reverse=True)
        self.res = amount + 1
        def helper(remiander, index, c):
            '''dfs遍历所有子数组'''
            #如果余数刚好为0, 表示凑到了可以合成amount的币数
            if remiander == 0:
                self.res = min(self.res, c)
                return
            #如果coins的指针index越界,则返回上一层递归
            if index == len(coins):
                return
            #从大到小地遍历能够得到amount的面值和数量
            for ci in range(remiander/coins[index], -1, -1):
                #如果该面值的数量加上已有的硬币数量小于已知的res,则继续往下递归,直到凑够或着超过amount
                if ci + c < self.res:
                    helper(remiander - ci*coins[index], index+1, ci+c)
                #否则返回
                else:
                    break
        #从最大的面值开始递归深度遍历
        helper(amount, 0, 0)
        return -1 if self.res == amount + 1 else self.res

if __name__ == '__main__':
    coins = [1,5,10,20,50,100]
    amount = random.randrange(1000)
    solution = Solution()
    print solution.coinChange(coins, amount)
    print solution.coinChange_dfs(coins, amount)
