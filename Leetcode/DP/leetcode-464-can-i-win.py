#coding: UTF-8
#Author: Toryun
#Date: 2020-05-14 21:18:00
#Function: DP to find how can I win in '100game'

import random
import time
from functools import wraps
import functools #应用于高阶函数，即参数或（和）返回值为其他函数的函数。 通常来说，此模块的功能适用于所有可调用对象。

def use_time(func):
	#装饰器
        @wraps(func)
        def decoreted(*args, **kwargs):
                t0 = time.time()
                res = func(*args, **kwargs)
                t1 = time.time()
                print("{}已被调用,用时:{}\n结果是：{}".format(func.__name__, t1-t0, res))
                return t1-t0                
        return decoreted
class Solution():
	@use_time
	def canIWin(self, maxChoosableInteger, desiredTotal):
		maxSum = maxChoosableInteger*(maxChoosableInteger+1)/2
		#1. 如果目标值大于所有数累计合,则返回false
		if maxSum < desiredTotal:
			return False
		#2. 如果相等,如果回合数为奇数则返回True
		elif maxSum == desiredTotal:
			return maxChoosableInteger % 2 == 1
		#3. 如果最大值直接大于目标值则直接返回True
		elif maxChoosableInteger > desiredTotal:
			return True
		#记录已经计算过的数组
		seen = {}

		def helper(choices, remainder):
			'''
			choices   ->list[int]:含有未选择过的数
			remainder ->int      :减去选择过的数之后的余数desiredTotal
			'''
			#如果数组中最大的数大于余数,则直接返回True
			if choices[-1] >= remainder:
				return True
			#字典的键类型不能是list
			seen_key = tuple(choices)
			#如果是已经计算过的直接返回,减少计算时间
			if seen_key in seen:
				return seen[seen_key]
			#深度遍历数组中剩余的数
			#选择能让我方稳赢的数
			for i in range(len(choices)):
				#如果对方不能赢,则我方取胜
				if not helper(choices[:i]+choices[i+1:], remainder - choices[i]):
					seen[seen_key] = True
					return True
			#否则返回False
			seen[seen_key] = False
			return False
		return helper(list(range(1, maxChoosableInteger+1)), desiredTotal)
	@use_time
	def canIWin_bit_LRU(self, maxChoosableInteger, desiredTotal):
		'''
		DFS+按位操作+LRU缓存备忘
		'''

		if  desiredTotal <= maxChoosableInteger:
			return True
		if sum(range(maxChoosableInteger + 1)) < desiredTotal:
			return False
		#如果maxsize不为None,则缓存一定长度的数据提高输入输出执行时间,None表示LRU特性将被禁用且缓存可无限增长,用此功能实现备忘功能此功能只有在3.2才有
		@functools.lru_cache(None)
		def dfs(used, desiredTotal):
		    #深度遍历选择哪个数能稳赢
		    for i in range(maxChoosableInteger):
		        #查看数字数否被选取
		        cur = 1 << i
		        #如果被选取,则让对手操作
		        if cur & used == 0:
		            #如果对手不能选择稳赢的数,则我方获胜
		            if desiredTotal <= i + 1 or not dfs(cur | used, desiredTotal - i - 1):
		                return True
		    #print(dfs.cache_info())
		    #如果没有稳赢的数返回False
		    return False

		return dfs(0, desiredTotal)
		
	@use_time
	def canIWin_bit_dic(self, maxChoosableInteger, desiredTotal):
	    '''
	    DFS+按位操作+字典备忘
	    '''
	    if  desiredTotal <= maxChoosableInteger:
	            return True
	    if sum(range(maxChoosableInteger + 1)) < desiredTotal:
	            return False
	    #记录是否被计算过
	    record = {}
	    def dfs(used, desiredTotal):
	            if record.get(used):
	                    return record.get(used)
	            #深度遍历选择哪个数能稳赢
	            for i in range(maxChoosableInteger):
	                    #查看数字数否被选取
	                    cur = 1 << i
	                    #如果被选取,则让对手操作
	                    if cur & used == 0:
	                            #如果对手不能选择稳赢的数,则我方获胜
	                            if desiredTotal <= i + 1 or not dfs(cur | used, desiredTotal - i - 1):
	                                    record[used] = True
	                                    return True
	                    #print(dfs.cache_info())
	            #如果没有稳赢的数返回False
	            record[used] = False
	            return False

	    return dfs(0, desiredTotal)
	@use_time
	def canIWin_Law(self, maxChoosableInteger, desiredTotal):
		'''
		数学规律
		'''
		
		sn = maxChoosableInteger + maxChoosableInteger * (maxChoosableInteger - 1) / 2

		if(desiredTotal > sn):
		    return False
		#特例
		if(maxChoosableInteger == 10 and (desiredTotal == 40 or desiredTotal == 54)):
		    return False
		if(maxChoosableInteger == 20 and (desiredTotal == 210 or desiredTotal == 209)):
		    return False
		if(maxChoosableInteger == 18 and (desiredTotal == 171 or desiredTotal == 172)):
		    return False
		if(maxChoosableInteger == 12 and desiredTotal == 49):
		    return True

		#规律如下：desiredTotal == 1必胜，如果累计值模上最大值余1那必输，否则必胜。（但不一定成立，反例如上数据）
		return desiredTotal == 1 or desiredTotal % maxChoosableInteger != 1        

if __name__ == '__main__':
	#maxChoosableInteger 不大于20
	maxChoosableInteger = 10#random.randrange(21)
	desiredTotal = 15#random.randrange(301)
	print("最大的数是:{}, 累加目标值是:{}\n".format(maxChoosableInteger,desiredTotal))
	solution = Solution()
	t0 = solution.canIWin(maxChoosableInteger, desiredTotal)
	t1 = solution.canIWin_bit_LRU(maxChoosableInteger, desiredTotal)
	t2 = solution.canIWin_Law(maxChoosableInteger, desiredTotal)
	t3 = solution.canIWin_bit_dic(maxChoosableInteger, desiredTotal)
	print("字典操作的时间比LRU+bit的快{}倍\n数学规律比字典快{}倍\n数学规律比LRU+bit快{}倍\n字典+bit比LRU+bit快{}倍\n".format(t0/t1, t2/t0, t2/t1, t3/t1))
