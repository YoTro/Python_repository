#coding:utf-8
#Author: Toryun
#Date: 2020-05-26 02:36:00
#Function: Generate random maze by DFS
import random
import numpy as np
import time

L = random.randint(7,21) #迷宫大小

#墙和路定义
Wall = 0
Path = 1
wall = "#"
path = "."
#迷宫复杂度, 数值越大,迷宫越简单
Rank = 0
RandMAX = 32767
#初始化迷宫
maze = np.zeros((L,L))

def CreateMaze(x, y):
	'''
	(x, y)
	1. 随机四个方向开始挖、
	2. 控制挖的范围不能回头
	3. 检测挖的位置周围是否挖穿
	重复这一过程
	'''
	maze[x][y] = Path
	#选择方向开挖
	#所以需要生成一个随机方向数组
	direction = [[-1,0], [0,1], [0,-1], [1,0]]
	random.shuffle(direction)
	for i in range(4):
		#设置迷宫复杂度
		if Rank == 0:
			rank = 1
		else:
			rank = 1 + random.randint(0, RandMAX) % Rank
		while rank > 0:
			dx = x + direction[i][0]
			dy = y + direction[i][1]
			#如果回头则break
			if maze[dx][dy] == Path:
				break

			#判断是否挖空
			count = 0
			for j in range(dx - 1, dx + 2, 1):
				for k in range(dy - 1, dy + 2, 1):
					#判断(dx,dy)新位置的上下左右位置是否只有一个坐标为通路
					if abs(j-dx) + abs(k-dy) == 1 and maze[j][k] == Path:
						count += 1
			#挖穿退出
			if count > 1:
				break
			rank -= 1
			maze[dx][dy] = Path
		if rank <= 0:
			CreateMaze(dx, dy)

def showMaze(maze):
	'''
	打印迷宫
	'''
	for i in range(len(maze)):
		for j in range(len(maze[0])):
			if maze[i][j]:
				print(path),
			else:
				print(wall),
		print("\n")

if __name__ == '__main__':
	for i in range(len(maze)):
		for j in range(len(maze[0])):
			maze[i][0] = Path
			maze[i][L-1] = Path
			maze[L-1][j] = Path
			maze[0][j] = Path
	CreateMaze(2, 2)
	showMaze(maze)
	#行列各复制一次扩展
	maze1 = np.repeat(maze, 2, axis=1)
	maze1 = np.repeat(maze1, 2, axis=0)
	print("New maze")
	showMaze(maze1)
