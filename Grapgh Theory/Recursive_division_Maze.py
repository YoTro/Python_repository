#coding:utf-8
#Author: Toryun
#Date: 2020-05-26 20:57:00
#Function: Recursive segmentation to build random maze 递归分割法构建随机迷宫
import numpy as np 
import random

L = random.randint(6, 21)#定义迷宫初始长度
PATH = 0                 #路
WALL = 1                 #墙
RAND_MAX = 32767         #随机范围

class Maze():
	def __init__(self):
		self.maze = np.zeros((L,L))#初始化迷宫
		#外围都是墙
		for i in range(L):
			self.maze[i][L-1] = WALL
			self.maze[L-1][i] = WALL
			self.maze[0][i] = WALL
			self.maze[i][0] = WALL

	def showMaze(self, maze):
		'''打印迷宫'''
		for i in range(len(maze)):
			for j in range(len(maze[0])):
				if maze[i][j] == WALL:
					print("#"),
				else:
					print(" "),
			print("\n")

	def CreateMaze(self, x1, y1, x2, y2):
		'''
		随机选择矩形内的一个点进行分割,然后打通三面墙, 然后继续分割最后停止‘’‘
		x2->int: 对角线上的两点
		y2->int: 
		x1->int: 
		y1->int: 

		'''
		#停止条件
		if x2 - x1 < 2 or y2- y1 <= 2:
			return
		#随机取点
		x = x1 + 1 + random.randint(0,RAND_MAX)%(x2 - x1 - 1)
		y = y1 + 1 + random.randint(0,RAND_MAX)%(y2 - y1 - 1)

		#开始分割
		for i in range(x1, x2+1):
			self.maze[i][y] = WALL
		for i in range(y1, y2+1):
			self.maze[x][i] = WALL

		#递归分割
		self.CreateMaze(x1, y1, x-1, y-1)
		self.CreateMaze(x1, y+1, x-1, y2)
		self.CreateMaze(x+1, y+1, x2, y2)
		self.CreateMaze(x+1, y1, x2, y-1)

		#开始通路
		#随机取三面墙
		r = [0,0,0,1]
		random.shuffle(r)

		for i in range(4):
			if r[i] == 0:
				rx = x
				ry = y
				#判断该位置是否能确保打通相邻两块区域，判断依据，上下左右位置最多只有两面墙，下面一样
				if i == 0:
					while (self.maze[rx-1][ry]+self.maze[rx+1][ry]+self.maze[rx][ry-1]+self.maze[rx][ry+1])>2*WALL:
						rx = x1 + random.randint(0, RAND_MAX)%(x - x1)
				elif i == 1:
					while (self.maze[rx-1][ry]+self.maze[rx+1][ry]+self.maze[rx][ry-1]+self.maze[rx][ry+1])>2*WALL:
						ry = y + 1 + random.randint(0, RAND_MAX)%(y2 - y)
				elif i == 2:
					while (self.maze[rx-1][ry]+self.maze[rx+1][ry]+self.maze[rx][ry-1]+self.maze[rx][ry+1])>2*WALL:
						rx = x + 1 + random.randint(0, RAND_MAX)%(x2 - x)
				elif i == 3:
					while (self.maze[rx-1][ry]+self.maze[rx+1][ry]+self.maze[rx][ry-1]+self.maze[rx][ry+1])>2*WALL:
						ry = y1 + random.randint(0, RAND_MAX)%(y - y1)
				self.maze[rx][ry] = PATH

if __name__ == '__main__':
	Maze0 = Maze()
	maze0 = Maze0.CreateMaze(1,1, L-2, L-2)
	Maze0.showMaze(Maze0.maze)
