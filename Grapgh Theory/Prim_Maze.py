#coding: utf-8
#Author: Toryun
#Date: 2020-05-27 20:23:00
#Function: Generate random maze by Prim
import numpy as np 
import random

L = random.randint(8, 21)#定义迷宫初始长度
PATH = 1                 #路
WALL = 0                 #墙
RAND_MAX = 32767         #随机范围

class Maze():
	def __init__(self):
		self.maze = np.zeros((L,L))#初始化迷宫
		#外围都是路
		for i in range(L):
			self.maze[i][L-1] = PATH
			self.maze[L-1][i] = PATH
			self.maze[0][i] = PATH
			self.maze[i][0] = PATH

	def showMaze(self, maze):
		'''打印迷宫'''
		for i in range(len(maze)):
			for j in range(len(maze[0])):
				if maze[i][j] == WALL:
					print("#"),
				else:
					print(" "),
			print("\n")

	def CreateMaze(self):
		#随机添加(2,2)进入队列X,Y
		X = [2]
		Y = [2]
		#当队列为空时,结束循环
		while len(X) > 0:
			#随机在墙队列中取一个节点
			r = random.randint(0, RAND_MAX)%len(X)
			x = X[r]
			y = Y[r]
			#判断上下左右四个方向是否为路
			count = 0
			for i in range(x-1, x+2):
				for j in range(y-1, y+2):
					#abs(x-i) + abs(y-j) == 1表示上下左右, 大于零表示如果是路,就进1
					if abs(x-i) + abs(y-j) == 1 and self.maze[i][j] > 0:
						count += 1
			#如果上下左右位置只有一个位置是路,则开挖(x,y)
			if count <= 1:
				self.maze[x][y] = 1
				#把剩余的墙坐标加入队列
				for i in range(x-1, x+2):
					for j in range(y-1, y+2):
						if abs(x-i) + abs(y-j) == 1 and self.maze[i][j] == 0:
							X.append(i)
							Y.append(j)

			#删除当前队列中已经是路的节点坐标
			X.pop(r)
			Y.pop(r)
		#设置入口和出口
		#self.maze[2][1] = 1
		#for i in range(L-3, -1, -1):
			#if self.maze[i][L-3] == 1:
				#self.maze[i][L-2] = 1
				#break

if __name__ == '__main__':
	Maze0 = Maze()
	maze0 = Maze0.CreateMaze()
	Maze0.showMaze(Maze0.maze)	
