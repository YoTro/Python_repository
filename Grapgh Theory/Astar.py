#coding:utf-8
import Prim_Maze
import random
import time
import heapq
import copy
import collections
def add_target(maze):
	n = len(maze)
	m = len(maze[0])
	sp = 2  #玩家
	tp = 3  #目标
	spp = (2,2)
	tpp = (random.randint(3, n-3), random.randint(3, n-3))
	maze[spp[0]][spp[1]] = sp
	maze[tpp[0]][tpp[1]] = tp
	return maze

def showAMaze(maze):
	'''打印迷宫'''
	sp = 2    #玩家
	tp = 3.   #目标
	PATH = 1  #路
	WALL = 0  #墙
	for i in range(len(maze)):
		for j in range(len(maze[0])):
			if maze[i][j] == WALL:
				print("#"),
			elif maze[i][j] == PATH:
				print(" "),
			elif maze[i][j] == sp:
				print("S"),
			else:
				print("T"),
		print("\n")

def Astar(maze):
	'''
	1.         把open list堆化, 起点加入 open list 。

	2.         重复如下过程：

	a.         遍历 open list ，查找 F 值最小的节点，把它作为当前要处理的节点。

	b.         把这个节点移到 close list 。

	c.         对当前方格的 8 个相邻方格的每一个方格

	◆     如果它是不可抵达的或者它在 close list 中，忽略它。否则，做如下操作。

	◆     如果它不在 open list 中，把它加入 open list ，并且把当前方格设置为它的父亲，计算该方格的 F ， G 和 H 值。

	◆     如果它已经在 open list 中，检查这条路径 ( 即经由当前方格到达它那里 ) 是否更好，用 G 值作参考。更小的 G 值表示这是更好的路径。如果是这样，把它的父亲设置为当前方格，并重新计算它的 G 和 F 值, 然后入堆。如果你的 open list 是按 F 值排序的话，入堆后会重新排序。

	d.         停止条件，

	◆     把终点加入到了 open list 中，此时路径已经找到

	◆     或者查找终点失败，并且 open list 是空的，此时没有路径。

	3.         保存路径。从终点开始，每个方格沿着父节点移动直至起点，这就是你的路径。

	'''
	n = len(maze)
	m = len(maze[0])
	sp = 2    #玩家
	tp = 3.   #目标
	PATH = 1  #路
	WALL = 0  #墙
	cost = {} #起点到现在位置的总花费集合
	ret = [] #返回路径

	#1. 获取目标点的坐标和玩家起始点的坐标
	#a. 先对图进行分类加入字典
	classfiy = collections.defaultdict(list)
	for i in range(n):
		for j in range(m):
			classfiy[maze[i][j]].append((i,j))
	player_position, tp_position, PATH_set = classfiy[sp][0], classfiy[tp][0], classfiy[PATH]
	#2. 计算F = G + H
	#a. H = 曼哈顿距离 Manhattan_distance
	#b. G = 移动一步的距离(10或者14) 10:上下左右移动, 14:斜方向移动
	#c. F 估值函数

	def H(player_position):
		return abs(player_position[0] - tp_position[0]) + abs(player_position[1] - tp_position[1])

	def G(player_position, neighbor_p):
		px, py = player_position
		nx, ny = neighbor_p
		#只要有一个差值为0, 0跟任何值进行与操作都等于0即表示假,返回10
		return 14 if ny - py and nx - px else 10

	def F(player_position):
		'''估值函数'''
		dx, dy = player_position  #玩家位置
		tx, ty = tp_position #目标位置
		h = abs(dx - tx) + abs(dy - ty)
		sq = min(abs(ty - dy), abs(tx - dx)) #斜方向移动sq个单位
		mv = abs(abs(ty - dy) - abs(tx - dx)) #平移mv个单位
		return  sq * 14 + mv * 10

	def judge(x, y):
		'''判断是否越界或者撞墙'''
		if x < 0 or x >= n or y < 0 or y >= m:
			return False
		if maze[x][y] == WALL:
			return False
		return True	

	def neighbors(player_position):
		'''获取邻居'''
		x, y = player_position
		n = []
		for j in range(x - 1, x + 2):
			for k in range(y -1, y + 2):
				if j == x and k == y:
					continue
				n.append((j,k))
		return n

	def index_node(p, openlist):
		'''从集合中移除节点'''
		for i in range(len(openlist)):
			if p in openlist[i]:
				return i
		return -1

	f = F(player_position)
	h = H(player_position)
	cost[player_position] = 0
	parent_position = None
	node = (f, 0, h, player_position, parent_position)
	open_list = [node]
	closed_list = []
	PARENT = {}
	new_maze = copy.deepcopy(maze)
	while len(open_list)>0:
		#永远把最小值推出堆
		f, g, h, player_position, parent_position = heapq.heappop(open_list)
		#把访问过的节点放入closedlist
		heapq.heappush(closed_list, player_position)
		if player_position == tp_position:
			ret.append(player_position)
			break
		for neighbor in neighbors(player_position):
			#如果越界或者撞墙, 跳过
			if not judge(neighbor[0], neighbor[1]) or neighbor in closed_list:
				continue
			neighbor_g = g + G(player_position, neighbor)
			#如果不在openlist中
			if index_node(neighbor, open_list) == -1:
				#如果也不在closedlist中
				if neighbor not in closed_list:
					
					cost[neighbor] = neighbor_g
					neighbor_h = H(neighbor)
					neighbor_f = F(neighbor)
					neighbor_node = (neighbor_f, neighbor_g, neighbor_h, neighbor, player_position)
					heapq.heappush(open_list, neighbor_node)
					PARENT[player_position] = PATH
			else:
				#如果在openlist中,新的G值比原来的小,更新这个节点的父节点和f,g值
				if neighbor_g < cost[neighbor]:
					neighbornode = open_list.pop(index_node(neighbor, open_list))
					newneighbornode = (neighbor_g + H(neighbor, tp_position), neighbor_g, neighbor, player_position)
					PARENT[player_position] = PATH
					cost[neighbor] = neighbor_g
					heapq.heappush(open_list, newneighbornode)
		if player_position in PARENT:
			new_maze[player_position[0]][player_position[1]] = 2
		heapq.heappush(closed_list, player_position)
		time.sleep(1)
		showAMaze(new_maze)
	ret = PARENT.keys()
	ret.sort()
	return ret if len(ret) >1 else -1

if __name__ == '__main__':
	maze0 = Prim_Maze.Maze()
	maze0.CreateMaze()
	maze0.showMaze(maze0.maze)
	maze1 = add_target(maze0.maze)
	showAMaze(maze1)
	ret = Astar(maze1)
	print(ret)

		
		
		


