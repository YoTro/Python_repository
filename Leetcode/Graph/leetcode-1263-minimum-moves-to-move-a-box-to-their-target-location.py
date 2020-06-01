#coding:utf-8
import time
import heapq
import copy
import collections

class Solution(object):

    def minPushBox(self, grid):
        """
        :type grid: List[List[str]]
        :rtype: int
        「推箱子」是一款风靡全球的益智小游戏，玩家需要将箱子推到仓库中的目标位置。

        游戏地图用大小为 n * m 的网格 grid 表示，其中每个元素可以是墙、地板或者是箱子。

        现在你将作为玩家参与游戏，按规则将箱子 'B' 移动到目标位置 'T' ：

        玩家用字符 'S' 表示，只要他在地板上，就可以在网格中向上、下、左、右四个方向移动。
        地板用字符 '.' 表示，意味着可以自由行走。
        墙用字符 '#' 表示，意味着障碍物，不能通行。
        箱子仅有一个，用字符 'B' 表示。相应地，网格上有一个目标位置 'T'。
        玩家需要站在箱子旁边，然后沿着箱子的方向进行移动，此时箱子会被移动到相邻的地板单元格。记作一次「推动」。
        玩家无法越过箱子。
        返回将箱子推到目标位置的最小 推动 次数，如果无法做到，请返回 -1。

        提示：

        1 <= grid.length <= 20
        1 <= grid[i].length <= 20
        grid 仅包含字符 '.', '#', 'S' , 'T', 以及 'B'。
        grid 中 'S', 'B' 和 'T' 各只能出现一个。
        """
        def get_pos(c):
            '''获取目标坐标'''
            for i in range(len(grid)):
                for j in range(len(grid[i])):
                    if grid[i][j] == c:
                        return i,j
        n = len(grid)
        m = len(grid[0])
        rs = get_pos('S')#玩家
        xs = get_pos('B')#箱子
        ts = get_pos('T')#目标位置
        s  = (rs, xs, 0)#定义节点:玩家位置,箱子位置,步长
        mx = [-1,0,0,1]#(mx[i],my[i])表示向西(左), 南(下), 北(上), 东(右)运动一步
        my = [0,-1,1,0]
        q = [s]#利用队列来进行广度优先遍历
        def judeg(x,y):
            '''判断位置是否越界或者碰墙'''
            if x < 0 or x >= n or y < 0 or y >= m:
                return False
            if grid[x][y] == '#':#墙
                return False
            return True
        #记忆存储走过了的点
        vis = [ [-1]*2500 for _ in range(2500)]
        def j_vis(rs, bs, step):
            '''存储玩家和箱子的位置、步长'''
            #有时候(4,4) 和(5,3)的和都等于8,会有冲突, 所以从x100可以抵消冲突
            s1 = 100*rs[0] + rs[1] 
            s2 = 100*bs[0] + bs[1]
            #如果这条路径没有遍历过,则更新步长
            if vis[s1][s2] == -1:
                vis[s1][s2] = step
                return True
            #判断走过路径的步长如果大于已知最小步长,则返回false不添加进队列
            if step >= vis[s1][s2]:
                return False
            #如果此路径走过且步长小于已知最小步长,则更新数组中的步长
            vis[s1][s2] = step
            return True
        #初始化
        ret = -1
        new_grid = grid[:]
        while len(q)>0:
            #广度优先遍历
            rs, xs, step = q.pop(0)
            #如果箱子到达目标位置, 返回最小值,否则-1
            if xs[0] == ts[0] and xs[1] == ts[1]:
                if ret == -1:
                    ret = step
                ret = min(ret,step)
                new_grid[xs[0]][xs[1]] = ret
            #玩家上下左右广度优先遍历
            for i in range(4):
                tx = rs[0] + mx[i]
                ty = rs[1] + my[i]
                #如果下一个玩家新位置越界或碰墙就跳过
                if not judeg(tx,ty):
                    continue
                #如果玩家到达箱子位置,且加(mx[i], my[i])可以保证沿着箱子的方向进行push
                if tx == xs[0] and ty == xs[1]:
                    xtx = xs[0] + mx[i]
                    xty = xs[1] + my[i]
                    #如果越界或碰墙就跳过
                    if not judeg(xtx,xty):
                        continue
                    #下一个箱子位置为新的箱子位置
                    new_xs = (xtx,xty)
                    #计入一次
                    f = 1
                else:
                    #如果玩家未到达箱子,则玩家继续寻找下一个位置,而箱子原位置不动
                    new_xs = (xs[0], xs[1])
                    #不计数
                    f = 0
                #玩家新位置
                new_rs = (tx,ty)
                #查看新点是否遍历过,更新步长
                if j_vis(new_rs, new_xs, step+f):
                    #添加新节点
                    q.append((new_rs,new_xs,step+f))
                    new_grid[new_rs[0]][new_rs[1]], new_grid[rs[0]][rs[1]] = "S", "."
            print q
            time.sleep(1)
            print("此时")
            for i in range(n):
                print("{}\n".format(new_grid[i]))
        return ret
    def minPushBox_Astar(self, grid):
        n, m = len(grid), len(grid[0])     #迷宫长,宽
        g = collections.defaultdict(list)  #存储每个重要物品的坐标
        for i in range(n):
            for j in range(m):
                g[grid[i][j]] += [complex(i,j)]
        sp = g["S"][0]  #玩家位置
        tp = g["T"][0]  #目标位置
        bp = g["B"][0]  #箱子位置
        path = g["."]   #路位置list
        directions = (1, -1, 1j, -1j)#上下左右
        visited = set() #存储已访问过的节点
        step = 1        #箱子的移动步数
        Path_set = path + [sp] + [tp] + [bp]   #所有路坐标的list
        #A*估值函数
        def F(a, s):
            '''
            a: (箱子)当前位置坐标
            s: (箱子)已走步数
            返回一个元组,其中一个是曼哈顿距离, 一个是欧式距离
            '''
            euclidean_dist = abs(a - tp)
            manhattan_dist = abs((a - tp).real) + abs((a - tp).imag) + s 
            return (manhattan_dist, euclidean_dist)
        #对人的移动使用
        def bestfirst(from_position, to_position, path_set):
            '''
            最好优先算法也叫做A算法,和A*相似
            from_position: 当前位置
            to_position:   最后位置
            path_set:      判断所在位置是否在路位置集合上
            返回值rtype:    bool
            '''
            p = 0                                #防止重复计算小根堆里的节点,类似于唯一键值prime_key
            f = abs(from_position - to_position) #估价函数为当前点和目标点的距离
            heapplayer = [(f, p, from_position)]
            #遍历人的优先队列
            while heapplayer:
                #取出堆中估值函数最小, 即路径绝对值最短节点, 其中, f和p参数没有用处
                f, _, curr_position = heapq.heappop(heapplayer)
                #如果到达最后位置,返回True
                if curr_position == to_position:
                    return True
                #遍历现在位置的上下左右四个方位
                for direction in directions:
                    next_position = curr_position + direction
                    #如果新的位置没有越界和碰墙,则添加进入人的优先队列(小根堆)
                    if next_position in path_set:
                        #print(abs(next_position - to_position), p, next_position)
                        heapq.heappush(heapplayer, (abs(next_position - to_position), p, next_position))
                        p += 1
                        path_set.remove(next_position) #把进入堆的坐标都去点
            return False #如果不能到达指定位置,则返回false
        time = 1        #作用于防止节点进堆后存在节点前几个参数相同,发生比较复数情况的发生        
        node = (F(bp, step), step, time, sp, bp)
        heapbox = [node]                #箱子移动小根堆
        #遍历箱子移动堆
        while heapbox:
            #取出节点, 其中, 估值函数, step在程序中没有使用到
            f, step, _, sp, bp = heapq.heappop(heapbox)
            #向四个方向开始遍历
            for direction in directions:
                #玩家下一个位置需要处于箱子旁边
                next_position = bp - direction
                #箱子下一个位置是沿着箱子方向移动一步之后的位置         
                nextbox_position = bp + direction
                #箱子必须在路上
                if nextbox_position in Path_set:
                    #人和箱子的位置没有重复访问
                    if (next_position, bp) not in visited:
                        #必须去除箱子的位置,因为人不能触碰或跨过箱子
                        if bp in Path_set:
                            copypathset = copy.deepcopy(Path_set)
                            copypathset.remove(bp)
                        #人去找箱子,并到达箱子旁
                        if bestfirst(sp, next_position, copypathset):
                            #箱子到达终点,返回步数
                            if nextbox_position == tp:
                                return step
                            heapq.heappush(heapbox, (F(nextbox_position, step + 1), step + 1, time, bp, nextbox_position))
                            time += 1
                            #添加遍历过的节点
                            visited.add((next_position, bp))
        return -1

    def minPushBox_tarjan(self, graph):
        n, m = len(graph), len(graph[0])  #图的尺寸
        p = collections.defaultdict(list) #默认字典
        sp = "S"                          #玩家
        bp = "B"                          #箱子
        tp = "T"                          #目标位置
        path = "."                        #路
        uid = [0]                         #防止重复节点冲突,增加uid
        step = 1                          #箱子移动步数
        directions = (1, -1, 1j, -1j)     #方向
        visited = set()                   #是否被访问过节点集合
        for i in range(n):                #遍历查找相关节点位置
            for j in range(m):
                p[graph[i][j]] += [complex(i,j)] 
        Sp, Bp, Tp, Path = p[sp][0], p[bp][0], p[tp][0], p[path]
        Path_set = [Sp] + [Bp] + [Tp] + Path
        def F(bp, step):
            '''估值函数曼哈顿距离和欧式距离'''
            #作用于嵌套函数的局部变量.作用域为使用本函数F(bp, step)之后的整个minPushBox_tarjan函数
            uid[0] += 1
            manhattan_dist = abs(Bp - Tp).real + abs(Bp - Tp).imag + step
            euclidean_dist = abs(Bp - Tp)
            return (manhattan_dist, euclidean_dist, uid[0])
        node = (F(Bp, 1), step, Sp, Bp)
        heapbox = [node]                  #箱子路径堆
        count = [0]                       #tarjan算法中的时间戳
        low = dict.fromkeys(Path_set, 0)  #帮助算法深度遍历时回溯找到根节点的参数,默认为0
        dfn = low.copy()                  #首次添加的时间戳, 一经添加无法修改
        index = {}                        #建立坐标与时间戳的映射，方便计算各个坐标所归属的强连通分量
        def tarjan(curr_node, parent_node):
            '''标准无向图基于深度优先搜索的tarjan算法，参数curr_node为当前拓展点，记录拓展的父节点parent_node防止重复拓展'''
            dfn[curr_node] = count[0]
            low[curr_node] = count[0]
            index[count[0]] = curr_node
            count[0] += 1
            #遍历四个方向
            for direction in directions:
                neighbor_node = curr_node + direction
                #如果邻居节点在路径集合上并且不是父节点
                if neighbor_node in Path_set:
                    #如果没有被访问过
                    if not low[neighbor_node]:
                        tarjan(neighbor_node, curr_node)
                    #如果访问过的节点时间戳比当前节点的时间戳要小，则表示属于同一个强连通分量，取较小那个
                    low[curr_node] = min(low[neighbor_node], low[curr_node])
        #从现在箱子位置出发遍历,父节点为-1
        tarjan(Bp, -1)
        #遍历所有路径点,查找所有强连通分量
        for curr_node in Path_set:
            connect_node = [curr_node]
            #查找所有属于同一个强连通分量的点
            while (low[connect_node[-1]] != dfn[connect_node[-1]]):
                connect_node.append(index[low[connect_node[-1]]])
            #时所有归属于一个强连通分量的时间戳相等于根
            for v in connect_node[:-2]:
                    low[v] = low[connect_node[-1]]
        #遍历整个堆
        while heapbox:
            f, step, Sp, Bp = heapq.heappop(heapbox)
            for direction in directions:
                #箱子沿着某个方向移动一步
                nextbox_position = Bp + direction
                #表示玩家到达现在箱子位置坐标的旁边一个坐标
                next_sp = Bp - direction
                #如果箱子下个位置在路径上,表示玩家能够推动它
                if nextbox_position in Path_set:
                    #如果玩家能够到达箱子推动方向的反方向一步距离
                    if next_sp in Path_set:
                        #如果箱子和人的位置都没有访问过(防止循环遍历)
                        if (next_sp, Bp) not in visited:
                            #如果时间戳相等,表示它们是相互连通的
                            if low[next_sp] == low[Sp]:
                                #如果到达目的地
                                if nextbox_position == Tp:
                                    return step
                                #玩家位置为下一个位置为Bp表示玩家始终处于箱子的旁边一个位置并沿箱子方向推动箱子移动了一个步数
                                heapq.heappush(heapbox, (F(nextbox_position, step+1), step+1, Bp, nextbox_position))
                                #记录访问过的节点
                                visited.add((next_sp, Bp))
        return -1







if __name__ == '__main__':
    solution = Solution()
    grid = [["#","#",".","#",".",".",".","."],[".",".",".",".",".",".",".","."],[".",".",".",".",".","T",".","#"],[".",".",".",".",".","#",".","."],[".",".",".",".",".","#",".","."],[".",".",".",".",".",".","S","."],[".",".",".","B",".",".",".","."],[".",".",".",".",".",".",".","."]]
    #ret = solution.minPushBox(grid)
    ret1 = solution.minPushBox_tarjan(grid)
    print(ret1)
