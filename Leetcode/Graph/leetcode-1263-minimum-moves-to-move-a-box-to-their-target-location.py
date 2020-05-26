#coding:utf-8
import time
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

if __name__ == '__main__':
    solution = Solution()
    grid = [["#","#","#","#","#","#"],
            ["#","T","#","#","#","#"],
            ["#","B",".",".","#","#"],
            ["#",".","#",".","#","#"],
            ["#",".","#",".","S","#"],
            ["#",".",".",".","#","#"],
            ["#","#","#","#","#","#"]]
    ret = solution.minPushBox(grid)
    print(ret)
