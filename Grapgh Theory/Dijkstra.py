#-*-coding:UTF-8-*-
#Python version:2.7.13
#System:window7
#Time:2019-11-11
#Function:The Dijkstra algorithm for Shortest Path Problem in Grapgh
def dij(start, graph):
    n = len(graph)
    # 初始化各項資料，把costs[start]初始化為0，其他為無窮大
    # 把各個頂點的父結點設定成-1
    costs = [99999 for _ in range(n)]
    print costs
    costs[start] = 0
    parents = [-1 for _ in range(n)]
    visited = [False for _ in range(n)] # 標記已確定好最短花銷的點
    t = []  # 已經確定好最短花銷的點列表
    while len(t) < n:
        # 從costs裡面找最短花銷(找還沒確定的點的路徑)，標記這個最短邊的頂點，把頂點加入t中
        minCost = 99999
        minNode = None
        for i in range(n):
            if not visited[i] and costs[i] < minCost:
                minCost = costs[i]
                minNode = i
                print minNode,minCost
        t.append(minNode)
        
        visited[minNode] = True
        # 從這個頂點出發，遍歷與它相鄰的頂點的邊，計算最短路徑，更新costs和parents
        for edge in graph[minNode]:
            print "{0}+{1}<{2}".format(minCost,edge[1],costs[edge[0]])
            if not visited[edge[0]] and minCost + edge[1] < costs[edge[0]]:
                costs[edge[0]] = minCost + edge[1]
                parents[edge[0]] = minNode
                print t,visited,costs,parents
    return costs, parents


# 主程式

# Data
data = [
    [1, 0, 8],
    [1, 2, 5],
    [1, 3, 10],
    [1, 6, 9],
    [2, 0, 1],
    [0, 6, 2],
    [3, 6, 5],
    [3, 4, 8],
    [0, 5, 4],
    [5, 6, 7],
    [5, 3, 8],
    [5, 4, 5]
]
n = 7  # 結點數

# 用data資料構建鄰接表
graph = [[] for _ in range(n)]
for edge in data:
    graph[edge[0]].append([edge[1], edge[2]])
    graph[edge[1]].append([edge[0], edge[2]])


# 從1開始找各點到1的最短路徑（單源最短路徑）
# costs: 各點到店1的最短路徑
# parents: 各點連結的父結點，可以用parents建立最短路徑生成樹
costs, parents = dij(2, graph)
print('costs')
print(costs)
print('parents')
print(parents)
