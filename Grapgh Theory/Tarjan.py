#coding:utf-8
#Date: 2020-05-31 17:30
#Author: Toryun
#Function: finding the strongly connected components of a graph
def strongly_connected_components(graph):
    """
    Tarjan's Algorithm (named for its discoverer, Robert Tarjan) is a graph theory algorithm
    for finding the strongly connected components of a graph.
    
    Based on: http://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
    """

    count = [0]        #编号器(ps: 为什么不用int?因为数组的作用域大于int,可以作用于整个递归函数.当然我们也可以在函数参数上传入一个int类型的初始值)
    stack = []         #用来存储被访问过的强连通节点
    low = {}           #记录节点访问时的编号并在每次遍历子节点时更新它来找到它的父节点,以此来找到整个搜索子树的根节点
    dfn = {}           #depth-first-number:为节点添加首次访问的时间戳,节点一旦被访问打上时间戳,就不在被修改
    result = []        #最终需要返回的结果(所有强连通分量,和单个节点)
    
    def strongconnect(node):
        # 给每个节点node一个深度优先搜索标号index

        dfn[node] = count[0]
        low[node] = count[0]
        count[0] += 1
        stack.append(node)
    
        # 如果该节点为单节点, 没有其它节点相连,则为空
        try:
            E = graph[node]
        except:
            E = []
        #深度遍历node节点(可以称其为强连通分量的根,因为它是第一个被访问的节点)的子节点
        for v in E:
            if v not in dfn:
                # 后继节点v未访问，递归调用strongconnect函数把v加入low并编号入栈
                strongconnect(v)
                low[node] = min(low[node],low[v])

            #如果节点是访问过的
            elif v in stack:
                # 返回编号是最小的节点
                low[node] = min(low[node],dfn[v])
                
        # 若node是根则出栈，并得到一个强连通分量,此时的node在栈底
        if low[node] == dfn[node]:
            connected_component = []
            #把栈内的子节点全部加入result
            while True:
                v = stack.pop()
                connected_component.append(v)
                if v == node:
                    break
            component = tuple(connected_component)
            result.append(component)
    #依次push入栈,遍历图所有节点,防止存在因一次tarjan而没有遍历到的节点
    for node in graph:
        if node not in dfn:
            strongconnect(node)
    return result
if __name__ == '__main__':
    
    '''grid = {
    "A": {"B":5,"C":1},
    "B": {"A":5,"C":2,"D":1},
    "C": {"A":1,"B":2,"D":4,"E":8},
    "D": {"B":1,"C":4,"E":3,"F":6},
    "E": {"C":8,"D":3},
    "F": {"D":6},
    "G": {"F":3, "H":5,"S":19},
    "H": {"G":5,"I":8,"J":4},
    "I": {"H":8,"K":3}
    }'''
    grid = {
    "A": {"B":5},
    "B": {"C":2},
    "C": {"A":1},
    "D": {"B":1,"C":4,"E":3},
    "E": {"F":8,"D":3},
    "F": {"C":6}}
    ret = strongly_connected_components(grid)
    print("The strongly connected componets is \n{}\n".format(ret))

