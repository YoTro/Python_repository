# -*- coding:utf-8 -*-
#fuction: using DP
#Author: Toryun
import heapq
import math
graph = {
	"A": {"B":5,"C":1},
	"B": {"A":5,"C":2,"D":1},
	"C": {"A":1,"B":2,"D":4,"E":8},
	"D": {"B":1,"C":4,"E":3,"F":6},
	"E": {"C":8,"D":3},
	"F": {"D":6}
}
def __init__distance(graph,s):
	#初始化距离字典
	distance = {s:0}
	for vertex in graph:
		if vertex != s:
			distance[vertex] = float('inf')
	return distance

def get_key (dict, value):
        for k, v in dict.items():
                if v == value:
                        return k 

def dijkstra(graph,distance,s):
	pqueue = []
	seen = set()
	parent = {s:None}
	heapq.heappush(pqueue,(0,s))
	while (len(pqueue)>0):
		pair = heapq.heappop(pqueue)
		#print pqueue
		dist = pair[0]
		vertex = pair[1]
		seen.add(vertex)
		nodes = graph[vertex].keys()
		#print nodes
		for w in nodes:
			if w not in seen:
				if dist + graph[vertex][w] < distance[w]:
					heapq.heappush(pqueue,(dist + graph[vertex][w],w))
					parent[w] = vertex
					distance[w] = dist + graph[vertex][w]

	return parent,distance
if __name__ == '__main__':
        print graph.keys()
        s = raw_input("Please choose one key to caculate the shortest path:\n")
        distance = __init__distance(graph,s)
        parent,distance = dijkstra(graph,distance,s)
        print parent
        print distance
        distance = sorted(distance.items(), key=lambda x: x[1])
        d = []
        for i in distance:
               d.append(i[0]) 
        lon = "-->"
        print "The shortest path of this graph is \n-----------------------\n"+lon.join(d)+"\n-----------------------"
