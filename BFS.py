# -*- coding:utf-8 -*-
#fuction: using DP
#Author: Toryun

graph = {
	"A": ["B","C"],
	"B": ["D","C","F","A","E"],
	"C": ["A","B","G","E","H"],
	"D": ["B"],
	"E": ["B","C","F","G"],
	"F": ["B","E"],
	"G": ["E","C"],
	"H": ["C"]
}

def BFS(graph,s):
	#s is vertex which is stared
	queue = []
	seen = set()
	queue.append(s)
	seen.add(s)
	while (len(queue)>0):
		vertex = queue.pop(0)
		#print queue
		nodes = graph[vertex]
		#print nodes
		for w in nodes:
			if w not in seen:
				queue.append(w)
				seen.add(w)
				#print seen,queue
		print vertex
if __name__ == '__main__':
        s = raw_input("Please choose one key to caculate the shortest path:\n")
        BFS(graph,s)