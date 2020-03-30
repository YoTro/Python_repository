# -*- coding:utf-8 -*-
#fuction: using DP
#Author: Toryun

graph = {
	"A": ["B","C"],
	"B": ["A","C","D"],
	"C": ["A","B","D","E"],
	"D": ["B","C","E","F"],
	"E": ["C","D"],
	"F": ["D"]
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
	print queue
if __name__ == '__main__':
        BFS(graph,"E")
