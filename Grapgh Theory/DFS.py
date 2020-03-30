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

def DFS(graph,s):
	stack = []
	seen = set()
	parent = {s:None}
	stack.append(s)
	seen.add(s)
	while (len(stack)>0):
		vertex = stack.pop()
		#print stack
		nodes = graph[vertex]
		#print nodes
		for w in nodes:
			if w not in seen:
				stack.append(w)
				seen.add(w)
				parent[w] = vertex
				#print seen,stack
		print vertex
	print parent
if __name__ == '__main__':
        DFS(graph,"A")
