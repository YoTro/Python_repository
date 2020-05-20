#coding:utf-8
#Author: Toryun
#Date: 2020-05-18 16:56:00

class TreeNode():
    def __init__(self, x):
        self.val = x
        self.left = None
        self.right = None
def generateTrees(n):
    '''生成1~n为节点的所有二叉搜索树, 有卡特兰数种的二叉搜索树'''
    def generate_trees(start, end):
        if start > end:
            return [None]
        all_trees = []
        for i in range(start, end+1):
            ltree = generate_trees(start, i-1)
            rtree = generate_trees(i+1, end)
            for l in ltree:
                for r in rtree:
                    c = TreeNode(i)
                    c.left = l
                    c.right = r
                    all_trees.append(c)
        return all_trees
    return generate_trees(1, n) if n else []
def preorder(head):
    if head:
        print head.val
        preorder(head.left)
        preorder(head.right)
    return head
