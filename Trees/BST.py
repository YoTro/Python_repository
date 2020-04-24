#coding:UTF-8
#Author: Toryun
#Date: 2020-04-22 17:57:00
#Function: Create/delect Binary tree and search tree
import random

class treenode():
    '''定义结构体:子树'''
    def __init__(self,x):
        self.value = x
        self.left = None
        self.right = None
        self.parent = None
        self.height = 0

class BinaryTree():
    def __init__(self,root):
        '''初始化一个根节点'''
        self.root = None
    def add(self,x):
        '''添加左(右)节点'''
        
        if self.root is None:
            self.root = treenode(x)
        else:
            '''广度优先遍历添加节点'''
            queue = []
            queue.append(self.root)
            while len(queue) > 0:
                node = queue.pop(0)
                if not node.left:#表示左节点为空节点
                    node.left = treenode(x)
                    node.left.parent = node
                    node.left.height = 1 + node.height
                    return
                else:
                    queue.append(node.left)
                    #print " left {}".format(str(node.left.value))
                if not node.right:
                    node.right = treenode(x)
                    node.right.parent = node
                    node.right.height = 1 + node.height
                    return
                else:
                    queue.append(node.right)
                    #print " right {}".format(str(node.right.value))
                
                        
    def delect(self,x):
        if self.root is None:
            return 0
        else:
            queue = []
            queue.append(self.root)
            while len(queue)>0:
                node = queue.pop(0)
                if node.value == x:
                    if node.right:
                        node = node.right
                    else:
                        if node.left.right:
                            node = node.left.right
                            if node.left.right.left:
                                node.left.right = node.left.right.left
                        if node.left:
                            node = node.left
    def insert(self,x):
        '''以左节点小于右节点方式做插入操作(BST)'''
        if self.root is None:
            self.root = treenode(x)
            return
        else:
            stack = []
            stack.append(self.root)
            while len(stack) > 0:
                node = stack.pop()
                if node.right and node.value <= x:
                    stack.append(node.right)
                elif node.left and node.value > x:
                    stack.append(node.left)
                elif not node.right and node.value <= x:
                    node.right = treenode(x)
                    node.right.height = 1 + node.height
                elif not node.left and node.value > x:
                    node.left = treenode(x)
                    node.left.height = 1 + node.height
    def DFS(self):
        '''deep fisrt search'''
        if self.root:
            stack = []
            stack.append(self.root)
            p = []
            while len(stack)>0:
                node = stack.pop()
                p.append(node.value)
                print node.value,
                if node.right:
                    stack.append(node.right)                
                if node.left:
                    stack.append(node.left)
                #print p
        else:
            return
    def BFS(self):
        '''Breadth-first-search/'''
        if self.root is None:
            return
        queue = []
        p = []
        queue.append(self.root)
        while len(queue) > 0:
            
            node = queue.pop(0)
            p.append(node.value)
            print node.value,
            if node.left:
                queue.append(node.left)
                #print "left{}".format(node.left.value),
            if node.right:
                queue.append(node.right)
                #print "right{}".format(node.right.value),
    def NLR(self,root):
        '''Pre-order
        (L)	Recursively traverse N's left subtree.
        (R)	Recursively traverse N's right subtree.
        (N)	Process the current node N itself.

        '''
        if root:
            print root.value,
            self.NLR(root.left)
            self.NLR(root.right)
    def LNR(self,root):
        '''In-order'''
        if root:
            self.LNR(root.left)
            print root.value,
            self.LNR(root.right)
    def LRN(self,root):
        '''Post-order'''
        if root:
            self.LRN(root.left)
            self.LRN(root.right)
            print root.value,

if __name__ =='__main__':
    btree = BinaryTree(None)
    l = [x for x in range(random.randrange(1,10))]
    l = [7 ,9 ,10,1 ,4 ,5 ,13 ]
    for i in range(len(l)):
        btree.insert(l[i])
    print("深度优先遍历:\n")
    btree.DFS()
    print("\n广度优先遍历:\n")
    btree.BFS()
    print("\n前序遍历:\n")
    btree.NLR(btree.root)
    print("\n中序遍历:\n")
    btree.LNR(btree.root)
    print("\n后序遍历:\n")
    btree.LRN(btree.root)
    
            
