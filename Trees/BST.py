#coding:UTF-8
#Author: Toryun
#Date: 2020-04-22 17:57:00
#Function: Create Binary tree and search tree
import random

class treenode():
    '''定义结构体:子树'''
    def __init__(self,x):
        self.value = x
        self.left = None
        self.right = None

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
                    return
                else:
                    queue.append(node.left)
                    #print " left {}".format(str(node.left.value))
                if not node.right:
                    node.right = treenode(x)
                    return
                else:
                    queue.append(node.right)
                    #print " right {}".format(str(node.right.value))
    def DFS(self):
        '''deep fisrt search'''
        if self.root:
            stack = []
            stack.append(self.root)
            #p = []
            while len(stack)>0:
                node = stack.pop()
                #p.append(node.value)
                print node.value,
                if node.right:
                    stack.append(node.right)                
                if node.left:
                    stack.append(node.left)
                #print p
        else:
            return
    def BFS(self):
        '''Breadth-first-search'''
        if self.root is None:
            return
        queue = []
        
        queue.append(self.root)
        while len(queue) > 0:
            p = []
            node = queue.pop(0)
            
            print node.value,
            if node.left:
                queue.append(node.left)
                #print "left{}".format(node.left.value),
            if node.right:
                queue.append(node.right)
                #print "right{}".format(node.right.value),
            for i in queue:
                p.append(i.value)
            print p
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
        btree.add(l[i])
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
    
            
