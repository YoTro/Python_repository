#coding:UTF-8
#Author: Toryun
#Date: 2020-04-22 17:57:00
#Function: Create/delect Binary tree and search tree
import random
import gc#手动回收内存
#from memory_profiler import profile#内存检测

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
                
                       
    def delete(self,x):
        if self.root is None:
            return 0
        else:
            queue = []
            q = []
            tag = 0
            queue.append(self.root)
            while len(queue)>0:
                node = queue.pop(0)
                if node.left:
                    queue.append(node.left)
                if node.right:
                    queue.append(node.right)
                q.append(node)
            while len(q)>0:
                node = q.pop(0)
                if node.value == x:
                    tag = 1
                    if (node.left==node.right==None):
                        print ("删除的节点值为{}".format(node.value))
                        node.value = None
                        return
                    elif not node.left or not node.right:
                        if not node.left:
                            print ("删除的节点值为{},补缺的节点为{}".format(node.value,node.right.value))
                            node.value = node.right.value
                            node.right = None
                            return
                        if not node.right:
                            print ("删除的节点值为{},补缺的节点为{}".format(node.value,node.left.value))
                            node.value = node.left.value
                            node.left = None
                            return
                    else:
                        tmp = q.pop()
                        while not tmp.value:
                            tmp = q.pop()
                        print ("删除的节点值为{},补缺的节点为{}".format(node.value,tmp.value))
                        node.value = tmp.value
                        tmp.value = None
                        break
                        
                if tag != 1 and len(q)==0:
                    #print tag
                    print("\nWe can't find {} in this tree".format(x))
                
                    
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
    def hasPathSum(self, root, sum):
        """
        :type root: TreeNode
        :type sum: int
        :rtype: bool
        给定一个二叉树和一个目标和，判断该树中是否存在根节点到叶子节点的路径，这条路径上所有节点值相加等于目标和。
        说明: 叶子节点是指没有子节点的节点。
        """
        if root is not None:
            sum -= root.value
            #print("\n{} {}").format(sum,root.value)
            if not root.left and not root.right:
                return sum == 0
            return self.hasPathSum(root.left,sum) or self.hasPathSum(root.right,sum)
        else:
            return False
    def getMaxdepth(self):
        if self.root is None:
            return 0
        else:
            Max_d = []
            stack = []
            stack.append(self.root)
            while len(stack) > 0:
                node = stack.pop()
                if node.left:
                    stack.append(node.left)
                if node.right:
                    stack.append(node.right)
                Max_d.append(node.height)
        return max(Max_d)
    def minDepth(self, root):
        """
        :type root: TreeNode
        :rtype: int
        """
        if not root:
            return 0

        children = [root.left, root.right]
        # if we're at leaf node
        if not any(children):
            return 1

        min_depth = float('inf')
        for c in children:
            if c:
                min_depth = min(self.minDepth(c), min_depth)
        return min_depth + 1
    def DFS(self):
        '''deep fisrt search'''
        print("深度优先遍历:\n")
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
        print("\n广度优先遍历:\n")
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

    def levelOrder(self, root):
        """
        :type root: TreeNode
        :rtype: List[List[int]]
        """
        if not root:
            return []
        else:
            q = []
            q.append(root)
            j = []
            while len(q)>0:
                p = []
                d = []
                for c in q:
                    if c:
                        p.append(c.value)
                        #print c.value
                        if c.left:
                            d.append(c.left)
                        if c.right:
                            d.append(c.right)
                        #print len(d)
                q = d
                #print len(q)
                if p:
                    j.append(p)
            print("\n层次遍历(按层输出):\n")
            print j
            return j
        
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
    def LNR2(self, root):
        '''利用栈中序遍历'''
        stack,rst = [root],[]
        while stack:
            i = stack.pop()
            if isinstance(i,treenode):
                stack.extend([i.right,i.val,i.left])
                print stack
            elif isinstance(i,int):
                rst.append(i)
        return rst
    
    def LRN(self,root):
        '''Post-order'''
        if root:
            self.LRN(root.left)
            self.LRN(root.right)
            print root.value,

if __name__ =='__main__':
    btree = BinaryTree(None)
    l = [x for x in range(random.randrange(1,20))]
    for i in range(len(l)):
        btree.add(l[i])
    btree.DFS()
    btree.BFS()
    btree.levelOrder(btree.root)
    #btree.hasPathSum(btree.root,3)
    btree.delete(random.choice(l))
    Maxdepth = btree.getMaxdepth()
    print("\n树的最大深度为{}\n".format( Maxdepth))
    mindepth = btree.minDepth(btree.root)
    print("\n树的最小深度为{}\n".format( mindepth))
    print("\n前序遍历:\n")
    btree.NLR(btree.root)
    print("\n中序遍历:\n")
    btree.LNR(btree.root)
    print("\n后序遍历:\n")
    btree.LRN(btree.root)
    
            
