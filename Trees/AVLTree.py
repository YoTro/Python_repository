#coding:UTF-8
#Author: Toryun
#Date: 2020-04-23 22:34:00
#Function: AVL Tree

import random

off = 0 #控制开关:控制平衡二叉树插入节点时动态显示步骤
class AvlTreeNode():
    def __init__(self,x,h):
        '''定义节点'''
        self.value = x
        self.left = None
        self.right = None
        self.height = h#默认为1
class AVLTree():
    def __init__(self):
        self.root = None
    def add(self,x):
        '''添加子节点并安装左小右大的方式不带平衡调整'''
        if self.root is None:
            self.root = AvlTreeNode(x,1)
        else:
            node = self.root
            while node:
                node.height = max(self.get_height(node.left), self.get_height(node.right)) + 1
                if x < node.value:
                    if not node.left:
                        node.left = AvlTreeNode(x,1)
                        return node
                    else:
                        node = node.left
                if x >= node.value:
                    if not node.right:
                        node.right = AvlTreeNode(x,1)
                        return node
                    else:
                        node = node.right

                    
    def __insert(self, root, x):
        '''带平衡调整插入root 永远是根节点'''
        global off
        if not root:
            root = AvlTreeNode(x,1)
        if root.value == x:
            return root
        elif x < root.value:
            #print root.value
            root.left = self.__insert(root.left, x)
            
        else:
            #print root.value
            root.right = self.__insert(root.right, x)
        root.height = max(self.get_height(root.left), self.get_height(root.right)) + 1
        balance = abs(self.get_balance(root))
        if balance < 2:
            if off > 0:
                print("root.value:{}\n".format(root.value))
                self.levelorder(root)
        else:
            if off > 0:
                print("不平衡时的root.value:{}\n".format(root.value))
                self.levelorder(root)
            root = self.balance_Tree(root,x)
        return root
    def insert(self,x):
        '''插入节点保持平衡'''
        self.root = self.__insert(self.root,x)
        return self.root
    def update(self,x0,x):
        '''修改节点值
        x0:要查找的值
        x: 新增值
        ''' 
        if self.root == None:
            return 0
        else:
            node = self.search(x0)
            if node:
                node.value = x
                return node
            else:
                print("未查找到指定值")
                return False
    def search(self,x):
        '''查找子节点'''
        if not self.root:
            return None
        else:
            stack = []
            stack.append(self.root)
            while len(stack) > 0:
                node = stack.pop()
                if node == None:
                    print("No this node")
                    return None
                if node.value<x:
                    stack.append(node.right)
                elif node.value>x:
                    stack.append(node.left)
                else:
                    print("node.value:{}\nnode.height:{}\n".format(node.value, node.height))
                    return node
    def __delete(self,root,x):
        '''删除节点并保持树的平衡'''
        if root == None:
            print("We have not searched this node, so we can't delete it")
            return root
        else:
            if x < root.value:
                root.left = self.__delete(root.left, x)
            elif x > root.value:
                root.right = self.__delete(root.right, x)
            else:
                print("The deleted node's value is {},the height is {}\n".format(x,root.height))
                if not root.left or not root.right:
                    #单子节点或者没有子节点
                    if not root.left:
                        #print("Replace node's value is {}\n".format(root.right.value))
                        root = root.right
                    elif not root.right:
                        #print("Replace node's value is {}\n".format(root.left.value))
                        root = root.left
                    else:
                        root = None
                    if not root:
                        print None
                    else:
                        print("The replaced node's value is {}\n".format(root.value))
                else:
                    #双子节点
                    min_node = self._minNode(root.right)#寻找大于x的最小值
                    print("Replace node's value is {}\n".format(min_node.value))
                    root.value = min_node.value
                    root.right = self.__delete(root.right, root.value)#删除去替换x的节点
                print("Delete success!\n")
                #保持二叉树的平衡性
                balance_x = self.get_balance(self.root)
                if abs(balance_x) < 2:
                    return root
                else:
                    return self.balance_Tree(self.root, x)
            return root
    def delete(self,x):
        '''删除节点并保持树的平衡'''
        return self.__delete(self.root,x)
        
    def get_height(self, root):
        '''获取节点高度'''
        if not root:
            return 0
        return root.height
            #print root.value,max(self.get_height(root.left), self.get_height(root.right)) + 1
                       
    def get_balance(self,root):
        '''获取平衡因子'''
        if not root:
            return 0
        return self.get_height(root.left) - self.get_height(root.right)
    def is_balance(self,root):
        '''判断是否平衡'''
        if not root:
            print True
            return True
        if abs(self.get_balance(root)) < 2:
            self.is_balance(root.left) and self.is_balance(root.right)
        else:
            print False
            return False
    
    def balance_Tree(self,node,x):
        '''
        调整二叉树
        a) Left Left Case
                     
                 z                                      y 
                / \                                   /   \
               y   T4      right_rotate (z)          x      z
              / \          - - - - - - - - ->      /  \    /  \ 
             x   T3                               T1  T2  T3  T4
            / \
          T1   T2
        ============================================================================
        b) Left Right Case
                  
             z                               z                           x
            / \                            /   \                        /  \ 
           y   T4  Left Rotate (y)        x    T4  Right Rotate(z)    y      z
          / \      - - - - - - - - ->    /  \      - - - - - - - ->  / \    / \
        T1   x                          y    T3                    T1  T2 T3  T4
            / \                        / \
          T2   T3                    T1   T2
        ============================================================================

          c) Right Right Case
                 
          z                                y
         /  \                            /   \ 
        T1   y     left_rotate(z)       z      x
            /  \   - - - - - - - ->    / \    / \
           T2   x                     T1  T2 T3  T4
               / \
             T3  T4
        ============================================================================

        d) Right Left Case

           z                            z                            x
          / \                          / \                          /  \ 
        T1   y   Right Rotate (y)    T1   x      Left Rotate(z)   z      y
            / \  - - - - - - - - ->     /  \   - - - - - - - ->  / \    / \
           x   T4                      T2   y                  T1  T2  T3  T4
          / \                              /  \
        T2   T3                           T3   T4

        '''
        balance = self.get_balance(node)
        if balance > 1:
            if x <node.left.value:
                #(a) Left Left Case
                print("LL")
                return self.right_rotate(node)
            else:
                #(b) Left Right Case
                print("LR")
                return self.LR(node)
        if balance < -1:
            if x > node.right.value:
                #(c) Right Right Case
                print("RR")
                return self.left_rotate(node)
            else:
                #(d) Right Left Case
                print("RL")
                return self.RL(node)
    def get_minNode(self):
        '''最小左节点'''
        if not self.root:
            return None
        else:
            Min_node = self._minNode(self.root)
            print("最小左节点为{}\n".format(Min_node.value))
            return  Min_node
    def _minNode(self,root):
        if root.left:
            node = root.left
            return self._minNode(node)
        else:
            return root
    def get_maxNode(self):
        '''最大右节点'''
        if not self.root:
            return None
        else:
            Max_node = self._maxNode(self.root)
            print("最大右节点为{}\n".format(Max_node.value))
            return  Max_node
    def _maxNode(self, root):
        if root.right:
            node = root.right
            return self._maxNode(node)
        else:
            return root
    def right_rotate(self, z):
        '''LL'''
        y = z.left
        T3 = y.right
        #开始向右旋转
        y.right = z
        z.left = T3
        
        z.height = max(self.get_height(z.left), self.get_height(z.right)) + 1
        y.height = max(self.get_height(y.left), self.get_height(y.right)) + 1 
        return y
    def left_rotate(self, z):
        '''RR'''
        y = z.right
        T2 = y.left
        
        y.left = z
        z.right = T2
        #开始向左旋转
        z.height = max(self.get_height(z.left), self.get_height(z.right)) + 1
        y.height = max(self.get_height(y.left), self.get_height(y.right)) + 1     
        return y
    def LR(self, z):
        z.left = self.left_rotate(z.left)
        return self.right_rotate(z)
    def RL(self, z):
        z.right = self.right_rotate(z.right)
        return self.left_rotate(z)
    def BFS(self):
        print("\n广度优先遍历:\n")
        if self.root is None:
            return
        queue = []
        h_bfs = []
        queue.append(self.root)
        while len(queue)>0:
            node = queue.pop(0)
            print node.value,
            h_bfs.append(node.height)
            if node.left:
                queue.append(node.left)
            if node.right:
                queue.append(node.right)
        print("\n{}".format(h_bfs))    
    def DFS(self):
        print("深度优先遍历:\n")
        if self.root is None:
            return        
        stack = []
        stack.append(self.root)
        h_dfs = []
        while len(stack)>0:
            node = stack.pop()
            print node.value,
            h_dfs.append(node.height)
            if node.right:
                stack.append(node.right)
            if node.left:
                stack.append(node.left)
        print("\n{}".format(h_dfs))
    def levelorder(self,root):
        print("\n层次遍历(按层输出):\n")
        if not root:
            return None
        else:
            q = []
            q.append(root)
            v = []
            h = max(self.get_height(root.left), self.get_height(root.right)) + 1
            while len(q)>0:
                p = []
                s = []
                f = 2*h+1
                print "*"*(2*h-3+f),
                t = 0
                for c in q:
                    k = 7
                    if c:
                        s.append(c.value)
                        print"{}".format(c.value)+" "*(k),
                        if c.left:
                            p.append(c.left)
                        else:
                            p.append(None)
                        if c.right:
                            p.append(c.right)
                        else:
                            p.append(None)
                    else:
                        print " "*(k),
                q = p
                print("\n")
                if s:
                    v.append(s)
                h -= 1
            return v
    def preorder_tree(self,root):
        '''先序遍历'''
        if not root:
            return 0
        else:
            print root.value,
            self.preorder_tree(root.left)
            self.preorder_tree(root.right)
    def Inorder_tree(self,root):
        '''中序遍历'''
        if not root:
            return 0
        else:
            self.Inorder_tree(root.left)
            print root.value,
            self.Inorder_tree(root.right)
    def postorder_tree(self,root):
        '''后序遍历'''
        if not root:
            return 0
        else:
            self.postorder_tree(root.left)
            self.postorder_tree(root.right)
            print(root.value),

if __name__ == '__main__':
    l = [x for x in range(random.randrange(10,20))]
    random.shuffle(l)
    print l
    #l = [1, 4, 2, 0, 6, 3, 5]
    Avltree = AVLTree()
    for i in range(len(l)):
        Avltree.add(l[i])
    Avltree.DFS()
    Avltree.BFS()
    Avltree.levelorder(Avltree.root)
    print("\n前序遍历:\n")
    Avltree.preorder_tree(Avltree.root)
    print("\n中序遍历:\n")
    Avltree.Inorder_tree(Avltree.root)
    print("\n后序遍历:\n")
    Avltree.postorder_tree(Avltree.root)
    print("\n判断该树是否平衡:\n")
    Avltree.is_balance(Avltree.root)
    Avltree.get_minNode()
    Avltree.get_maxNode()
    Avltree1 = AVLTree()
    for i in range(len(l)):
        Avltree1.insert(l[i])
    Avltree1.levelorder(Avltree1.root)
    print("\n判断该树是否平衡:\n")
    print Avltree1.is_balance(Avltree1.root)
    Avltree1.DFS()
    Avltree1.BFS()
    seeks = random.choice(l)
    Avltree1.search(seeks)
    Avltree1.delete(seeks)
    Avltree1.update(seeks,seeks+1)
