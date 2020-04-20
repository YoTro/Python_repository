class TreeNode(object):
    def __init__(self,x):
        self.value = x
        self.left = None
        self.right = None
    def insertleft(self,x):
        if self.left == None:
            self.left = TreeNode(x)
        else:
            t = TreeNode(x)
            self.left, t.left = t, self.left
    def insertright(self,x):
        if self.right == None:
            self.right = TreeNode(x)
        else:
            t = TreeNode(x)
            t.right = self.right
            self.right = t

class solution():
    def rangeSumbst(self,root,L,R):
        self.ans = 0
        stack = [root]
        def dfs(node,stack):
            while stack:
                node = stack.pop()
                if node:
                    #print(node.value)
                    if L <= node.value <= R:
                        self.ans += node.value
                    if node.value < R:
                        stack.append(node.right)
                    if node.value > L:
                        stack.append(node.left)
        dfs(root,stack)
        return self.ans

def postorder(tree):
    if tree:
        for key in postorder(tree.left):
            yield key
        for key in postorder(tree.right):
            yield key
        yield tree.key
        
if __name__ == '__main__':
    root = [10,5,15,3,7,None,18]
    L = 7
    R = 15
    tree = TreeNode('')
    for i in range(((len(root)-2)/2)):
        #print(i,root[i*2+1],root[i*2+2])
        tree.insertleft(root[i*2+1])
        
        tree.insertright(root[i*2+2])

    if tree:
        for key in postorder(tree.left):
            yield key
        for key in postorder(tree.right):
            yield key
        yield tree.key
    s = solution()
    ans = s.rangeSumbst(tree,L,R)
    print(ans)
