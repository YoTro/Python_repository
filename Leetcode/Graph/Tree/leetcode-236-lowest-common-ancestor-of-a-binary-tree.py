#Definition for a binary tree node.
class TreeNode(object):
 def __init__(self, x):
     self.val = x
     self.left = None
     self.right = None

class Solution(object):
    '''
    二叉树的最近公共祖先
    定义:“对于有根树 T 的两个结点 p、q，最近公共祖先表示为一个结点 x，满足 x 是 p、q 的祖先且 x 的深度尽可能大（一个节点也可以是它自己的祖先）。
    方法一:递归
    方法二:hashmap存储父节点,先遍历p的父节点,并把访问过的父节点标记
    再访问q的父节点,一旦遇到标记以访问过的就返回该节点为最近公共祖先
    '''
        
    def lowestCommonAncestor(self, root, p, q):
        """
        :type root: TreeNode
        :type p: TreeNode
        :type q: TreeNode
        :rtype: TreeNode
        """
        rl, rl_dic = self.inorder(root)
        pl,pl_dic = self.inorder(p)
        ql,ql_dic = self.inorder(q)
        res = [0]*len(rl)#做标记的数组
        #如果q或者p在对方的子节点中,则直接返回对方为最近公共祖先
        if q.val in pl:
            return p
        if p.val in ql:
            return q
        #如果在左右子树上
        if q.val not in pl and p.val not in ql:
            qr = rl_dic[q.val]
            res[rl.index(qr)] = -1
            pr = rl_dic[ p.val ]
            if qr == pr:
                #print pr
                return TreeNode(qr)    
            while qr != root.val:
                #print qr
                qr = rl_dic[ qr]
                res[rl.index(qr)] = -1
            print res,res[rl.index(pr)]
            while res[rl.index(pr)] != -1:
                #print pr
                pr = rl_dic[ pr]                               
            return TreeNode(pr)
    def inorder(self, root):
        '''中序遍历'''
        stack, res = [root], []
        root_dic = {}
        while stack:
            node = stack.pop()
            if isinstance(node, TreeNode):
                stack.extend([node.val,node.right, node.left])
                if not node.right:
                    r = None
                else:
                    r = node.right.val
                if not node.left:
                    l = None
                else:
                    l = node.left.val
                root_dic[l] = node.val
                root_dic[r] = node.val
            elif isinstance(node, int):
                res.append(node)
        #print res, root_dic
        return res, root_dic
   def lowestCommonAncestor_Recursive(self, root, p, q):
        """
        :type root: TreeNode
        :type p: TreeNode
        :type q: TreeNode
        :rtype: TreeNode
        时间复杂度：O(N)
        空间复杂度：O(N)
        
        """
        #1. 终止条件
        if not root or root == p or root == q:
            return root
        #2. 返回值
        left = self.lowestCommonAncestor_Recursive(root.left, p, q)
        right = self.lowestCommonAncestor_Recursive(root.right, p, q)
        #3. 一次递归操作 
        if not left:
        #如果左边为空,则p,q祖先在右子树
            return right
        if not right:
        #如果️边为空,则p,q祖先在左子树
            return left
        return root
