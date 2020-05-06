class ListNode(object):
 def __init__(self, x):
     self.val = x
     self.next = None
res = []
class Solution:
    
    def __init__(self):
        self.head = None
        
    def add(self, head, x):
        if not head:
            head = ListNode(x)
        else:
            head.next = self.add(head.next, x)
        return head
    def add2(self, l):
        if len(l) == 0:
            return None
        for i in range(len(l)):
            self.head = self.add(self.head, l[i])
        return self.head
    def dfs(self, head):
        if head:
            res.append(head.val)
            self.dfs(head.next)
        
        return res
    

if __name__ == '__main__':
    lt = Solution()
    l = [x*2 for x in range(7)]
    lt.add2(l)
    r = lt.dfs(lt.head)
    print r

