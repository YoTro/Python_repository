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
    def delete(self, head, x):
        if not head or not head.next:
            return None
        a = head
        b = head.next
        if x != head.val:
            head.next = self.delete( head.next, x)
        if b.val == x:
            a.next = b.next
        return head
    def insert(self, index, x):
        '''
        index->int: 索引要插入的地方
        x->int:     要插入的值
        '''
        if not self.head:
            return None
        node = self.search(index)
        if node:
            tmp = ListNode(x)
            node.next = tmp
            tmp.next = node.next.next
            print("Insert success!")
        return self.head
    def search(self, x):
        node = self.head
        found = False
        while not found and node:
            if node.val == x:
                print("We found this node in the linked list")
                found = True
                return node
            else:
                node = node.next
        print("We can't found this node")
    def update(self, index, x):
        '''
        index->int: original node
        x->int:     new node
        '''
        if not self.head:
            return None
        node = self.search(index)
        if node:
            node.val = x
            print("Update success!")
        else:
            return None
        return node
            
    def dfs(self, head):
        if head:
            res.append(head.val)
            self.dfs(head.next)   
        return res
    def reverse(self, head):
        #1. 终止条件
        if not head or not head.next:
            return head
        #2. 返回值
        cur = self.reverse(head.next)
        #3. 一次递归操作
        head.next.next = head
        head.next = None
        return cur
    def mergeTwoLists(self, l1, l2):
        '''合并两个升序链表'''
        #1. 终止条件
        if l1 is None:
            return l2
        elif l2 is None:
            return l1
        #2. 返回值和一次递归中的操作内容
        elif l1.val < l2.val:
            l1.next = self.mergeTwoLists(l1.next, l2)
            return l1
        else:
            l2.next = self.mergeTwoLists(l1, l2.next)
            return l2   

if __name__ == '__main__':
    lt = Solution()
    l = [x*2 for x in range(7)]
    lt.add2(l)
    r = lt.dfs(lt.head)
    print r

