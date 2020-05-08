def kthGrammar( N, K):
    '''https://leetcode-cn.com/problems/k-th-symbol-in-grammar/solution/di-kge-yu-fa-fu-hao-by-leetcode/'''
    if N == 1: return 0
    return (1 - K%2) ^ kthGrammar(N-1, (K+1)/2)


def kthGrammar_recursive(self, N, K):
    if N == 1: return 0
    if K <= (2**(N-2)):
        return kthGrammar_recursive(N-1, K)
    return kthGrammar_recursive(N-1, K - 2**(N-2)) ^ 1

def kthGrammar_bin(self, N, K):
    return bin(K - 1).count('1') % 2
