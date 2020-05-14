import random
def Hanoi_Tower(n):
    assert n > 0 and isinstance(n, int)
    dp = [1]*(n+1)
    for i in range(2,n+1):
        dp[i] = 2*dp[i-1] + 1
    return dp[-1]

if __name__ == '__main__':
	t = random.randrange(50)
	print Hanoi_Tower(t)