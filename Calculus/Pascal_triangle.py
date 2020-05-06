#coding:utf-8
#Date: 2020-05-07 00:34:00
#杨辉三角递归
def pascalTri(numRows):
    """
    :type numRows: int
    :rtype: List[List[int]]
    递归三步:
    1. 整个递归的终止条件
    2. 返回值p
    3. 在一次递归中需要做的操作
    """
    
    if numRows == 0:
        return []

    #终止条件
    if numRows == 1:
        p = [[1]]
        return p
    #进入递归
    p = generate(numRows - 1)
    #递归中一次运算中需要做的事情  
    n = 1
    j = []#第numRows行的list
    j.append(n)
    for i in range(1,numRows-1):
        j.append(p[numRows-2][i-1]+p[numRows-2][i])
    j.append(1)
    p.append(j)
    return p
