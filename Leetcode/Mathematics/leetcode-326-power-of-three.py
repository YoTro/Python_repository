def isPowerOfThree( n):
    """
    :type n: int
    :rtype: bool
    """
    if n == 0:
        return False
    while n>3:
        print n
        n /= 3.0
    if n<3 and n != 0:
        return False
    else:
        print n
        return True
