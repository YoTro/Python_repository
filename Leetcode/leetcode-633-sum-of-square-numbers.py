
import math
c = int(raw_input("please input a interger :\n"))
def judgeSquareSum( c):
    """
    :type c: int
    :rtype: bool
    """
    x = [i for i in range(int(math.sqrt(c)+1))]
    for i in x:
        if c < pow(i,2):
            break
        y = math.sqrt(c - pow(i,2))
        #print y
        b = True
        if math.modf(y)[0] == 0: 
            return b
        else:
            if i > c:
                return False

