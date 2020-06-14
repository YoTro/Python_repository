import numpy as np
import heapq
from scipy.sparse import csc_matrix


def pageRank(G, s=.85, maxerr=.0001):
    """
    Computes the pagerank for each of the n states
    Parameters
    $$R = ( d M + \frac { 1 - d } { n } E ) R = A R$$
    ----------
    G: matrix representing state transitions
       Gij is a binary value representing a transition from state i to j.
    s: probability of following a transition. 1-s probability of teleporting
       to another state.
    maxerr: if the sum of pageranks between iterations is bellow this we will
            have converged.
    """
    n = G.shape[0]

    # compressed sparse G-matrix according to column
    A = csc_matrix(G, dtype=np.float)
    #Retrun one dimensional array that the count of every rows' nonzero numbers
    rsums = np.array(A.sum(1))[:, 0]
    #return the indexs of nonzero which include (row,col)
    ri, ci = A.nonzero()
    #probability l(pij)
    A.data /= rsums[ri]
    # bool array of sink states
    sink = rsums == 0
    #initialize r
    ro, r = np.zeros(n), np.ones(n)
    # Compute pagerank r with Power iteration until converge
    while np.sum(np.abs(r - ro)) > maxerr:
        #initialize ro 
        ro = r.copy()
        # calculate each pagerank at a time
        for i in range(n):
            # inlinks of state i
            Ai = np.array(A[:, i].todense())[:, 0]
            # account for sink states
            Di = sink / float(n)
            # account for teleportation to state i
            Ei = np.ones(n) / float(n)

            r[i] = ro.dot(Ai * s + Di * s + Ei * (1 - s))
    # return normalized pagerank
    return r / float(sum(r))

if __name__ == '__main__':
    #adjacency matrix
    G = np.array([[1,0,1,0,0,0,0],
                  [0,1,0,0,0,0,0],
                  [0,0,1,1,0,0,0],
                  [0,0,0,0,1,0,0],
                  [1,0,0,0,0,0,1],
                  [0,0,0,0,0,1,0],
                  [0,0,0,1,0,0,1]])
    print(pageRank(G,s=.85))
