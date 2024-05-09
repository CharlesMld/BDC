# BDC
Big Data Computing homeworks group repository

# Assignment of HW2
Modified version of HW1 where D is equal to the radius of a k-center clustering (for a suitable nb K of centers) (D basically replaced by K)
*the radius of a k-center clustering* max distance of a point to its closest center
2 advantages :
1. a better control on the number of non-empty cells
2. the potential for a sharper analysis

The goal of the homework will be to assess the effectiveness of this technique and test the scalability of a MapReduce implementation when run on large datasets.

## Task 1 (Charles) **DONE**
Modify of method/function MRApproxOutliers written for HW1 by removing the parameter K and the printing of the first K cells in non-decreasing order of cell size. (Please note that in HW2, a parameter K is used outside MRApproxOutliers, but with a totally different meaning with respect to HW1). Also fix bugs (if any) that have been pointed out in the correction of HW1.

## Task 2 (Zhaku)
Write a method/function SequentialFFT which implements the Farthest-First Traversal algorithm, through standard sequential code. SequentialFFT takes in input a set P of points and an integer  parameter K, and must return a set C of K centers. Both P and C must be represented as lists (ArrayList in Java and list in Python). The implementation should run in $O(|P|\times K)$ time.

## Task 3
Write a method/function MRFFT which takes in input a set P of points, stored in an RDD, and  an integer  parameter K, and implements the following MapReduce algorithm:
- Round 1 and 2 compute a set C of K centers, using the MR-FarthestFirstTraversal algorithm described in class. The coreset computed in Round 1, must be gathered in an ArrayList in Java, or a list in Python, and, in Round 2, the centers are obtained by running SequentialFFT on the coreset.
- Round 3 computes the radius R of the clustering induced by centers. Which is $max(dist(x,C)), \forall x\in P$. Impossible to download P to compute R since its too large. We must keep it stored as an RDD. However, the set of centers C computed in Round 2, can be used as a global variable. We're asked to copy C into a broadcast variable which can be accessed by the RDD methods that will be used to compute R. For efficiency, we suggest to compute the maximum of the $dist(x,C)$ distances with a reduce operation, which Sparks implements exploiting the partitions.

**MRFFT must compute and print, separately, the running time required by each of the above 3 rounds.**

## Task 4
GxxxHW2.py (for Python users), where xxx is your 3-digit group number (e.g., 004 or 045), which receives in input, as command-line arguments, a path to the file storing the input points,  and 3 integers M,K,L, and does the following:
- Prints the command-line arguments and stores M,K,L into suitable variables.
- Reads the input points into an RDD of strings (called rawData) and transforms it into an RDD of points (called inputPoints), represented as pairs of floats, subdivided into L partitions.
- Prints the total number of points.
- Executes MRFFT with parameters inputPoints and K, prints the returned radius and stores it into a float D (UPDATED)
- Executes MRApproxOutliers, modified as described above, with parameters inputPoints, D,M and prints its running time. (UPDATED)

**IMPORTANT**\
The program should receive command line arguments in the order specified and output should look like:\
>/data/BDC2324/uber-large.csv M=3 K=100 L=16\
Number of points = 1880795\
Running time of MRFFT Round 1 = 216 ms\
Running time of MRFFT Round 2 = 82 ms\
Running time of MRFFT Round 3 = 172 ms\
Radius = 0.10125376\
Number of sure outliers = 11\
Number of uncertain points = 42\
Running time of MRApproxOutliers = 420 ms

# Thoughts about HW2

