from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand
import math
import time


"""
MRApproxOutliers
"""
def MRApproxOutliers(inputPoints, D, M): 
    start_time = time.time()
    
    omega = D/(2*math.sqrt(2))
    
    # Given an RDD of points returns the cell each point(element of the RDD) belongs to
    def pointToCell(iterator):
        result = []

        for point in iterator:
            cell_x = int(math.floor(point[0] / omega))
            cell_y = int(math.floor(point[1] / omega))
            cell = (cell_x, cell_y)
            result.append((cell,1))

        return result
    
    # Step A
    # cells_counts is an RDD whose elements are (cell, number of points)
    cells_counts = inputPoints.mapPartitions(pointToCell).reduceByKey(lambda a,b: a + b)
    # Assuming there are few non-empty cells I can save the RDD in a dictionary
    cells_counts_dict = cells_counts.collectAsMap()
    
    

    # Step B
    # These 2 functions count for each cell the number of points in a 7x7 and 3x3 region of cells respectively
    def region_counts7(cell_counts):
        cell, _ = cell_counts
        x, y = cell
        total_count = 0
        for i in range(x - 3, x + 4):
            for j in range(y - 3, y + 4):
                if (i, j) in cells_counts_dict:
                    total_count += cells_counts_dict[(i, j)]

        return (cell, total_count)
    
    def region_counts3(cell_counts):
        cell, _ = cell_counts
        x, y = cell
        total_count = 0
        for i in range(x - 1, x + 2):
            for j in range(y - 1, y + 2):
                if (i, j) in cells_counts_dict:
                    total_count += cells_counts_dict[(i, j)]

        return (cell, total_count)


    
    # Filter operation to select the cells sure and uncertain outlier cells 
    outlierCells = cells_counts.map(region_counts7).filter(lambda x: x[1] <= M).collectAsMap()
    uncertainCells = cells_counts.map(region_counts3).filter(lambda x: x[1] <= M and x[0] not in outlierCells).collectAsMap()

    # Count sure outliers and uncertain points
    outlierPoints = inputPoints.filter(lambda x: (int(math.floor(x[0] / omega)), int(math.floor(x[1] / omega))) in outlierCells).count()
    uncertainPoints = inputPoints.filter(lambda x: (int(math.floor(x[0] / omega)), int(math.floor(x[1] / omega))) in uncertainCells).count()

    print("Number of sure outliers =", outlierPoints,"\nNumber of uncertain points =",uncertainPoints )

    # Running time
    end_time = time.time()
    running_time_ms = int((end_time - start_time) * 1000)
    print("Running time of MRApproxOutliers =", running_time_ms, "ms")
    return outlierPoints


"""
MRFFT
"""
def SequentialFFT(P,K):
    C = [rand.choice(P)] # we choose the first center randomly, C is set of centers
    while len(C) < K:
        # we calculate the farthest point from the existing centers
        # Fixed an issue here, we should check for new centers in P-C , not in P as before
        farthest_point = max((point for point in P if point not in C), key=lambda point: min(math.dist(point, center) for center in C))
        # we add the farthest point to the centers list
        C.append(farthest_point)
    return C


def MRFFT(P, K):
    # ROUND 1
    st = time.time()
    
    partitions = P.repartition(10)
    centers_per_partition = partitions.mapPartitions(lambda partition: SequentialFFT(list(partition), K))
    
    et = time.time()
    print(f"Running time of MRFFT Round 1 = {int((et - st) * 1000)} ms")
    
    # ROUND 2
    st = time.time()
    
    C = SequentialFFT(centers_per_partition.collect(), K) # C is the set of centers
    
    et = time.time()
    print(f"Running time of MRFFT Round 2 = {int((et - st) * 1000)} ms")

    # ROUND 3
    st = time.time()
    
    context = SparkContext.getOrCreate()
    broadcast_C = context.broadcast(C)
    points_2_distances = P.map(lambda point: min(math.dist(point, center) for center in broadcast_C.value))
    FarthestPoint = points_2_distances.reduce(lambda x, y: max(x, y))
    
    et = time.time()
    print(f"Running time of MRFFT Round 3 = {int((et - st) * 1000)} ms")
    
    print(f"Radius = {FarthestPoint}")
    return FarthestPoint


"""
main
"""
def main():
    # spark setup
    conf = SparkConf().setAppName('G064HW2')
    conf.set("spark.locality.wait", "0s");
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # check and process command line args
    assert len(sys.argv) == 5, "Usage: python G064HW2.py <file_name> <M> <K> <L>"

    global data_path, M, K, L
    
    data_path = sys.argv[1]
    M = int(sys.argv[2])
    K = int(sys.argv[3])
    L = int(sys.argv[4])

    # print command line args
    print(f"{data_path} M={M} K={K} L={L}")
    
    # read data
    rawData = sc.textFile(data_path).repartition(numPartitions=L)
    inputPoints = rawData.map(lambda line: [float(i) for i in line.split(",")])
    
    print(f"Number of points = {inputPoints.count()}")

    D = MRFFT(inputPoints, K)
    MRApproxOutliers(inputPoints, D, M)

if __name__ == "__main__":
	main()