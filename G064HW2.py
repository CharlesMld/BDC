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
def find_duplicate_points(point_list):
    # Create an empty dictionary to store the counts of each point
    point_list = [tuple(point) for point in point_list]
    point_counts = {}

    # Iterate through the list of points and count occurrences
    for point in point_list:
        if point in point_counts:
            point_counts[point] += 1
        else:
            point_counts[point] = 1

    # Filter points that have count greater than 1 (i.e., duplicates)
    duplicate_points = [(point,count) for point, count in point_counts.items() if count > 1]

    return duplicate_points

def SequentialFFT(P, K):
    rand.seed(42)
    centers = [rand.choice(P)]
    distances = [[i, math.dist(point, centers[0])] for i, point in enumerate(P)]

    while len(centers) < K:
        # Find the farthest point
        farthest_point_index, _ = max(distances, key=lambda x: x[1])
        farthest_point = P[farthest_point_index]
        
        # Update distances for the newly added center
        for i, point in enumerate(P):
            if point not in centers:
                distances[i][1] = min(distances[i][1], math.dist(point, farthest_point))
            else:
                pass

        centers.append(farthest_point)
            
    return centers



def MRFFT(P, K):
    # ROUND 1
    st = time.time()
    
    centers_per_partition = P.mapPartitions(lambda partition: SequentialFFT(list(partition),K))
    centers_count = centers_per_partition.count()
    centers_per_partition.cache()
    
    et = time.time()
    print(f"Running time of MRFFT Round 1 = {int((et - st) * 1000)} ms")
    
    # ROUND 2
    
    st = time.time()
    aggregated_centers = centers_per_partition.collect()
    
    
    C = SequentialFFT(aggregated_centers, K)
    print(f"centers={C}") # run SequentialFFT again to get the final set of centers
    
    et = time.time()
    print(f"Running time of MRFFT Round 2 = {int((et - st) * 1000)} ms")

    # ROUND 3
    st = time.time()
    
    context = SparkContext.getOrCreate()
    broadcast_C = context.broadcast(C)
    #Compute radius
    R = P.mapPartitions(lambda partition: [min(math.dist(point, center) for center in broadcast_C.value) for point in partition]).reduce(lambda x,y: max(x,y))
    #FarthestPoint = P.map(lambda point: min(math.dist(point, center) for center in broadcast_C.value)).reduce(max)
    et = time.time()
    print(f"Running time of MRFFT Round 3 = {int((et - st) * 1000)} ms")
    
    print(f"Radius = {round(R,8)}")
    return R


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
    
    data_path = sys.argv[1]
    M = int(sys.argv[2])
    K = int(sys.argv[3])
    L = int(sys.argv[4])

    # print command line args
    print(f"{data_path} M={M} K={K} L={L}")
    
    # read data
    rawData = sc.textFile(data_path).repartition(numPartitions=L)
    inputPoints = rawData.map(lambda line: [float(i) for i in line.split(",")])
    
    #inputPoints.cache()
    #input = inputPoints.collect()
    print(f"Number of points = {inputPoints.count()}")

    D = MRFFT(inputPoints, K)
    MRApproxOutliers(inputPoints, D, M)
    #dup = find_duplicate_points(input)
    #print(f"dup={len(dup)}")

if __name__ == "__main__":
	main()