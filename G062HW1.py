from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand
from pyspark.mllib.linalg import Vectors
import math
import time

# Definition of global variables
data_path = None
D = None
M = None
K = None
L = None


def MRApproxOutliers(inputPoints, D, M, K):
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
    # cells_counts is an RDD whose elements are (cell,number of points)
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
     
    print("Number of sure outliers =",outlierPoints,"\nNumber of uncertain points =",uncertainPoints )
    

    # Print the first K non-empty cells
    # If there are less than K non-empty cells , take(K) returns all the elements
    first_K_cells = cells_counts.map(lambda x: (x[1], x[0])).sortByKey().take(K)
    cell_print_str = ""
    
    for count, cell in first_K_cells:
        cell_print_str += f"Cell: ({cell[0]},{cell[1]})  Size = {count}\n"
    
    print(cell_print_str.rstrip('\n'))

    # Running time
    end_time = time.time()
    running_time_ms = int((end_time - start_time) * 1000)
    print("Running time of MRApproxOutliers =", running_time_ms, "ms")
    return outlierPoints

# Function used by ExactOutliers to compute all pairwise distances
def computeDistance(point_p,inputPoints):
    
    distances_p = []
    for point in inputPoints:
            dist = math.dist(point_p,point)
            distances_p.append(dist)
    
    return distances_p


def ExactOutliers(listOfPoints,D,M,K):
    start_time = time.time()
    

    outliers = [] 
    inputPoints = [tuple(p) for p in listOfPoints] # I need a list tuples since lists are not hashable
    outlier_ball_dict = {}
    ballP_list = []
    
    # Compute all pairwise distances
    for point_p in inputPoints:
        ballP_list.clear()
        distances_p = computeDistance(point_p,inputPoints)
        
        # For a certain point p , save in a list all points inside its ball of radius D
        for dist in distances_p:
            if dist <= D:
               ballP_list.append(dist)
        
        # If there are less than M points inside the ball of point p , label p as an outlier
        if len(ballP_list) <= M:
            outliers.append(point_p)
            outlier_ball_dict[point_p] = len(ballP_list)
    
    print("Number of Outliers =",len(outliers))
    
    # Sort the outlier dict by value in descending order
    sorted_ball_sizes = sorted(outlier_ball_dict.items(), key=lambda x: x[1],reverse = True) 
    
    # Print the first K outliers by ball size
    for i in range(min(K, len(outliers))):
        point, _ = sorted_ball_sizes[i]
        print(f"Point: ({point[0]},{point[1]})")
    
    end_time = time.time()
    
    # Running time
    running_time_ms = int((end_time - start_time) * 1000)
    print("Running time of ExactOutliers =", running_time_ms, "ms")
    
    return outliers

def main():

    conf = SparkConf().setMaster("local").setAppName('G062HW1')
    sc = SparkContext(conf=conf)

    # CHECKING NUMBER OF CMD LINE PARAMETERS
    assert len(sys.argv) == 6, "Usage: python G062HW1.py <file_name> <D> <M> <K> <L>"


    global data_path,D,M,K,L
    MAX_POINTS = 200000
    
    # Parse command-line arguments
    data_path = sys.argv[1]
    D = float(sys.argv[2])
    M = int(sys.argv[3])
    K = int(sys.argv[4])
    L = int(sys.argv[5])

    print(f"{data_path} D={D} M={M} K={K} L={L}")
    
    rawData = sc.textFile(data_path).repartition(numPartitions=L)
    inputPoints = rawData.map(lambda line: [float(i) for i in line.split(",")])
    
    print("Number of points =",inputPoints.count())

    if (inputPoints.count() < MAX_POINTS):
        listOfPoints = inputPoints.collect()
        ExactOutliers(listOfPoints,D,M,K)

    MRApproxOutliers(inputPoints,D,M,K)
        
    
if __name__ == "__main__":
	main()
