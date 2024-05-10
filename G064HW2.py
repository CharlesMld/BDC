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
L = None


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
    cells_counts = inputPoints.map(pointToCell).reduceByKey(lambda a,b: a + b)
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

def main():
    print("Starting...")
    conf = SparkConf().setMaster("local").setAppName('G064HW2')
    sc = SparkContext(conf=conf)

    # CHECKING NUMBER OF CMD LINE PARAMETERS
    assert len(sys.argv) == 5, "Usage: python G064HW2.py <file_name> <D> <M> <L>"


    global data_path,D,M,L
    
    
    # Parse command-line arguments
    data_path = sys.argv[1]
    D = float(sys.argv[2])
    M = int(sys.argv[3])
    L = int(sys.argv[4])

    print(f"{data_path} D={D} M={M} L={L}")
    
    rawData = sc.textFile(data_path).repartition(numPartitions=L)
    inputPoints = rawData.map(lambda line: [float(i) for i in line.split(",")])
    
    print("Number of points =",inputPoints.count())

    MRApproxOutliers(inputPoints,D,M)
        
    
if __name__ == "__main__":
	main()