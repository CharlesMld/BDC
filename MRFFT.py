from pyspark import SparkContext, SparkConf
import random as rand
import math
import heapq
import time
# Now wants a list as input
def SequentialFFT(P, K):
    rand.seed(42)
    centers = [rand.choice(P)]
    remaining_points = [point for point in P if point not in centers]
    while len(centers) < K:
        farthest_point = max(remaining_points, key=lambda p: min(math.dist(p, c) for c in centers) )
        remaining_points.remove(farthest_point)
        centers.append(farthest_point)
    return centers




def MRFFT(P, K):
    print("Starting MRFFT...")
    partitions = P.repartition(2)
    print("----------------- ROUND 1 -----------------\n")
    
    centers_per_partition = partitions.mapPartitions(lambda partition: SequentialFFT(list(partition), K))
    
    #print("Centers per partition: ", centers_per_partition.collect(), "\n")
    
    print("----------------- ROUND 2 -----------------\n")
    
    centers_per_partition_count = centers_per_partition.count()
    centers_per_partition.cache()
    start_time = time.time()
    gathered_centers = centers_per_partition.collect()
    
    C = SequentialFFT(gathered_centers, K) # C is the set of centers
    end_time = time.time()
    running_time_ms = ((end_time - start_time) )
    print("Running time of MRApproxOutliers =", running_time_ms, "ms")
    print("Centers: ", C, "\n")

    print("----------------- ROUND 3 -----------------\n")
    context = SparkContext.getOrCreate()
    broadcast_C = context.broadcast(C)
    print(f"broad ={broadcast_C.value}")

    points_2_distances = P.map(lambda point: min(math.dist(point, center) for center in broadcast_C.value))
    #print("Distances: ", points_2_distances.collect(), "\n")
    FarthestPoint = points_2_distances.reduce(lambda x, y: max(x, y))
    print("Radius: ", FarthestPoint, "\n")

    
    #print(f"app name context={context.appName}")
    #print(f"config context = {context.getConf}")
    
    


    # list = [element for element in partitions]
    # centers = [rand.choice(list)]  # Select a random point from inputPoints
    # while len(centers) < K:
    #     # we compute the farthest point from the existing centers
    #     farthest_point = max(list, key=lambda point: min(math.dist(point, center) for center in centers))
    #     # we add the farthest point to the centers list
    #     centers.append(farthest_point)
    # print("Centers: ", centers, "List: ", list)
    # return centers



def main():
    print("Starting...")
    conf = SparkConf().setMaster("local").setAppName('MRFFT')
    sc = SparkContext(conf=conf)
    print(f"app name sc = {sc.appName}")
    print(f"config sc = {sc.getConf}")
    rawData = sc.textFile("artificial1M_9_100.csv")
    inputPoints = rawData.map(lambda line: [float(i) for i in line.split(",")])
    MRFFT(inputPoints, 3)
    
    print("Number of points =",inputPoints.count())

if __name__ == "__main__":
    main()
    