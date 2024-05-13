from pyspark import SparkContext, SparkConf
import random as rand
import math

# Now wants a list as input
def SequentialFFT(P,K):
    #listP = [element for element in P] # Changed name because list is a python keyword
    C = [rand.choice(P)] # we choose the first center randomly, C is set of centers
    while len(C) < K:
        # we calculate the farthest point from the existing centers
        # Fixed an issue here, we should check for new centers in P-C , not in P as before
        farthest_point = max((point for point in P if point not in C), key=lambda point: min(math.dist(point, center) for center in C))
        # we add the farthest point to the centers list
        C.append(farthest_point)
    return C

def FarthestPoint(P, centers):
    list = [element for element in P]
    farthestpoints = [max(list, key=lambda point: min(math.dist(point, center) for center in centers))]
    return farthestpoints



def MRFFT(P, K):
    print("Starting MRFFT...")
    partitions = P.repartition(10)
    print("----------------- ROUND 1 -----------------\n")
    centers_per_partition = partitions.mapPartitions(lambda partition: SequentialFFT(list(partition), K))
    print("Centers per partition: ", centers_per_partition.collect(), "\n")
    
    print("----------------- ROUND 2 -----------------\n")
    C = SequentialFFT(centers_per_partition.collect(), K) # C is the set of centers
    print("Centers: ", C, "\n")

    print("----------------- ROUND 3 -----------------\n")
    context = SparkContext.getOrCreate()
    broadcast_C = context.broadcast(C)
    print(f"broad ={broadcast_C.value}")

    points_2_distances = P.map(lambda point: min(math.dist(point, center) for center in broadcast_C.value))
    print("Distances: ", points_2_distances.collect(), "\n")
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

def count_active_spark_contexts(): # This is just to count the number of contexts so I'm sure it's only 1
    active_contexts = SparkContext._active_spark_context
    if active_contexts is None:
        return 0
    elif isinstance(active_contexts, list):
        return len(active_contexts)
    else:
        return 1

def main():
    print("Starting...")
    conf = SparkConf().setMaster("local").setAppName('MRFFT')
    sc = SparkContext(conf=conf)
    print(f"app name sc = {sc.appName}")
    print(f"config sc = {sc.getConf}")
    rawData = sc.textFile("input.txt")
    inputPoints = rawData.map(lambda line: [float(i) for i in line.split(",")])
    MRFFT(inputPoints, 3)
    
    print("Number of points =",inputPoints.count())

if __name__ == "__main__":
    main()
    