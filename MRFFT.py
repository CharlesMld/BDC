from pyspark import SparkContext, SparkConf
import random as rand
import math

def SequentialFFT(P,K):
    list = [element for element in P]
    centers = [rand.choice(list)] # we choose the first center randomly
    while len(centers) < K:
        # we calculate the farthest point from the existing centers
        farthest_point = max(list, key=lambda point: min(math.dist(point, center) for center in centers))
        # we add the farthest point to the centers list
        centers.append(farthest_point)
    return centers

def FarthestPoint(P, centers):
    list = [element for element in P]
    farthestpoints = [max(list, key=lambda point: min(math.dist(point, center) for center in centers))]
    return farthestpoints

def MRFFT(P, K):
    print("Starting MRFFT...")
    partitions = P.repartition(10)
    print("----------------- ROUND 1 -----------------\n")
    centers_per_partition = partitions.mapPartitions(lambda partition: SequentialFFT(partition, K))
    print("Centers per partition: ", centers_per_partition.collect(), "\n")
    
    print("----------------- ROUND 2 -----------------\n")
    centers = SequentialFFT(centers_per_partition.collect(), K)
    print("Centers: ", centers, "\n")

    print("----------------- ROUND 3 -----------------\n")
    points_2_distances = P.map(lambda point: min(math.dist(point, center) for center in centers))
    print("Distances: ", points_2_distances.collect(), "\n")
    FarthestPoint = P.reduce(lambda x, y: max(x, y))
    print("Max distance = Radius: ", FarthestPoint, "\n")


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
    rawData = sc.textFile("input.txt")
    rawData = rawData.map(lambda line: [float(i) for i in line.split(",")])
    MRFFT(rawData, 3)

if __name__ == "__main__":
    main()