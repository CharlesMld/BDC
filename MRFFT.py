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
    print(type(P))
    # R = max(P, key=lambda point: min(math.dist(point, center) for center in centers_per_partition.collect()))


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
    inputPoints = rawData.map(lambda line: [float(i) for i in line.split(",")])
    MRFFT(inputPoints, 3)

if __name__ == "__main__":
    main()