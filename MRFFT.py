from pyspark import SparkContext, SparkConf
import random as rand
import math

def euclidean_distance(point1, point2):
    return math.sqrt(sum((p1 - p2)**2 for p1, p2 in zip(point1, point2)))

def SequentialFFT(P):
    centers = [rand.choice(P)]  # Select a random point from inputPoints
    # Repeat until k centers are selected
    while len(centers) < K:
        # we compute the farthest point from the existing centers
        farthest_point = max(P, key=lambda point: min(euclidean_distance(point, center) for center in centers))
        # we add the farthest point to the centers list
        centers.append(farthest_point)
    return centers

def MRFFT(RDDinputPoints):
    transformed_RDD = RDDinputPoints.mapPartitions(SequentialFFT)
    print(transformed_RDD.collect())
    return transformed_RDD

def main():
    print("Starting...")
    conf = SparkConf().setMaster("local").setAppName('MRFFT')
    sc = SparkContext(conf=conf)
    global K 
    K = 3    
    # list = [[0.4, 0.9], [0.5, 4.1], [0.8, 0.91], [0.81, 1.1], [1.1, 5.0], [1.11, 5.1], [1.5, 1.1], [1.52, 1.11], [1.53,1.12], [1.54,1.13], 
    #        [1.51,3.2], [1.52,3.6], [3.21,4.6], [4.11,4.11], [4.32,4.3]]
    sc.textFile("input.txt") # is an RDD of strings split into 2 partitions
    rawData = sc.textFile("input.txt").repartition(numPartitions=3) # arbitrary number of partitions
    # Partition 1: ["0.4,0.9", "0.5,4.1", "0.8,0.91", "0.81,1.1"]
    # Partition 2: ["1.1,5.0", "1.11,5.1", "1.5,1.1", "1.52,1.11"]
    # inputPoints = rawData.map(lambda line: [float(i) for i in line.split(",")])
    print(rawData.glom().collect())
    # Partition 1: [[0.4, 0.9], [0.5, 4.1], [0.8, 0.91], [0.81, 1.1]]
    # Partition 2: [[1.1, 5.0], [1.11, 5.1], [1.5, 1.1], [1.52, 1.11]]
    # P = inputPoints.collect() to collect all points in a list
    MRFFT(rawData)

if __name__ == "__main__":
    main()