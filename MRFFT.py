from pyspark import SparkContext, SparkConf
import random as rand
import math

def MRFFT(P):
    list = [element for element in P]
    centers = [rand.choice(list)]  # Select a random point from inputPoints
    while len(centers) < K:
        # we compute the farthest point from the existing centers
        farthest_point = max(list, key=lambda point: min(math.dist(point, center) for center in centers))
        # we add the farthest point to the centers list
        centers.append(farthest_point)
    print("Centers: ", centers, "List: ", list)
    return centers

def main():
    print("Starting...")
    conf = SparkConf().setMaster("local").setAppName('MRFFT')
    sc = SparkContext(conf=conf)
    global K 
    K = 3    
    # list = [[0.4, 0.9], [0.5, 4.1], [0.8, 0.91], [0.81, 1.1], [1.1, 5.0], [1.11, 5.1], [1.5, 1.1], [1.52, 1.11], [1.53,1.12], [1.54,1.13], 
    #        [1.51,3.2], [1.52,3.6], [3.21,4.6], [4.11,4.11], [4.32,4.3]]
    rawData = sc.textFile("input.txt").repartition(2)
    rawData = rawData.map(lambda line: [float(i) for i in line.split(",")])
    # Partition 1: ["0.4,0.9", "0.5,4.1", "0.8,0.91", "0.81,1.1"]
    # Partition 2: ["1.1,5.0", "1.11,5.1", "1.5,1.1", "1.52,1.11"]
    # inputPoints = rawData.map(lambda line: [float(i) for i in line.split(",")])
    print(rawData.glom().collect())
    # Partition 1: [[0.4, 0.9], [0.5, 4.1], [0.8, 0.91], [0.81, 1.1]]
    # Partition 2: [[1.1, 5.0], [1.11, 5.1], [1.5, 1.1], [1.52, 1.11]]
    # P = inputPoints.collect() to collect all points in a list
    print(rawData.mapPartitions(MRFFT).collect())

if __name__ == "__main__":
    main()