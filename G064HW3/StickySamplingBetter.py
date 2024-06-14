from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import math
import random as rand

# After how many items should we stop?
THRESHOLD = -1  # To be set via command line

# Operations to perform after receiving an RDD 'batch' at time 'time'
def stickySampling(batch):
    global streamLength, sample, phi, epsilon, delta, stopping_condition

    batch_size = batch.count()
    if batch_size == 0:
        return
    # Stop the function
    if streamLength[0] >= THRESHOLD: 
        return
    # Increment stream length
    streamLength[0] += batch_size

    # Setting sampling rate
    expr = 1 / (delta * phi)
    r = (math.log(expr)) / epsilon
    s_rate = r / THRESHOLD # s_rate = 0.00016539483766422745
    print (f"s rate = {s_rate}")
    # Perform sampling 
    local_S = {}
    for item in batch.collect():  # Collect batch elements as a list and iterate
        item_int = int(item)
        p = rand.random()
        if (item_int in sample) :
            sample[item_int] = sample[item_int] + 1
        else:
            if p <= s_rate:
                sample[item_int] = 1



    # Stop the stream
    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()

if __name__ == '__main__':
    assert len(sys.argv) == 6, "USAGE: port, threshold, phi, epsilon, delta"

    conf = SparkConf().setMaster("local[*]").setAppName("DistinctExample")
    
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)  # Batch duration of 0.01 seconds
    ssc.sparkContext.setLogLevel("ERROR")
    
    stopping_condition = threading.Event()

    # INPUT READING
    portExp = int(sys.argv[1])
    print("Receiving data from port =", portExp)

    THRESHOLD = int(sys.argv[2])
    print("Threshold =", THRESHOLD)

    phi = float(sys.argv[3])
    epsilon = float(sys.argv[4])
    delta = float(sys.argv[5])

    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    global streamLength, sample, approx_frequent_items, approx_Fi
    streamLength = [0]  # Stream length (an array to be passed by reference)
    sample = {}  # Hash Table for the frequent elements
    approx_frequent_items = []
    approx_Fi = 0

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(lambda batch: stickySampling(batch))

    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")

    ssc.stop(False, False)  # False = Stop streaming context but not sparkContext; True = stopGracefully
    print("Streaming engine stopped")

    # COMPUTE AND PRINT FINAL STATISTICS
    print("Number of items processed =", streamLength[0])
    print("Size of S =", len(sample))
    print("S =", sample.items())

    # Compute approx_frequent_items based on the final state of S
    approx_frequent_items = [key for key in sample.keys() if sample[key] >= ((phi - epsilon) * THRESHOLD)]
    approx_Fi = len(approx_frequent_items)
    print("approx =", approx_frequent_items)
    print("frequency_thres",((phi - epsilon) * THRESHOLD))
    print(f"Number of elements in sample = {len(sample)}")