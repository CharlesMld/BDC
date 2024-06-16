from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random as rand
import math


# After how many items should we stop?
n = -1 # To be set via command line

def reservoir_sampling(stream, m):
    reservoir=[]
    # Initializing the reservoir with the first m elements from the stream
    for i in range(m):
        reservoir.append(stream[i])
    # print(f"Initial reservoir : ", reservoir)
    # print("\nProcessing the rest of the stream:\n")
    for i in range(m, len(stream)):
        # random index in range [0, i]
        j = rand.randint(0, i)
        # If random index within range of the reservoir size, replace the element j-th element
        if j < m:
            reservoir[j] = stream[i]
    return reservoir

'''
In my program it works but when i plug it in DistinctItems it doesn't work
So i just put the code of the stickySampling function inside process_batch
Now process batch computes exact frequent items and sticky sampling

def stickySampling(batch):
    global streamLength, sample, phi, epsilon, delta, stopping_condition

    batch_size = batch.count()
    if batch_size == 0:
        return
    # Stop the function
    if streamLength[0] >= n: 
        return
    # Increment stream length
    streamLength[0] += batch_size

    # Setting sampling rate
    expr = 1 / (delta * phi)
    r = (math.log(expr)) / epsilon
    s_rate = r / n # s_rate = 0.00016539483766422745
    
    # Perform sampling 
    for item in batch.collect():  # Collect batch elements as a list and iterate
        item_int = int(item)
        p = rand.random()
        if (item_int in sample):
            sample[item_int] = sample[item_int] + 1
        else:
            if p <= s_rate:
                sample[item_int] = 1

    # Stop the stream
    if streamLength[0] >= n:
        stopping_condition.set()
'''
# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    # We are working on the batch at time `time`.
    global streamLength, sample, phi, epsilon, delta, stopping_condition, histogram, all_elements

    batch_size = batch.count()
    # If we already have enough points (> n), skip this batch.
    if streamLength[0]>=n:
        return
    streamLength[0] += batch_size
    # Extract the distinct items from the batch
    batch_items = batch.map(lambda s: (int(s), 1)).reduceByKey(lambda a, b: a + b).collect()
    
    '''
    Exact frequent items

    '''
    # Histogram will contain (item,count)
    for key, count in batch_items:
        if key not in histogram:
            histogram[key] = count
        else:
            histogram[key] += count

   
    '''
    Sticky Sampling
    '''
    # Setting sampling rate
    expr = 1 / (delta * phi)
    r = (math.log(expr)) / epsilon
    s_rate = r / n # s_rate = 0.00016539483766422745
    
    # Perform sampling 
    for item in batch.collect():  # Collect batch elements as a list and iterate
        item_int = int(item)
        p = rand.random()
        if (item_int in sample):
            sample[item_int] = sample[item_int] + 1
        else:
            if p <= s_rate:
                sample[item_int] = 1

    #print(f"current sample = {sample}")
            
    # If we wanted, here we could run some additional code on the global histogram
    if batch_size > 0:
        # print("Batch size at time [{0}] is: {1}".format(time, batch_size))
        batchList = batch.collect()
        batchList = [int(x) for x in batchList]
        all_elements.extend(batchList)
        
        
    if streamLength[0] >= n:
        stopping_condition.set()

if __name__ == '__main__':
    assert len(sys.argv) == 6, "USAGE: port, n, phi, epsilon, delta"

    # IMPORTANT: when running locally, it is *fundamental* that the
    # `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
    # there will be no processor running the streaming computation and your
    # code will crash with an out of memory (because the input keeps accumulating).
    conf = SparkConf().setMaster("local[*]").setAppName("DistinctExample")
    # If you get an OutOfMemory error in the heap consider to increase the
    # executor and drivers heap space with the following lines:
    # conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    
    # Here, with the duration you can control how large to make your batches.
    # Beware that the data generator we are using is very fast, so the suggestion
    # is to use batches of less than a second, otherwise you might exhaust the memory.
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)  # Batch duration of 0.01 seconds
    ssc.sparkContext.setLogLevel("ERROR")
    
    # TECHNICAL DETAIL:
    # The streaming spark context and our code and the tasks that are spawned all
    # work concurrently. To ensure a clean shut down we use this semaphore.
    # The main thread will first acquire the only permit available and then try
    # to acquire another one right after spinning up the streaming computation.
    # The second tentative at acquiring the semaphore will make the main thread
    # wait on the call. Then, in the `foreachRDD` call, when the stopping condition
    # is met we release the semaphore, basically giving "green light" to the main
    # thread to shut down the computation.
    # We cannot call `ssc.stop()` directly in `foreachRDD` because it might lead
    # to deadlocks.
    stopping_condition = threading.Event()
    
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # INPUT READING
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    portExp = int(sys.argv[1])
    n = int(sys.argv[2])
    phi = float(sys.argv[3])
    epsilon = float(sys.argv[4])
    delta = float(sys.argv[5])
    print("INPUT PROPERTIES")
    print(f"n = {n} phi = {phi} epsilon = {epsilon} delta = {delta} port = {portExp}")
        
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    global streamLength, histogram, sample, approx_frequent_items, reservoir
    streamLength = [0] # Stream length (an array to be passed by reference)
    histogram = {} # Hash Table for the distinct elements
    sample = {}  # Hash Table for the sticky frequent elements
    approx_frequent_items = []
    all_elements = []

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    # For each batch, to the following.
    # BEWARE: the `foreachRDD` method has "at least once semantics", meaning
    # that the same data might be processed multiple times in case of failure.
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    # print("Starting streaming engine")
    ssc.start()
    # print("Waiting for shutdown condition")
    stopping_condition.wait()
    # print("Stopping the streaming engine")
    # NOTE: You will see some data being processed even after the
    # shutdown command has been issued: This is because we are asking
    # to stop "gracefully", meaning that any outstanding work
    # will be done.
    ssc.stop(False, True) # False = Stop streaming context but not sparkContext; True = stopGracefully
    # print("Streaming engine stopped")

    # COMPUTE AND PRINT FINAL STATISTICS
    # print("Number of items processed =", streamLength[0])

    print("EXACT ALGORITHM")
    print("Number of items in the data structure =", len(histogram))
    freq = phi * n  # Example threshold
    true_frequent_items = {key: histogram[key] for key in histogram if histogram[key] > freq}
    print("Number of true frequent items =", len(true_frequent_items))
    print("True frequent items:")
    for element in true_frequent_items:
        print(element)

    # print("RESERVOIR SAMPLING")
    # print("Size m of the sample =", int(1/phi))
    # all_elements = all_elements[:n]
    # print("All elements =", len(all_elements))
    # reservoir = reservoir_sampling(all_elements, int(1/phi))    
    # print("Reservoir from reservoir sampling =", reservoir)
    # distinct_items = list(set(reservoir))
    # print("Number of estimated frequent items =", len(distinct_items))
    # print("Estimated frequent items:")
    # distinct_items.sort()
    # for element in distinct_items:
    #     if element in true_frequent_items:
    #         print(element, "+")
    #     else :
    #         print(element, "-")
    # common_items = list(set(true_frequent_items) & set(distinct_items))
    # print("Number of true frequent items produced by RS =", len(common_items))

    print("STICKY SAMPLING")
    print("Number of items in the Hash Table =", len(sample))
    approx_frequent_items = [key for key in sample.keys() if sample[key] >= ((phi - epsilon) * n)]
    print("Number of estimated frequent items =", len(approx_frequent_items))
    approx_frequent_items.sort()
    for element in approx_frequent_items:
        if element in true_frequent_items:
            print(element, "+")
        else :
            print(element, "-")
    common_items = list(set(true_frequent_items) & set(approx_frequent_items))
    print("Number of true frequent items produced by SS =", len(common_items))