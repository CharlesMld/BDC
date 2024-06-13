from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random

# After how many items should we stop?
THRESHOLD = -1 # To be set via command line
reservoir = []

def reservoir_sampling(stream, m):
    # Initializing the reservoir with the first m elements from the stream
    for i in range(m):
        reservoir.append(stream[i])
    print(f"Initial reservoir : ", reservoir)
    print("\nProcessing the rest of the stream:\n")
    for i in range(m, len(stream)):
        # random index in range [0, i]
        j = random.randint(0, i)
        # If random index within range of the reservoir size, replace the element j-th element
        if j < m:
            print(f"Element {stream[i]} at index {i} replaces reservoir element at index {j} ({reservoir[j]})")
            reservoir[j] = stream[i]
    return reservoir

# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    # We are working on the batch at time `time`.
    global streamLength, histogram
    batch_size = batch.count()
    # If we already have enough points (> THRESHOLD), skip this batch.
    if streamLength[0]>=THRESHOLD:
        return
    streamLength[0] += batch_size
    # Extract the distinct items from the batch
    batch_items = batch.map(lambda s: (int(s), 1)).reduceByKey(lambda i1, i2: 1).collectAsMap()

    # Update the streaming state
    for key in batch_items:
        if key not in histogram:
            histogram[key] = 1
            
    # If we wanted, here we could run some additional code on the global histogram
    if batch_size > 0:
        print("Batch size at time [{0}] is: {1}".format(time, batch_size))
        batchList = batch.collect()
        batchList = [int(x) for x in batchList]
        print("Int batch list : ", batchList)
        reservoir = reservoir_sampling(batchList, 20)

    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()
        


if __name__ == '__main__':
    assert len(sys.argv) == 3, "USAGE: port, threshold"

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
    print("Receiving data from port =", portExp)
    
    THRESHOLD = int(sys.argv[2])
    print("Threshold = ", THRESHOLD)
        
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    streamLength = [0] # Stream length (an array to be passed by reference)
    histogram = {} # Hash Table for the distinct elements
    

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    # For each batch, to the following.
    # BEWARE: the `foreachRDD` method has "at least once semantics", meaning
    # that the same data might be processed multiple times in case of failure.
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    # NOTE: You will see some data being processed even after the
    # shutdown command has been issued: This is because we are asking
    # to stop "gracefully", meaning that any outstanding work
    # will be done.
    ssc.stop(False, True) # False = Stop streaming context but not sparkContext; True = stopGracefully
    print("Streaming engine stopped")

    # COMPUTE AND PRINT FINAL STATISTICS
    print("Number of items processed =", streamLength[0])
    print("Number of distinct items =", len(histogram))
    print("Final sample of 20 items by Reservoir Sampling: ", reservoir)
    largest_item = max(histogram.keys())
    print("Largest item =", largest_item)