from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import math 
import random as rand

# After how many items should we stop?
THRESHOLD = -1 # To be set via command line


# Operations to perform after receiving an RDD 'batch' at time 'time'
def stickySampling(time,batch):
    # Useful vars
    global streamLength,S,phi,epsilon,delta
    batch_size = batch.count()

    # Stop the function
    if streamLength[0] >= THRESHOLD:
        return
    # Increment stream length
    streamLength[0] += batch_size
    

    '''Algorithm implementation'''
    # Setting sampling rate
    expr = 1/(delta*phi)
    r = (math.log(expr))/(epsilon)
    s_rate = r/THRESHOLD

    # I need key value pairs but
    # I should not doo this initialization
    # The stream should be processed as it comes
    batch_items = batch.map(lambda x: (int(x) , 1)).collectAsMap()

    for key in batch_items:
        if key in S:
            S[key] += S.get(key,0)
        else:
            p = rand.random()
            if (p <= s_rate):
                S[key] = 1

    # Print the frequent items
    approx_frequent_items = []
    for key in S:
        if S.get(key) >= ((phi - epsilon)*THRESHOLD):
            approx_frequent_items.append(key)
        

    # Stop the stream
    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()








    
        


if __name__ == '__main__':
    assert len(sys.argv) == 6, "USAGE: port, threshold,phi,epsilon,delta,"

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

    phi = float(sys.argv[3])
    epsilon = float(sys.argv[4])
    delta = float(sys.argv[5])
        
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    streamLength = [0] # Stream length (an array to be passed by reference)
    S = {} # Hash Table for the distinct elements
    

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    
    stream.foreachRDD(lambda time, batch: stickySampling(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")

    ssc.stop(False, True) # False = Stop streaming context but not sparkContext; True = stopGracefully
    print("Streaming engine stopped")

    # COMPUTE AND PRINT FINAL STATISTICS
    print("Number of items processed =", streamLength[0])
    print("Number of distinct items =", len(S))
    largest_item = max(S.keys())
    print("Largest item =", largest_item)
    
