# Group notes
The *threshold* parameter refers to the number of elements after which we should stop the streaming. Therefore, we notice that if we define *threshold* $<$ minimum batch size, the program will be forced to process 1 entire batch which is 360 elements. The same if we set *threshold*=361, the program will be forced to process 2 batches $\Rightarrow$ 720 elements


# Official assignment
In this homework, you will use the Spark Streaming API to devise a program which processes a stream of items and compares the effectiveness of two methods to identify frequent items: 
1. the method based on reservoir sampling
2. the method based on sticky sampling

## Spark streaming setting that will be used for the homework

Server (which generates continuous integers) has been already activated on the machine **algo.dei.unipd.it** and emits the items (viewed as strings) on specific **ports (from 8886 or 8889)**. Your program must first define a **Spark Streaming Context sc** that provides access to the stream through the method **socketTextStream** which transforms the input stream, coming from the specified machine and port number, into a _Discretized Stream_ (**DStream**) of **batches of items**. A batch consists of the items arrived during a time interval whose duration is specified at the creation of the context sc. 

**Each batch is viewed as an RDD of strings**, and a set of RDD methods are available to process it. A method **foreachRDD** is then invoked to process the batches one after the other. Typically, the processing of a batch entails the update of some data structures stored in the driver's local space (i.e., its working memory) which are needed to perform the required analysis. The beginning/end of the stream processing will be set by invoking **start/stop** methods from the context sc. Typically, the stop command is invoked after the desired number of items is processed (with some tolerance, given that items are processed in batches, and the size of each batch cannot be tightly controlled).  
  
To learn more about Spark Streaming you may refer to the official Spark site. Relevant links are:  

- [Spark Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html) (full documentation)
- [Transformations on Streams](https://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations-on-dstreams) (list of transformations applicable to the RDDs in a DStream)  

## Running the program and template

Your program will be run in local mode on your PC, exactly as the one devised for Homework 1. The **master should be set to local[*]**.  
  
In order to see a concrete application of the above setting you can download and run the following **example program** which takes as input the port number (_port_) and the number of elements (_threshold_) to be processed, and computes the exact number of distinct elements among the first _threshold_ elements of the stream coming from port number _port_:
**Python version** [DistinctItemsExample.py](https://esami.elearning.unipd.it/pluginfile.php/463565/mod_page/content/53/DistinctItemsExample.py?time=1716885849540)

## TASK for HW3

You must write a program **GxxxHW3.java** (for Java users) or **GxxxHW3.py** (for Python users), where xxx is your 3-digit group number (e.g., 004 or 045), which receives in input the following **5 command-line arguments (in the given order):**  

- **An integer** n: the number of items of the stream to be processed
- **A float** _$\phi$_: the frequency thresold in [0,1] 
- **A float** $\epsilon$: the accuracy parameter in [0,1]
- **A float** $\delta$: the confidence parameter in [0,1]
- **An integer** _portExp_: the port number  

The program must process all items in the batches up to and including the first batch which contains the n-th item of the stream Σ emitted by **machine algo.dei.unipd.it** at port _portExp_, and it must compute the following information:

- The true frequent items with respect to the threshold _phi_
- An _m_-sample of $\Sigma$ using **Reservoir Sampling** with m = ⌈1/$\phi$⌉
- The **_epsilon_-Approximate Frequent Items** computed using **Sticky Sampling** with confidence parameter _delta_

If some action must be performed with a probability p, generate a random number in x$\in$[0,1] and perform the action only if $x\leq p$. Use the random generators provided by Python.  

The program should print:
- The input parameters provided as command-line arguments  
- The number of true frequent items
- The true frequent items, in increasing order (one item per line)  
- All items in the sample computed with Reservoir Sampling, in increasing order (one item per line)
- The number of items in the hash table used by Sticky Sampling.
- The _epsilon_-Approximate Frequent Items computed by Sticky Sampling, in increasing order (one item per line)  

THIS FILE (**TO BE ADDED**) shows how to format your output. Make sure that your program complies with the input and output format.  
  
**The program that you submit should run without requiring additional files.** Test your program on your local or virtual machine using various configurations of parameters, and **report your results using the table given in** THIS WORD FILE (**TO BE ADDED**).  

#### PORTS IN algo.dei.unipd.it
The ports from 8886 to 8889 of algo.dei.unipd.it generate four streams of 32-bit integers:
- 8887: it generates a stream where a few elements are very frequent, while all the remaining are randomly selected in the 32-bit integer domain.
- 8889: it generates a stream where a few elements are very frequent, some elements are moderately frequent, and all the remaining are randomly selected in the 32-bit integer domain.  
- 8886: it is the "deterministic" version of the stream 8887, meaning that it generates the exact same stream every time you connect to this port. It should be used to test your algorithm.
- 8888: it is the "deterministic" version of the stream 8889, meaning that it generates the exact same stream every time you connect to this port. It should be used to test your algorithm.

**SUBMISSION INSTRUCTIONS.** Each group must submit a zipped folder GxxxHW3.zip, where xxx is your group number. The folder must contain the program (GxxxHW3.java or GxxxHW3.py) and a file GxxxHW3table.docx with the aforementioned table. Only one student per group must do the submission using the link provided in the Homework 3 section. Make sure that your code is free from compiling/run-time errors and that you comply with the specification, otherwise your grade will be penalized.  
  
If you have questions about the assignment, contact the teaching assistants (TAs) by email to bdc-course@dei.unipd.it . The subject of the email must be "HW3 - Group xxx", where xxx is your group number. If needed, a zoom meeting between the TAs and the group will be organized.