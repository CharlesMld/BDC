#!/bin/bash

# Define list of num-executors values
num_executors_list=(2 4 8 16)

# Loop through each num-executors value
for num_executors in "${num_executors_list[@]}"
do
    # Execute spark-submit command with the given num-executors value
    echo "Executing with num-executors = $num_executors"
    spark-submit --num-executors $num_executors G064HW2.py /data/BDC2324/artificial100M_9_100.csv 10 110 16 > output_$num_executors.txt
    
    # Extract required information and save it in a separate text file
    echo "num-executors = $num_executors" >> test1.txt
    grep -E "Round [123] =|MRApproxOutliers =" output_$num_executors.txt >> test1.txt
    echo "" >> test1.txt
done
