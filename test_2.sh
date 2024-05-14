#!/bin/bash

# Define list of K values
K_list=(50 70 90 110 130)

# Loop through each K value
for K_value in "${K_list[@]}"
do
    # Execute spark-submit command with the given K value
    echo "Executing with K = $K_value"
    spark-submit --num-executors 16 G064HW2.py /data/BDC2324/artificial10M_9_100.csv 3 $K_value 16 > output_$K_value.txt
    
    # Extract required information and save it in a separate text file
    echo "K = $K_value" >> test2.txt
    grep -E "Radius =|Number of sure outliers =|Number of uncertain points =" output_$K_value.txt >> test2.txt
    echo "" >> test2.txt
done
