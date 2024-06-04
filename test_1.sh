#!/bin/bash

# Define the list of num-executors values
num_executors_list=(2 4 8 16)

# Define the output file
output_file="test_1_results.txt"

# Clear the output file
> "$output_file"

# Loop through the num-executors values
for num_executors in "${num_executors_list[@]}"
do
    # Run the Spark command with the current num-executors value
    output=$(spark-submit --num-executors "$num_executors" G064HW2.py /data/BDC2324/artificial100M_9_100.csv 10 110 16)

    # Check if the spark-submit command was successful
    if [ $? -eq 0 ]; then
        # Extract the desired information from the output using regular expressions
        round1_time=$(echo "$output" | grep -oP "Round 1 = \K\d+")
        round2_time=$(echo "$output" | grep -oP "Round 2 = \K\d+")
        round3_time=$(echo "$output" | grep -oP "Round 3 = \K\d+")
        approx_outliers_time=$(echo "$output" | grep -oP "MRApproxOutliers = \K\d+")

        # Write the extracted information to the output file
        echo "num-executors = $num_executors" >> "$output_file"
        echo "Round 1 = $round1_time ms" >> "$output_file"
        echo "Round 2 = $round2_time ms" >> "$output_file"
        echo "Round 3 = $round3_time ms" >> "$output_file"
        echo "MRApproxOutliers = $approx_outliers_time ms" >> "$output_file"
        echo "" >> "$output_file"
    else
        echo "Error running spark-submit with num-executors = $num_executors" >&2
    fi
done

echo "Output saved to $output_file"
