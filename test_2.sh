#!/bin/bash

# Define the list of K values
k_list=(50 70 90 110 130)

# Define the output file
output_file="test_2_results.txt"

# Clear the output file
> "$output_file"

# Loop through the K values
for k in "${k_list[@]}"
do
    # Run the Spark command with the current K value
    output=$(spark-submit --num-executors 16 G064HW2.py /data/BDC2324/artificial10M_9_100.csv 3 "$k" 16)

    # Check if the spark-submit command was successful
    if [ $? -eq 0 ]; then
        # Extract the desired information from the output using regular expressions
        radius=$(echo "$output" | grep -oP "Radius = \K\d+\.\d+")
        sure_outliers=$(echo "$output" | grep -oP "Number of sure outliers = \K\d+")
        uncertain_points=$(echo "$output" | grep -oP "Number of uncertain points = \K\d+")

        # Write the extracted information to the output file
        echo "K = $k" >> "$output_file"
        echo "Radius = $radius" >> "$output_file"
        echo "Number of sure outliers = $sure_outliers" >> "$output_file"
        echo "Number of uncertain points = $uncertain_points" >> "$output_file"
        echo "" >> "$output_file"
    else
        echo "Error running spark-submit with K = $k" >&2
    fi
done

echo "Output saved to $output_file"
