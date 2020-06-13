#!/bin/sh

# Author: Jerry Zhu
# CS236 UCR Spring 2020 project

echo "Running Project with arguments:"
echo "Location file folder: " $1
echo "Recording file folder: " $2
echo "Output folder: " $3

spark-submit --class cs236 --master local cs236_2.11-1.0.jar $1 $2 $3