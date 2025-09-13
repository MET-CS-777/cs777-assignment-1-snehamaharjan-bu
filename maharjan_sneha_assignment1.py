from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 17 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11]) and isfloat(p[4]) and isfloat(p[16])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: assignment1.py <input_file> <task1_output> <task2_output>")
        sys.exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    #creating the RDD
    rdd = sc.textFile(sys.argv[1])
    
    # Split each line into list of strings (comma separated) that are filtered based on the correctRows cleaning function
    rows = rdd.map(lambda line: line.split(",")).filter(correctRows)

    #Task 1

    #forming (medallion, driver) pairs
    medallion_driver = rows.map(lambda p: (p[0], p[1]))
    
    #only extract distinct pairs
    distinct_medallion_driver_pairs = medallion_driver.distinct()

    #count unique drivers for each medallion- add up by medallion as key
    medallion_driver_count = distinct_medallion_driver_pairs.map(lambda x: (x[0], 1)).reduceByKey(add)
    
    #get top 10 most driven taxis
    top10_taxis = medallion_driver_count.takeOrdered(10, key=lambda kv: (-kv[1], kv[0]))

    #take top 10 list and distribute it across the cluster as an RDD
    results_1 = sc.parallelize(top10_taxis)
    
    #save results

    results_1.coalesce(1).saveAsTextFile(sys.argv[2])

##########################################################

    #Task 2
    
    #get the total trip time and total income for each driver, making driver the key
    driver_income_time = rows.map(lambda p: (p[1], (float(p[16]), float(p[4]))))
    
    #grouping all records by the key driver ID to get each driver's total driving time and total income
    #for multiple instances of the same driver ID key, summing the time and income fields
    driver_final = driver_income_time.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    
    
    #filter to make sure total trip time for a driver is valid or greater than 0,
    #then calculate each driver key's income earned per minute
    driver_income_per_min = driver_final.filter(lambda kv: kv[1][1] > 0).mapValues(lambda s: s[0] / (s[1] / 60.0)).filter(lambda kv: kv[1] <= 10.0)


    # get the top 10 drivers based on income per minute
    top10_drivers = driver_income_per_min.takeOrdered(10, key=lambda kv: (-kv[1], kv[0]))


    #take top 10 list and distribute it across the cluster as an RDD, then save results
    results_2 = sc.parallelize(top10_drivers)
    results_2.coalesce(1).saveAsTextFile(sys.argv[3])

    sc.stop()