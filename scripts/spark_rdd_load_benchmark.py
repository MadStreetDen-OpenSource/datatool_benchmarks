from pyspark.context import SparkContext
from pyspark.sql import HiveContext
#from SparkJob import *
#from log_parser import *
import string
import os
import random
import sys
import traceback
import csv
import re
import operator
import time
from pyspark.sql.functions import udf, size, col
from string import digits
from pyspark.sql import functions as F

spark_context = SparkContext(appName='benchmark')
hive_context = HiveContext(spark_context)


def run_benchmarks(base_path):
    print("=========================================================================================")
    print("Loading data for: ")
    print(base_path)
    print("=========================================================================================")

    print("Time taken for loading RDD: ")
    start=time.time()
    rdd_raw = spark_context.textFile(base_path)
    rdd_with_headers = rdd_raw.mapPartitions(lambda x: csv.reader(x))
    header = rdd_with_headers.first() #extract header
    rdd = rdd_with_headers.filter(lambda x:x !=header)
    #header = rdd_with_headers.first
    #rdd = rdd_with_headers.zipWithIndex().filter(lambda (row,index): index > 0).keys()
    #rdd=df.rdd

    start_task=time.time()
    print(rdd.count())
    end_task=time.time()
    end=time.time()
    x=[base_path, end-start, end_task-start_task]
    print("=========================================================================================")
    print("OUTPUT")
    print(x)
    print("=========================================================================================")
    return x

def main():    
    # hiding logs
    log4j = spark_context._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    list = [ 100000, 1000000, 10000000, 50000000, 100000000]
    base_path = "s3://bucket-path/folder-path/"
    final_op=[]
    for x in list:
        path=base_path+str(x)+"/"
        print(path)
        op=run_benchmarks(path)
        final_op=final_op+[op]
    print(final_op)
    rdd_op=spark_context.parallelize(final_op)
    schema=['dataset','time1','time2']
    df_op=rdd_op.toDF(schema)
    df_op.coalesce(1).write.format('com.databricks.spark.csv').option("header","true").save("s3://bucket-name/folder-name/op_rdd_load")
 
if __name__ == "__main__":
    main()

# running instructions:
# spark-submit --packages com.databricks:spark-csv_2.11:1.2.0 gen_random.py
