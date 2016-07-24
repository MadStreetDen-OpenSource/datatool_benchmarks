from pyspark.context import SparkContext
from pyspark.sql import HiveContext
#from SparkJob import *
#from log_parser import *
import string
import os
import random
import sys
import traceback
import re
import operator
import time
from pyspark.sql.functions import udf, size, col
from string import digits
from pyspark.sql import functions as F

spark_context = SparkContext(appName='benchmark')
hive_context = HiveContext(spark_context)


def run_benchmarks(base_path,base_path2):
    print("=========================================================================================")
    print("Loading data for: ")
    print(base_path)
    print("=========================================================================================")

    print("Time taken for loading RDD 1: ")
    start=time.time()
    import csv
    rdd_raw = spark_context.textFile(base_path)
    rdd_with_headers = rdd_raw.mapPartitions(lambda x: csv.reader(x))
    header = rdd_with_headers.first() #extract header
    rdd = rdd_with_headers.filter(lambda x:x !=header)
    #header = rdd_with_headers.first
    #rdd = rdd_with_headers.zipWithIndex().filter(lambda (row,index): index > 0).keys()
    #rdd=df.rdd
    print(rdd.count())

    print("Time taken for loading RDD 2: ")
    import csv
    rdd_raw2 = spark_context.textFile(base_path2)
    rdd_with_headers2 = rdd_raw2.mapPartitions(lambda x: csv.reader(x))
    header2 = rdd_with_headers2.first() #extract header
    rdd2 = rdd_with_headers2.filter(lambda x:x !=header)
    #header = rdd_with_headers.first
    #rdd = rdd_with_headers.zipWithIndex().filter(lambda (row,index): index > 0).keys()
    #rdd=df.rdd
    print(rdd2.count())

    print("Time taken for RDD join: ")
    start_task=time.time()
    rdd_1=rdd.map(lambda x: (x[3], x))
    rdd_2=rdd2.map(lambda x: (x[3], x))
    joined_rdd=rdd_1.join(rdd_2)
    #print(joined_rdd.take(20))
    #joined_rdd2=joined_rdd.map(lambda x: x[1:])
    #print(joined_rdd.take(20))
    print(joined_rdd.count())
    end_task=time.time()
    end=time.time()
    x=[base_path, end-start, end_task-start_task]
    print(joined_rdd.take(2))
    print("=========================================================================================")
    print("OUTPUT")
    print(x)
    print("=========================================================================================")
    return x

def main():    
    # hiding logs
    log4j = spark_context._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    list = [100000, 1000000, 10000000, 50000000, 100000000]#[1000,1000,100000,1000000,100000000]
    #list = [1000,1000,100000,1000000,100000000]
    base_path = "s3://bucket-name/folder-name/"
    base_path2 = "s3://bucket-name/folder-name/merge/"
    final_op=[]
    for x in list:
        path=base_path+str(x)+"/"
        print(path)
        op=run_benchmarks(path, base_path2)
        final_op=final_op+[op]
    print(final_op)
    rdd_op=spark_context.parallelize(final_op)
    schema=['dataset','time1','time2']
    df_op=rdd_op.toDF(schema)
    df_op.coalesce(1).write.format('com.databricks.spark.csv').option("header","true").save("s3://bucket-name/folder-name/op_rdd_join_D")

 
if __name__ == "__main__":
    main()

# running instructions:
# spark-submit --packages com.databricks:spark-csv_2.11:1.2.0 gen_random.py
