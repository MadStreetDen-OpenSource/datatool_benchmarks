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
import csv
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

    #print("Time taken for loading DataFrame:")
    start=time.time()

    hive_context.sql('DROP TABLE IF EXISTS temp')
    query = "CREATE EXTERNAL TABLE temp(id int, A double, B double, C varchar(10), D int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' LOCATION \"" + base_path + "\""
    hive_context.sql(query)
    #df = hive_context.sql('select * from temp')

    print("Time taken for groupby d and sum on ID:")
    start_task=time.time()

    ## groupby
    #grouped_df = df.groupBy('d').sum('id')
    grouped_df = hive_context.sql('SELECT d, COUNT(id) FROM temp GROUP BY d')
    print(grouped_df.count())

    end_task=time.time()
    end=time.time()
    x=[base_path, end-start, end_task-start_task]
    #print(joined_df.show())
    print("=========================================================================================")
    print("OUTPUT")
    print(x)
    print("=========================================================================================")
    return x

def main():
    # hiding logs
    log4j = spark_context._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    list = [100000, 1000000, 10000000, 50000000, 100000000]
    base_path = "s3://bucket-name/folder-name/"
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
    df_op.coalesce(1).write.format('com.databricks.spark.csv').option("header","true").save("s3://bucket-name/folder-name/op_table_groupby_d")

if __name__ == "__main__":
    main()

# running instructions:
# spark-submit --packages com.databricks:spark-csv_2.11:1.2.0 gen_random.py
