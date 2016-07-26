import psycopg2 
import timeit
import pandas as pd

conn_string = "dbname='<fill dbname>' port='<fill port>' user='<fill username>' password='<fill password>' host='<fill host>' "
conn = psycopg2.connect(conn_string)
cursor=conn.cursor()

df=pd.DataFrame(columns=['size','table_a_load_time','table_b_load_time','table_a_sort_time_C','table_a_sort_time_D','join_time_C','join_time_D','aggregation_time_C','aggregation_time_D'])

data_size=[100000, 1000000, 10000000, 50000000, 100000000]

index=0
print "Starting script"
for i in data_size:
        print i 
	cursor.execute("drop table if exists dataset_a")
	cursor.execute("drop table if exists dataset_b")
	

	#Create Table A
	cursor.execute('create table dataset_a(id int, A double precision,B double precision,C varchar(max),D int)')

	#Create Table B
	cursor.execute('create table dataset_b(id int, A double precision,B double precision,C varchar(max),D int)')

	copy_location='s3://bucket-name/folder-name/'+str(i)+'/' 
        print "load data to table A"
	#Load Data into Dataset A
	dataset_a_load_start_time=timeit.default_timer()
	
	cursor.execute("copy dataset_a \
	from '{}' \
	credentials 'aws_access_key_id=<fill access key>;aws_secret_access_key=<fill secret key>' \
	csv \
	IGNOREHEADER as 1 \
	region 'us-west-2'".format(copy_location))
	
	dataset_a_load_time=str(timeit.default_timer()-dataset_a_load_start_time)

	df.set_value(index,'table_a_load_time',dataset_a_load_time)


   
        print "load data to table B"
	#Load Data into Dataset B
	dataset_b_load_start_time=timeit.default_timer()

	cursor.execute("copy dataset_b \
	from 's3://bucket-name/folder-name/merge/' \
	credentials 'aws_access_key_id=<fill access key>;aws_secret_access_key=<fill secret key>' \
	csv \
	IGNOREHEADER as 1 \
	region 'us-west-2'")

	dataset_b_load_time=str(timeit.default_timer()-dataset_b_load_start_time)
	
	df.set_value(index,'table_b_load_time',dataset_b_load_time)



        print "Join by C"

	#Join the dataset

	cursor.execute("drop table if exists joined_a_b_C")
	dataset_join_start_time_C=timeit.default_timer()

	cursor.execute("create temp table joined_a_b_C as \
	(select a.*,b.id as id2,b.A as A2,b.B as B2,b.D as D2 from dataset_a a inner join dataset_b b on a.C=b.C)")
	
	dataset_join_time_C=str(timeit.default_timer()-dataset_join_start_time_C)
	
	cursor.execute("drop table if exists joined_a_b_D")
	dataset_join_start_time_D=timeit.default_timer()

	cursor.execute("create temp table joined_a_b_D as \
	(select a.*,b.id as id2,b.A as A2,b.B as B2,b.c as C2 from dataset_a a inner join dataset_b b on a.D=b.D)")

	dataset_join_time_D=str(timeit.default_timer()-dataset_join_start_time_D)

	df.set_value(index,'join_time_C',dataset_join_time_C)
	
	df.set_value(index,'join_time_D',dataset_join_time_D)
	

	#Order the dataset by C,D

        cursor.execute("drop table if exists order_C")
	cursor.execute("drop table if exists order_D")

	dataset_a_sort_start_time_C=timeit.default_timer()
	cursor.execute("create temp table order_C as (select * from dataset_a order by C)")
	dataset_a_order_time_C=str(timeit.default_timer()-dataset_a_sort_start_time_C)

	dataset_a_sort_start_time_D=timeit.default_timer()
	cursor.execute("create temp table order_D as (select * from dataset_a order by D)")
	dataset_a_order_time_D=str(timeit.default_timer()-dataset_a_sort_start_time_D)

	df.set_value(index,'table_a_sort_time_C',dataset_a_order_time_C)
	df.set_value(index,'table_a_sort_time_D',dataset_a_order_time_D)



	#Aggregate the dataset

	cursor.execute("drop table if exists aggregate_a_b_C")
	cursor.execute("drop table if exists aggregate_a_b_D")

	dataset_aggregate_start_time_C=timeit.default_timer()
	cursor.execute("create temp table aggregate_a_b_C as (select C,sum(id) from dataset_a group by 1)")
	dataset_aggregate_time_C=str(timeit.default_timer()-dataset_aggregate_start_time_C)
	df.set_value(index,'aggregation_time_C',dataset_aggregate_time_C)

	dataset_aggregate_start_time_D=timeit.default_timer()
	cursor.execute("create temp table aggregate_a_b_D as (select D,sum(id) from dataset_a group by 1)")
	dataset_aggregate_time_D=str(timeit.default_timer()-dataset_aggregate_start_time_D)

	df.set_value(index,'aggregation_time_D',dataset_aggregate_time_D)

	index=index+1
	conn.commit()	
	print df.head()

df.to_csv('redshift_numbers.csv')
