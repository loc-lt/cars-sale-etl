import pyspark
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, max
import sys
import subprocess

# Receive 2 arguments: tblName, executionDate
tblName = input("Input table name from PostgreSQL which load to HDFS: ") 
executionDate = input("Input date you want ingest data from PostgreSQL to HDFS DataLake: ")

runTime = executionDate.split("-")
year = runTime[0]
month = runTime[1]
day = runTime[2]

# Create spark session
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Ingestion - from Postgres to HDFS") \
   .config('spark.driver.extraClassPath', "/postgre_driver_file/postgresql-42.6.0.jar") \
   .getOrCreate()

# Read table from db using spark jdbc
df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/my_company") \
   .option("dbtable", tblName) \
   .option("user", "postgres") \
   .option("password", "loc//14122000") \
   .option("driver", "org.postgresql.Driver") \
   .load()

# Function to interact with hdfs storage
def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err

tblLocation = f'hdfs://localhost:9000/datalake/{tblName}'

# Check whether folder exist of not
(ret, out, err) = run_cmd(['hdfs', 'dfs', '-du', '-s', tblLocation])
exists = True if len(str(out).split()) > 1 else False

tblQuery = ""
if exists:
    datalake_df = spark.read.format('parquet').load(tblLocation)
    record_id = datalake_df.agg(max("id")).head()[0]
    tblQuery = f"SELECT * FROM {tblName} WHERE id > {record_id} AS tmp"
else:
    tblQuery = f"SELECT * FROM {tblName} AS tmp"

# Get the lastest records in PostgreSQL
jdbc_df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/my_company") \
   .option("dbtable", tblName) \
   .option("user", "postgres") \
   .option("password", "loc//14122000") \
   .option("driver", "org.postgresql.Driver") \
   .load(tblQuery)

# Append records in PostreSQL from lastest record_id in HDFS Datalake
output_df = jdbc_df.withColumn("year", lit(year)).withColumn("month", lit(month)).withColumn("day", lit(day))
output_df.write.partitionBy("year", "month", "day").mode("append").parquet(tblLocation)