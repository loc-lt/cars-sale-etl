import pyspark
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, max
import sys

# ### receive arguments and initialize some variables
# if len(sys.argv) < 2:
#     print("No, I need two arguments!")
# # get tblName and executionDate
# tblName = ""
# executionDate = ""
# for i in range(1, len(sys.argv), 2):
#     if sys.argv[i] == "--tblName":
#         tblName = sys.argv[i+1]
#     elif sys.argv[i] == "--executionDate":
#         executionDate = sys.argv[i+1]

# get year, month, day from executionDate has format yyyy-mm-dd
runTime = executionDate.split("-")
year = runTime[0]
month = runTime[1]
day = runTime[2]

### create spark session
postgres_data_spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Ingestion - from Postgres to HIVE") \
   .config('spark.driver.extraClassPath', "postgresql-42.6.0.jar") \
   .getOrCreate()

##read table from db using spark jdbc
df = postgres_data_spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/my_company") \
   .option("dbtable", "orders") \
   .option("user", "postgres") \
   .option("password", "loc//14122000") \
   .option("driver", "org.postgresql.Driver") \
   .load()

print(df.show(10))
### get the lastest record_id in datalake

# # conf = SparkConf().setAppName("conf").setMaster("local")
# sc = SparkContext(conf=spark)
# sqlContext = SQLContext(sc)
# exists = sqlContext.read.format('parquet').load(f"hdfs://localhost:9870/datalake/{tblName}")
# print(exists)


# exists =  (f"hdfs://localhost:9870/datalake/{tblName}")
# df = SQLContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/demo/dataset.csv")



# conf = spark.sparkContext._jsc.hadoopConfiguration()
# fs = apache.hadoopFileSystem.get(conf)
# exists = fs.exists(org.apache.hadoop.fs.Path(f"/datalake/{tblName}"))
# tblLocation = f"hdfs://localhost:9000/datalake/{tblName}"
# tblQuery = ""
# if exists:
#     df = spark.read.parquet(tblLocation)
#     record_id = df.agg(max("id")).head()[0]
#     tblQuery = f"(SELECT * FROM `{tblName}` WHERE id > {record_id}) AS tmp"
# else:
#     tblQuery = f"(SELECT * FROM `{tblName}`) AS tmp"

# jdbcDF = spark.read.format("jdbc").options(
#     url="jdbc:mysql://localhost:3306/cong_ty_cua_tui?user=root&password=pa"
# ).load(tblQuery)

# outputDF = jdbcDF.withColumn("year", lit(year)).withColumn("month", lit(month)).withColumn("day", lit(day))
# outputDF.write.partitionBy("year", "month", "day").mode("append").parquet(tblLocation)