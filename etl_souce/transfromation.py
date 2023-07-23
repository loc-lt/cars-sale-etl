##import required libraries
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

# # get year, month, day from executionDate has format yyyy-mm-dd
# runTime = executionDate.split("-")
# year = runTime[0]
# month = runTime[1]
# day = runTime[2]

##create spark session
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', "postgresql-42.6.0.jar") \
   .getOrCreate()

##read table from db using spark jdbc
movies_df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/my_company") \
   .option("dbtable", "orders") \
   .option("user", "postgres") \
   .option("password", "loc//14122000") \
   .option("driver", "org.postgresql.Driver") \
   .load()

##print the movies_df
# print(tblName)
print(movies_df.show(10))
# print(1)