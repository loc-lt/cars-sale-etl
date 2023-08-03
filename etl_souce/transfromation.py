import pyspark.sql.functions as f
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext, HiveContext

# Get executionDate to transformation data from HDFS Data Lake to Data Warehouse
executionDate = input("Input date you want transform data from HDFS DataLake and save to Hive Storage: ")

# Get year, month, day variable from executionDate
runTime = executionDate.split("-")
year = runTime[0]
month = runTime[1]
day = runTime[2]

# Create spark session
spark = SparkSession \
   .builder \
   .appName("Daily Gross Revenue Report") \
   .config('hive.exec.dynamic.partition', 'true') \
   .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
   .config('spark.sql.warehouse.dir', 'hdfs://localhost:9000/user/hive/warehouse') \
   .enableHiveSupport() \
   .getOrCreate()

# Load data to spark df
orders_df = spark.read.parquet('hdfs://localhost:9000/datalake/orders').drop("year", "month", "day")
order_detail_df = spark.read.parquet('hdfs://localhost:9000/datalake/order_detail').drop("year", "month", "day")
products_df = spark.read.parquet('hdfs://localhost:9000/datalake/products').drop("year", "month", "day", "created_at")
inventory_df = spark.read.parquet('hdfs://localhost:9000/datalake/inventory').drop("year", "month", "day")

# Join dataframe
pre_df = orders_df \
    .filter(orders_df["created_at"] == executionDate) \
    .join(order_detail_df, orders_df["id"] == order_detail_df["order_id"], "inner") \
    .join(products_df, orders_df["product_id"] == products_df["id"], "inner") \
    .join(inventory_df.select(f.col("quantity").alias("inv_quantity"), f.col("id")), products_df["inventory_id"] == inventory_df["id"], "inner")

# Aggreate data
map_df = pre_df.groupBy("Make", "Model", "Category", "product_id", "inv_quantity") \
    .agg(
        f.sum("quantity").alias("Sales"),
        f.sum("total").alias("Revenue")
    )

# Prepare results
result_df = map_df \
    .withColumn("LetfOver", f.col("inv_quantity") - f.col("Sales")) \
    .withColumn("year", f.lit(year)) \
    .withColumn("month", f.lit(month)) \
    .withColumn("day", f.lit(day)) \
    .select("Make", "Model", "Category", "Sales", "Revenue", "year", "month", "day", "LetfOver")

# Write to Data Warehouse
result_df.write \
    .format("hive") \
    .partitionBy("year", "month", "day") \
    .mode("append") \
    .saveAsTable("reports.daily_gross_revenue")