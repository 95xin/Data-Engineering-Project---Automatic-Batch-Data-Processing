from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year

spark = SparkSession.builder.appName("CleanTaxiData").getOrCreate()

# 读取原始数据
df = spark.read.parquet("/opt/airflow/lab/yellow_tripdata_2025-01.parquet")


df_cleaned = df.dropna().dropDuplicates()

# 增加分区字段（如按月份分区）
df_cleaned = df_cleaned.withColumn("pickup_month", month(col("tpep_pickup_datetime")))

# 按月分区写出为Parquet（推荐大数据量时）
# df_cleaned.write.partitionBy("pickup_month").mode("overwrite").parquet("/opt/airflow/lab/cleaned_yellow_tripdata_2025_01_parquet")

# 输出为CSV（小数据量时，coalesce(1)合并为单文件）
df_cleaned.coalesce(1).write.mode("overwrite").csv("/opt/airflow/lab/cleaned_yellow_tripdata_2025-01.csv", header=True)

spark.stop()