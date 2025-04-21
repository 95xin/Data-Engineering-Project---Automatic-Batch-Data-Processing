from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year

spark = SparkSession.builder.appName("CleanTaxiData").getOrCreate()

# Read raw data
df = spark.read.parquet("/opt/airflow/lab/yellow_tripdata_2025-01.parquet")


df_cleaned = df.dropna().dropDuplicates()

# Add partition fields (like partitioning by month)
df_cleaned = df_cleaned.withColumn("pickup_month", month(col("tpep_pickup_datetime")))

# Write out as Parquet partitioned by month (recommended for large datasets)
# df_cleaned.write.partitionBy("pickup_month").mode("overwrite").parquet("/opt/airflow/lab/cleaned_yellow_tripdata_2025_01_parquet")

# Output as CSV (for small datasets, coalesce(1) merges into a single file)
df_cleaned.coalesce(1).write.mode("overwrite").csv("/opt/airflow/lab/cleaned_yellow_tripdata_2025-01.csv", header=True)

spark.stop()