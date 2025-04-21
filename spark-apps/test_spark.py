from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("TestSparkJob") \
        .getOrCreate()

    # Create test data
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["name", "value"])
    
    # Display data
    df.show()
    
    # Save results
    df.write.mode("overwrite").csv("/opt/spark/data/test_output")
    
    spark.stop()

if __name__ == "__main__":
    main()