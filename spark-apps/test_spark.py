from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("TestSparkJob") \
        .getOrCreate()

    # 创建测试数据
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["name", "value"])
    
    # 显示数据
    df.show()
    
    # 保存结果
    df.write.mode("overwrite").csv("/opt/spark/data/test_output")
    
    spark.stop()

if __name__ == "__main__":
    main()