from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import requests
import pandas as pd
import glob
from sqlalchemy import create_engine, text
from google.cloud import bigquery

# -------------------
# Python functions
# -------------------
# get folder path
csv_files = glob.glob("/opt/airflow/lab/cleaned_yellow_tripdata_2025-01.csv/*.csv")

def download_nyc_data():
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
    output_path = "/opt/airflow/lab/yellow_tripdata_2025-01.parquet"
    response = requests.get(url)
    with open(output_path, "wb") as f:
        f.write(response.content)
    print("下载完成！")


def create_postgres_table():
    engine = create_engine("postgresql+psycopg2://root:root@pgdatabase:5432/project1")
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS yellow_trips_2025_01 (
        id SERIAL PRIMARY KEY,
        "VendorID" INTEGER,
        "tpep_pickup_datetime" TIMESTAMP,
        "tpep_dropoff_datetime" TIMESTAMP,
        "passenger_count" INTEGER,
        "trip_distance" FLOAT,
        "RatecodeID" INTEGER,
        "store_and_fwd_flag" BOOLEAN,
        "PULocationID" INTEGER,
        "DOLocationID" INTEGER,
        "payment_type" INTEGER,
        "fare_amount" FLOAT,
        "extra" FLOAT,
        "mta_tax" FLOAT,
        "tip_amount" FLOAT,
        "tolls_amount" FLOAT,
        "improvement_surcharge" FLOAT,
        "total_amount" FLOAT,
        "congestion_surcharge" FLOAT,
        "Airport_fee" FLOAT,
        "pickup_month" INTEGER
);
        """))
        # 日期字段加索引
        conn.execute(text("""CREATE INDEX IF NOT EXISTS idx_pickup_datetime ON yellow_trips_2025_01 USING BTREE ("tpep_pickup_datetime");
                            """))
        conn.execute(text("""CREATE INDEX IF NOT EXISTS idx_dropoff_datetime ON yellow_trips_2025_01 USING BTREE ("tpep_dropoff_datetime");"""))
        print("表结构和索引已创建")


def write_to_postgres():
    create_postgres_table()  # 先建表
    engine = create_engine("postgresql+psycopg2://root:root@pgdatabase:5432/project1")
    chunksize = 100000
    if not csv_files:
        raise FileNotFoundError("未找到CSV文件")
    csv_file = csv_files[0]

    df_iter = pd.read_csv(
        csv_file,
        chunksize=chunksize,
        dtype={"tpep_pickup_datetime": str, "tpep_dropoff_datetime": str},
        iterator=True
    )

    chunk_index = 0
    while True:
        try:
            chunk = next(df_iter)
            chunk_index += 1

            # 清理列名：去空格
            chunk.columns = [col.strip() for col in chunk.columns]

            # 删除自增主键列，避免插入冲突
            if "id" in chunk.columns:
                chunk = chunk.drop(columns=["id"])

            # 字符串转时间
            chunk["tpep_pickup_datetime"] = pd.to_datetime(chunk["tpep_pickup_datetime"], errors="coerce")
            chunk["tpep_dropoff_datetime"] = pd.to_datetime(chunk["tpep_dropoff_datetime"], errors="coerce")

            # 数据质量校验（可选）
            if chunk.isnull().sum().sum() > 0:
                print(f"第 {chunk_index} 个 chunk 存在空值")

            # 插入数据
            chunk.to_sql("yellow_trips_2025_01", engine, if_exists="append", index=False, method="multi")
            print(f" 第 {chunk_index} 个 chunk 写入成功")

        except StopIteration:
            print("所有 chunk 处理完成，写入 PostgreSQL 完成！")
            break

        except Exception as e:
            print(f" 第 {chunk_index} 个 chunk 写入失败：{e}")
            break
    

def upload_to_bigquery_from_spark():

    client = bigquery.Client.from_service_account_json('/opt/airflow/config/zhenxin-project1-50133539b144.json')
    table_id = "zhenxin-project1.project1.yellow_trips_202501"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )
    
    if not csv_files:
        raise FileNotFoundError("未找到CSV文件")
    csv_file = csv_files[0]

    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_id,
            job_config=job_config,
        )
    job.result()
    print("CSV文件上传到BigQuery完成！")

def upload_to_bigquery_from_postgres():

    client = bigquery.Client.from_service_account_json('/opt/airflow/config/zhenxin-project1-50133539b144.json')
    table_id = "zhenxin-project1.project1.yellow_trips_2025_01_morningrush"

    # 不再创建 table，BigQuery 会自动创建（autodetect）
    
    # 从 PostgreSQL 查询 Morning Rush 数据
    engine = create_engine("postgresql+psycopg2://root:root@pgdatabase:5432/project1")
    query = "SELECT * FROM yellow_trips_2025_01 WHERE time_bucket = 'Morning Rush'"
    df = pd.read_sql(query, engine)

    # 导出为 CSV
    tmp_path = "/opt/airflow/lab/processed_trips_postgres_2025_01_morningrush.csv"
    df.to_csv(tmp_path, index=False)

    # 自动推断 schema，创建表并写入数据
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND" 
    )

    with open(tmp_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()

    print("已将 Morning Rush 数据自动上传至 BigQuery！")


# 参数化

DATA_DIR = "/opt/airflow/lab"
RAW_PARQUET_FILE = f"{DATA_DIR}/yellow_tripdata_2025-01.parquet"
CLEANED_CSV_DIR = f"{DATA_DIR}/cleaned_yellow_tripdata_2025-01.csv"
BQ_PARTITION_FIELD = "tpep_pickup_datetime"

# 数据库配置
POSTGRES_CONN = "postgresql+psycopg2://root:root@pgdatabase:5432/project1"
POSTGRES_TABLE = "yellow_trips_2025_01"

# BigQuery配置
BQ_PROJECT = "zhenxin-project1"
BQ_DATASET = "project1"
BQ_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.yellow_trips_2025_01_partitioned"
BQ_CREDENTIALS = "/opt/airflow/config/zhenxin-project1-50133539b144.json"

def check_data_quality():
    engine = create_engine("postgresql+psycopg2://root:root@pgdatabase:5432/project1")
    with engine.connect() as conn:
        # 行数校验
        result = conn.execute(text(f"SELECT COUNT(*) FROM {POSTGRES_TABLE}"))
        count = result.scalar()
        print(f"表{POSTGRES_TABLE}总行数: {count}")
        if count == 0:
            raise ValueError("数据表为空！")
    
        # 关键字段非空校验
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM {POSTGRES_TABLE}
            WHERE tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL
        """))
        null_count = result.scalar()
        if null_count > 0:
            raise ValueError(f"警告：存在{null_count}条关键字段为空的数据！")
        print("数据质量校验通过")


# -------------------
# DAG设置
# -------------------

# default_args = {
#   'owner': 'airflow',
#    'email': ['your_email@example.com'],
#    'email_on_failure': True,
#    'retries': 1,
#    'retry_delay': timedelta(minutes=5),}

default_args = {
    "start_date": datetime(2023, 1, 1),
}


with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    schedule_interval=None,  # 手动触发，测试阶段
    catchup=False,
) as dag:

    download_data = PythonOperator(
        task_id="download_data",
        python_callable=download_nyc_data,
    )

    spark_clean_data = SparkSubmitOperator(
        task_id="spark_clean_data",
        application="/opt/airflow/spark-apps/clean_nyc_taxi_pipeline.py",
        conn_id="spark_default",
        verbose=True,
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=write_to_postgres,
    )

    check_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )

    transform_in_postgres = PostgresOperator(
        task_id="transform_in_postgres",
        postgres_conn_id="postgres_default",
        sql="""
        ALTER TABLE yellow_trips_2025_01 ADD COLUMN IF NOT EXISTS time_bucket VARCHAR;
        UPDATE yellow_trips_2025_01
        SET time_bucket = CASE 
            WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 7 AND 9 THEN 'Morning Rush'
            WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 17 AND 19 THEN 'Evening Rush'
            ELSE 'Other'
            END;
        """,
    )

    upload_to_bq_from_spark = PythonOperator(
        task_id="upload_to_bq_from_spark",
        python_callable=upload_to_bigquery_from_spark,
    )

    upload_to_bq_from_postgres = PythonOperator(
        task_id="upload_to_bq_from_postgres",
        python_callable=upload_to_bigquery_from_postgres,
    )

    # Define task dependencies
    download_data >> spark_clean_data
    spark_clean_data >> [load_to_postgres, upload_to_bq_from_spark]
    load_to_postgres >> check_quality >> transform_in_postgres
    transform_in_postgres >> upload_to_bq_from_postgres


