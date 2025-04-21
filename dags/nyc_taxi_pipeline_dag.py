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
    print("Download completed!")


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
        # Add indexes for date fields
        conn.execute(text("""CREATE INDEX IF NOT EXISTS idx_pickup_datetime ON yellow_trips_2025_01 USING BTREE ("tpep_pickup_datetime");
                            """))
        conn.execute(text("""CREATE INDEX IF NOT EXISTS idx_dropoff_datetime ON yellow_trips_2025_01 USING BTREE ("tpep_dropoff_datetime");"""))
        print("Table structure and indexes created")


def write_to_postgres():
    create_postgres_table()  # Create table first
    engine = create_engine("postgresql+psycopg2://root:root@pgdatabase:5432/project1")
    chunksize = 100000
    if not csv_files:
        raise FileNotFoundError("CSV file not found")
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

            # Clean column names: remove spaces
            chunk.columns = [col.strip() for col in chunk.columns]

            # Remove auto-increment primary key column to avoid insertion conflicts
            if "id" in chunk.columns:
                chunk = chunk.drop(columns=["id"])

            # Convert strings to datetime
            chunk["tpep_pickup_datetime"] = pd.to_datetime(chunk["tpep_pickup_datetime"], errors="coerce")
            chunk["tpep_dropoff_datetime"] = pd.to_datetime(chunk["tpep_dropoff_datetime"], errors="coerce")

            # Data quality validation (optional)
            if chunk.isnull().sum().sum() > 0:
                print(f"Chunk {chunk_index} contains null values")

            # Insert data
            chunk.to_sql("yellow_trips_2025_01", engine, if_exists="append", index=False, method="multi")
            print(f"Chunk {chunk_index} written successfully")

        except StopIteration:
            print("All chunks processed, writing to PostgreSQL completed!")
            break

        except Exception as e:
            print(f"Chunk {chunk_index} writing failed: {e}")
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
        raise FileNotFoundError("CSV file not found")
    csv_file = csv_files[0]

    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_id,
            job_config=job_config,
        )
    job.result()
    print("CSV file upload to BigQuery completed!")

def upload_to_bigquery_from_postgres():

    client = bigquery.Client.from_service_account_json('/opt/airflow/config/zhenxin-project1-50133539b144.json')
    table_id = "zhenxin-project1.project1.yellow_trips_2025_01_morningrush"

    # No longer creating table, BigQuery will create it automatically (autodetect)
    
    # Query Morning Rush data from PostgreSQL
    engine = create_engine("postgresql+psycopg2://root:root@pgdatabase:5432/project1")
    query = "SELECT * FROM yellow_trips_2025_01 WHERE time_bucket = 'Morning Rush'"
    df = pd.read_sql(query, engine)

    # Export to CSV
    tmp_path = "/opt/airflow/lab/processed_trips_postgres_2025_01_morningrush.csv"
    df.to_csv(tmp_path, index=False)

    # Auto-detect schema, create table and write data
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND" 
    )

    with open(tmp_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()

    print("Morning Rush data has been automatically uploaded to BigQuery!")


# Parameterization

DATA_DIR = "/opt/airflow/lab"
RAW_PARQUET_FILE = f"{DATA_DIR}/yellow_tripdata_2025-01.parquet"
CLEANED_CSV_DIR = f"{DATA_DIR}/cleaned_yellow_tripdata_2025-01.csv"
BQ_PARTITION_FIELD = "tpep_pickup_datetime"

# Database configuration
POSTGRES_CONN = "postgresql+psycopg2://root:root@pgdatabase:5432/project1"
POSTGRES_TABLE = "yellow_trips_2025_01"

# BigQuery configuration
BQ_PROJECT = "zhenxin-project1"
BQ_DATASET = "project1"
BQ_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.yellow_trips_2025_01_partitioned"
BQ_CREDENTIALS = "/opt/airflow/config/zhenxin-project1-50133539b144.json"

def check_data_quality():
    engine = create_engine("postgresql+psycopg2://root:root@pgdatabase:5432/project1")
    with engine.connect() as conn:
        # Row count validation
        result = conn.execute(text(f"SELECT COUNT(*) FROM {POSTGRES_TABLE}"))
        count = result.scalar()
        print(f"Total rows in table {POSTGRES_TABLE}: {count}")
        if count == 0:
            raise ValueError("Data table is empty!")
    
        # Key field non-null validation
        result = conn.execute(text(f"""
            SELECT COUNT(*) FROM {POSTGRES_TABLE}
            WHERE tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL
        """))
        null_count = result.scalar()
        if null_count > 0:
            raise ValueError(f"Warning: There are {null_count} records with null values in key fields!")
        print("Data quality validation passed")


# -------------------
# DAG Settings
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
    schedule_interval=None,  # Manual trigger, testing phase
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


