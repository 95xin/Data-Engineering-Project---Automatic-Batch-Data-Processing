from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery

def upload_to_bigquery():
    client = bigquery.Client.from_service_account_json('/opt/airflow/config/zhenxin-project1-50133539b144.json')
    table_id = "zhenxin-project1.project1.taxi_zone_lookup"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    with open("/opt/airflow/spark-apps/taxi_zone_lookup.csv", "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_id,
            job_config=job_config,
        )
    job.result()
    print("Upload complete!")

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="upload_to_bigquery",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    upload_task = PythonOperator(
        task_id="upload_to_bigquery_task",
        python_callable=upload_to_bigquery,
    )
    