# Images Directory

This directory contains images used in the project documentation.

## Expected Files

- `nyc_taxi_pipeline_dag.png` - Screenshot of the Airflow DAG execution graph for the NYC taxi data pipeline

## How to Add the DAG Image

1. Take a screenshot of your Airflow DAG execution graph from the Airflow UI
2. Name the file `nyc_taxi_pipeline_dag.png`
3. Place it in this directory

The image should show the workflow with tasks including:
- download_data
- spark_clean_data
- load_to_postgres
- check_data_quality
- transform_in_postgres
- upload_to_bq_from_spark
- upload_to_bq_from_postgres 