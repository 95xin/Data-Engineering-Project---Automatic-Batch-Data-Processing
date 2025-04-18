from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'test_spark_integration',
    default_args=default_args,
    description='测试Airflow与Spark集成',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    test_spark = SparkSubmitOperator(
        task_id='test_spark_job',
        application='/opt/airflow/spark-apps/test_spark.py',
        conn_id='spark_default',
        verbose=True,
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.driver.memory': '1g',
            'spark.executor.memory': '1g'
        },
        env_vars={
            'SPARK_HOME': '/opt/spark'
        },
        application_args=[],
        do_xcom_push=False,
    )