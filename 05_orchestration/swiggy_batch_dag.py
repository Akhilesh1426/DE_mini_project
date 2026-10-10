from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'swiggy_batch_pipeline',
    default_args=default_args,
    description='Batch Ingestion and DBT Transformation',
    schedule_interval=timedelta(days=1),
    catchup=False
)

def export_raw_data():
    # Placeholder for the actual export script we wrote earlier
    os.system("python /home/jovyan/work/05_orchestration/scripts/export_raw_data.py")

export_task = PythonOperator(
    task_id='export_raw_postgres_data',
    python_callable=export_raw_data,
    dag=dag,
)

ingest_task = BashOperator(
    task_id='ingest_to_bronze',
    bash_command='cd /home/jovyan/work && /usr/local/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.7.3 /home/jovyan/work/02_ingestion/batch_ingestion.py',
    dag=dag,
)

dbt_task = BashOperator(
    task_id='dbt_transform',
    bash_command='cd /home/jovyan/work/04_transform && /opt/conda/bin/dbt run --profiles-dir .',
    dag=dag,
)

export_task >> ingest_task >> dbt_task
