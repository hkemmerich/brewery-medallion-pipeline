from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


args_default = {
    'owner': 'hugo',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG (
    dag_id = 'brewery_medallion_pipeline',
    description = 'Pipeline medallion da Brewery DB, bronze, silver e gold',
    default_args = args_default,
    start_date = datetime(2026, 3, 17),
    schedule = None,
    catchup =  False,
    tags= ['brewery', 'pyspark'],
) as dag:
    extract_bronze = BashOperator(task_id='extract_bronze',bash_command='cd /app && python src/extract/extract_bronze.py "{{ ts_nodash }}"')
    transform_silver = BashOperator(task_id='transform_silver',bash_command='cd /app && python src/transform/transform_silver.py "{{ ts_nodash }}"')
    aggregate_gold = BashOperator(task_id='aggregate_gold',bash_command='cd /app && python src/transform/aggregate_gold.py "{{ ts_nodash }}"')
    extract_bronze >> transform_silver >> aggregate_gold