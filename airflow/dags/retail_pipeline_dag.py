from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='retail_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    bronze_ingestion = BashOperator(
        task_id='run_bronze_ingestion',
        bash_command='echo "Bronze ingestion complete"'
    )

    silver_transform = BashOperator(
        task_id='run_silver_transform',
        bash_command='echo "Silver transformation complete"'
    )

    dbt_gold = BashOperator(
        task_id='run_dbt_gold',
        bash_command='echo "dbt Gold models complete"'
    )

    bronze_ingestion >> silver_transform >> dbt_gold
