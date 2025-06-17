from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='dataflow_batch_pipeline_csv_trigger',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
) as dag:

    trigger_dataflow = BashOperator(
        task_id='trigger_dataflow',
        bash_command="""
        python3 /home/airflow/gcs/data/dataflow_batch_pipeline.py \
        --runner DataflowRunner \
        --project=dataflow-poc-462411 \
        --region=us-east1 \
        --temp_location=gs://dataflow-batch/temp \
        --staging_location=gs://dataflow-batch/staging
        """
    )
