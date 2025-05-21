from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Constants / Configuration
PROJECT_ID = 'project3cloudcompser'
REGION = 'us-central1'
GCS_TEMPLATE_PATH = 'gs://composer18/templates/health_template'
DATAFLOW_TEMP_LOCATION = 'gs://composer18/temp'

# Define default args
default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define DAG
with models.DAG(
    dag_id='etl_health_data_dag',
    schedule_interval=None,  # Change to '@daily' or cron string if you want a schedule
    default_args=default_args,
    catchup=False,
    tags=['composer', 'dataflow', 'bq']
) as dag:

    # Trigger Dataflow job from template
    run_dataflow_template = DataflowTemplatedJobStartOperator(
        task_id='run_dataflow_etl',
        template=GCS_TEMPLATE_PATH,
        project_id=PROJECT_ID,
        location=REGION,
        job_name='health-data-etl-{{ ds_nodash }}',  # Ensures unique job name per run
        parameters={
            'input': 'gs://composer18/healthcare_dataset.csv',
            'output':'project3cloudcompser:healthdataset.health_etl'
        },
        environment={
            'tempLocation': DATAFLOW_TEMP_LOCATION,
            'zone': 'us-central1-c',
            # Optional enhancements
            'maxWorkers': 3,
            'machineType': 'n1-standard-1'
        },
    )
