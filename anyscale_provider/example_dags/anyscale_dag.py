from datetime import datetime, timedelta
from airflow import DAG
import os
# Assuming these hooks and operators are custom or provided by a plugin
from anyscale_provider.operators.anyscale import SubmitAnyscaleJob
from anyscale_provider.hooks.anyscale import AnyscaleHook

from airflow.models.connection import Connection
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Anyscale connection
ANYSCALE_CONN_ID = "anyscale_conn"

# Constants
BUCKET_NAME = 'anyscale-production-data-cld-g7m5cn8nnhkydikcjc6lj4ekye'
FILE_PATH = '/usr/local/airflow/example_dags/ray_scripts/script.py'
AWS_CONN_ID = 'aws_conn'

dag = DAG(
    'sample_anyscale_workflow',
    default_args=default_args,
    description='A DAG to interact with Anyscale triggered manually',
    schedule_interval=None,  # This DAG is not scheduled, only triggered manually
    catchup=False,
)

runtime_env = {"working_dir": FILE_PATH,
               "upload_path": "s3://"+BUCKET_NAME,
               "pip": ["requests","pandas","numpy","torch"]}

# Extract the filename from the file path for S3 key construction
filename = os.path.basename(FILE_PATH)
s3_key = f'scripts/{filename}'

upload_file_to_s3 = LocalFilesystemToS3Operator(
    task_id='upload_file_to_s3',
    filename=FILE_PATH,
    dest_key=s3_key,
    dest_bucket=BUCKET_NAME,
    aws_conn_id=AWS_CONN_ID,
    replace=True,
    dag=dag
)


submit_anyscale_job = SubmitAnyscaleJob(
    task_id='submit_anyscale_job',
    conn_id = ANYSCALE_CONN_ID,
    name = 'AstroJob',
    config = {"entrypoint": 'python script.py',
             "build_id": 'anyscaleray2100-py39',
             "compute_config_id": 'cpt_8kfdcvmckjnjqd1xwnctmpldl4',
             "runtime_env": runtime_env,
             "max_retries": 2},
    dag=dag,
)


# Defining the task sequence
upload_file_to_s3 >> submit_anyscale_job
