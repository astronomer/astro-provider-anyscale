import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from anyscale_provider.hooks.anyscale import AnyscaleHook
from anyscale_provider.operators.anyscale import RolloutAnyscaleService

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 2),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the Anyscale connection
ANYSCALE_CONN_ID = "anyscale_conn"
SERVICE_NAME = f"AstroService-CICD-{uuid.uuid4()}"

dag = DAG(
    "sample_anyscale_service_workflow",
    default_args=default_args,
    description="A DAG to interact with Anyscale triggered manually",
    schedule_interval=None,  # This DAG is not scheduled, only triggered manually
    catchup=False,
)

deploy_anyscale_service = RolloutAnyscaleService(
    task_id="rollout_anyscale_service",
    conn_id=ANYSCALE_CONN_ID,
    name=SERVICE_NAME,
    image_uri="anyscale/ray:2.23.0-py311",
    compute_config="my-compute-config:1",
    working_dir="https://github.com/anyscale/docs_examples/archive/refs/heads/main.zip",
    applications=[{"import_path": "sentiment_analysis.app:model"}],
    requirements=["transformers", "requests", "pandas", "numpy", "torch"],
    in_place=False,
    canary_percent=None,
    service_timeout_seconds=600,
    poll_interval=120,
    dag=dag,
)


def terminate_service():
    hook = AnyscaleHook(conn_id=ANYSCALE_CONN_ID)
    result = hook.terminate_service(service_id=SERVICE_NAME, time_delay=5)
    print(result)


terminate_anyscale_service = PythonOperator(
    task_id="initialize_anyscale_hook",
    python_callable=terminate_service,
    dag=dag,
)

# Defining the task sequence
deploy_anyscale_service >> terminate_anyscale_service
