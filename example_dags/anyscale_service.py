# This DAG is currently not working as expected.
# It fails with:
#  File "/Users/tatiana.alchueyr/Library/Application Support/hatch/env/virtual/astro-provider-anyscale/MZnqhpLD/tests.py3.11-2.11/lib/python3.11/site-packages/airflow/models/baseoperator.py", line 424, in wrapper
#    return func(self, *args, **kwargs)
#           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
#  File "/Users/tatiana.alchueyr/Code/astro-provider-anyscale/anyscale_provider/operators/anyscale.py", line 385, in execute
#    self.service_id = self.hook.deploy_service(
#                      ^^^^^^^^^^^^^^^^^^^^^^^^^
#  File "/Users/tatiana.alchueyr/Code/astro-provider-anyscale/anyscale_provider/hooks/anyscale.py", line 99, in deploy_service
#    service_id: str = self.client.service.deploy(
#                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^
# This is unrelated to the Airflow 3 support. It relates to a breaking change in the Anyscale SDK and I'm addressing it in a separate PR.

"""
import os
import uuid
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from anyscale_provider.hooks.anyscale import AnyscaleHook
from anyscale_provider.operators.anyscale import RolloutAnyscaleService

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 2),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Define the Anyscale connection
ANYSCALE_CONN_ID = "anyscale_conn"
service_id = os.getenv("ASTRO_ANYSCALE_PROVIDER_SERVICE_ID", {uuid.uuid4()})
SERVICE_NAME = f"AstroService-CICD-{service_id}"

dag = DAG(
    "sample_anyscale_service_workflow",
    default_args=default_args,
    description="A DAG to interact with Anyscale triggered manually",
    schedule=None,  # This DAG is not scheduled, only triggered manually
    catchup=False,
)

deploy_anyscale_service = RolloutAnyscaleService(
    task_id="rollout_anyscale_service",
    conn_id=ANYSCALE_CONN_ID,
    name=SERVICE_NAME,
    image_uri="anyscale/image/airflow-integration-testing:1",
    compute_config="airflow-integration-testing:1",
    working_dir="https://github.com/anyscale/docs_examples/archive/refs/heads/main.zip",
    applications=[{"import_path": "sentiment_analysis.app:model"}],
    requirements=["transformers", "requests", "pandas", "numpy", "torch"],
    in_place=False,
    canary_percent=None,
    service_rollout_timeout_seconds=600,
    poll_interval=30,
    dag=dag,
)


def terminate_service():
    hook = AnyscaleHook(conn_id=ANYSCALE_CONN_ID)
    result = hook.terminate_service(service_name=SERVICE_NAME, time_delay=5)
    print(result)


terminate_anyscale_service = PythonOperator(
    task_id="initialize_anyscale_hook",
    python_callable=terminate_service,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Defining the task sequence
deploy_anyscale_service >> terminate_anyscale_service

"""
