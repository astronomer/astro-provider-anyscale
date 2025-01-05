from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

from anyscale_provider.operators.anyscale import SubmitAnyscaleJob
from anyscale.job.models import JobQueueConfig, JobQueueSpec, JobQueueExecutionMode

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

# Constants
FOLDER_PATH = Path(__file__).parent / "ray_scripts"

dag = DAG(
    "sample_anyscale_job_workflow",
    default_args=default_args,
    description="A DAG to interact with Anyscale triggered manually",
    schedule=None,  # This DAG is not scheduled, only triggered manually
    catchup=False,
)

submit_anyscale_job = SubmitAnyscaleJob(
    task_id="submit_anyscale_job",
    conn_id=ANYSCALE_CONN_ID,
    name="AstroJob",
    image_uri="anyscale/image/airflow-integration-testing:1",
    compute_config="airflow-integration-testing:1",
    working_dir=str(FOLDER_PATH),
    entrypoint="python ray-job.py",
    requirements=["requests", "pandas", "numpy", "torch"],
    max_retries=1,
    job_timeout_seconds=3000,
    poll_interval=30,
    dag=dag,
)

JOB_QUEUE_NAME = "test-job-queue-180s-idle-timeout"

submit_anyscale_job_with_new_job_queue = SubmitAnyscaleJob(
    task_id="submit_anyscale_job_with_new_job_queue",
    conn_id=ANYSCALE_CONN_ID,
    name="AstroJobWithJobQueue",
    image_uri="anyscale/image/airflow-integration-testing:1",
    compute_config="airflow-integration-testing:1",
    working_dir=str(FOLDER_PATH),
    entrypoint="python ray-job.py",
    max_retries=1,
    job_timeout_seconds=3000,
    poll_interval=30,
    dag=dag,
    job_queue_config=JobQueueConfig(
        priority=100,
        job_queue_spec=JobQueueSpec(
            name=JOB_QUEUE_NAME,
            execution_mode=JobQueueExecutionMode.PRIORITY,
            max_concurrency=5,
            idle_timeout_s=180,
        ),
    ),
)

submit_anyscale_job_with_existing_job_queue = SubmitAnyscaleJob(
    task_id="submit_anyscale_job_with_existing_job_queue",
    conn_id=ANYSCALE_CONN_ID,
    name="AstroJobWithJobQueue",
    image_uri="anyscale/image/airflow-integration-testing:1",
    compute_config="airflow-integration-testing:1",
    working_dir=str(FOLDER_PATH),
    entrypoint="python ray-job.py",
    max_retries=1,
    job_timeout_seconds=3000,
    poll_interval=30,
    dag=dag,
    job_queue_config=JobQueueConfig(
        priority=100,
        # NOTE: This will reuse the existing job queue given it has the same spec
        job_queue_spec=JobQueueSpec(
            name=JOB_QUEUE_NAME,
            execution_mode=JobQueueExecutionMode.PRIORITY,
            max_concurrency=5,
            idle_timeout_s=180,
        ),
    ),
)

submit_another_anyscale_job_with_existing_job_queue = SubmitAnyscaleJob(
    task_id="submit_another_anyscale_job_with_existing_job_queue",
    conn_id=ANYSCALE_CONN_ID,
    name="AstroJobWithJobQueue",
    image_uri="anyscale/image/airflow-integration-testing:1",
    compute_config="airflow-integration-testing:1",
    working_dir=str(FOLDER_PATH),
    entrypoint="python ray-job.py",
    max_retries=1,
    job_timeout_seconds=3000,
    poll_interval=30,
    dag=dag,
    job_queue_config=JobQueueConfig(
        priority=100,
        target_job_queue_name=JOB_QUEUE_NAME,
    ),
)


# Defining the task sequence
submit_anyscale_job
submit_anyscale_job_with_new_job_queue >> [
    submit_anyscale_job_with_existing_job_queue,
    submit_another_anyscale_job_with_existing_job_queue,
]
