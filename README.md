# airflow-provider-anyscale

This repository provides a set of tools for integrating Anyscale with Apache Airflow, enabling the orchestration of Anyscale jobs within Airflow workflows. It includes a custom hook, two operators, and two triggers specifically designed for managing and monitoring Anyscale jobs and services.

### Components

#### Hook
- **AnyscaleHook**: Facilitates communication between Airflow and Anyscale. It uses the Anyscale API to interact with the Anyscale platform, providing methods to submit jobs, query their status, and manage services.

#### Operators
- **SubmitAnyscaleJob**: This operator submits a job to Anyscale. It takes configuration parameters for the job, such as the entry point, build ID, and compute configuration. The operator uses `AnyscaleHook` to handle the submission process.
- **RolloutAnyscaleService**: Similar to the job submission operator, this operator is designed to manage services on Anyscale. It can be used to deploy new services or update existing ones, leveraging `AnyscaleHook` for all interactions with the Anyscale API.

#### Triggers
- **AnyscaleJobTrigger**: Monitors the status of asynchronous jobs submitted via the `SubmitAnyscaleJob` operator. It ensures that the Airflow task waits until the job is completed before moving forward in the DAG.
- **AnyscaleServiceTrigger**: Works in a similar fashion to the `AnyscaleJobTrigger` but is focused on service rollout processes. It checks the status of the service being deployed or updated and returns control to Airflow upon completion.

### Example Usage

The provided `submit_anyscale_job.py` script is an example of how to configure and use the `SubmitAnyscaleJob` operator within an Airflow DAG:

```python
from airflow.models import DAG
from datetime import datetime
from custom_anyscale_operators import SubmitAnyscaleJob

# Define the default DAG arguments.
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 1),
}

# Define the DAG
dag = DAG(
    'anyscale_job_submission',
    default_args=default_args,
    schedule_interval='@daily'
)

# Job submission to Anyscale
submit_anyscale_job = SubmitAnyscaleJob(
    task_id='submit_anyscale_job',
    conn_id='anyscale_conn_id',  # Airflow connection ID for Anyscale
    name='AirflowJob',
    config={
        "entrypoint": 'python script.py',
        "build_id": 'anyscaleray2100-py39',
        "compute_config_id": '<my-compute-config-id>',
        "runtime_env": {},  # Dynamic runtime environment configurations
        "max_retries": 2
    },
    dag=dag,
)
```
The `deploy_anyscale_service.py` script uses the `RolloutAnyscaleService` operator to deploy a service on Anyscale:

```python
from airflow.models import DAG
from datetime import datetime
from custom_anyscale_operators import RolloutAnyscaleService

# Define the default DAG arguments.
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 1),
}

# Define the DAG
dag = DAG(
    'anyscale_service_deployment',
    default_args=default_args,
    schedule_interval='@daily'
)

# Deploy a service to Anyscale
deploy_anyscale_service = RolloutAnyscaleService(
    task_id="rollout_anyscale_service",
    conn_id='anyscale_conn_id',  # Airflow connection ID for Anyscale
    name="AirflowService",
    build_id="<my-build-id>",
    compute_config_id="<my-compute-config-id>",
    ray_serve_config={ 
        "applications": [
            {
                "name": "sentiment_analysis",
                "runtime_env": {
                    "working_dir": "https://github.com/anyscale/docs_examples/archive/refs/heads/main.zip"
                },
                "import_path": "sentiment_analysis.app:model",
            }
        ]
    },
    version = 11,
    dag=dag
)
```