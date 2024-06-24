# astro-provider-anyscale

This repository provides a set of tools for integrating Anyscale with Apache Airflow, enabling the orchestration of Anyscale jobs and services within Airflow workflows. It includes a custom hook, two operators, and two triggers specifically designed for managing and monitoring Anyscale jobs and services.

### Components

#### Hook
- **AnyscaleHook**: Facilitates communication between Airflow and Anyscale. It uses the Anyscale API to interact with the Anyscale platform, providing methods to submit jobs, query their status, and manage services.

#### Operators
- **SubmitAnyscaleJob**: This operator submits a job to Anyscale. It takes configuration parameters for the job, such as the entry point, build ID, and compute configuration. The operator uses `AnyscaleHook` to handle the submission process.
- **RolloutAnyscaleService**: Similar to the job submission operator, this operator is designed to manage services on Anyscale. It can be used to deploy new services or update existing ones, leveraging `AnyscaleHook` for all interactions with the Anyscale API.

#### Triggers
- **AnyscaleJobTrigger**: Monitors the status of asynchronous jobs submitted via the `SubmitAnyscaleJob` operator. It ensures that the Airflow task waits until the job is completed before moving forward in the DAG.
- **AnyscaleServiceTrigger**: Works in a similar fashion to the `AnyscaleJobTrigger` but is focused on service rollout processes. It checks the status of the service being deployed or updated and returns control to Airflow upon completion.

### Configuration Details for Anyscale Integration

To integrate Airflow with Anyscale, you will need to provide several configuration details:

- **Anyscale API Token**: Obtain your API token either by using the anyscale cli or through the [Anyscale console](https://console.anyscale.com/v2/api-keys?api-keys-tab=platform).

- **Compute Config (optional)**: This ID specifies the machines that will execute your Ray script. You can either:
  - Dynamically provide this via the `compute_config` input parameter, or
  - Create a compute configuration in Anyscale and use the resulting ID in the `compute_config_id` parameter.

- **Image URI**: Specify the docker image you would like your operator to use. Make sure your image is accessible within your Anyscale account. Note, you can alternatively specify a containerfile that can be used to dynamically build the image


### Usage

Install the Anyscale provider using the command below:

```sh
pip install astro-provider-anyscale
```

### Airflow Connection Configuration

To integrate Airflow with Anyscale, configure an Airflow connection with a unique name and set the password as the API token gathered through the Anyscale console.

1. **Access Airflow Web UI:**
   - Open the Airflow web interface and log in using your Airflow credentials.

2. **Create a New Connection in Airflow:**
   - Go to the "Admin" tab and select "Connections" from the dropdown menu.
   - Click the "Add a new record" button to create a new connection.

3. **Configure the Connection:**
   - **Conn Id:** Enter a unique identifier for the connection, e.g., `anyscale_conn`.
   - **Conn Type:** Select `Anyscale`
   - **Password:** Paste the API token you copied from the Anyscale console.

4. **Save the Connection:**
   - After filling in the required details, click the "Save" button at the bottom of the form to save the new connection.

### Code samples

The below script is an example of how to configure and use the `SubmitAnyscaleJob` operator within an Airflow DAG:

```python
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from anyscale_provider.operators.anyscale import SubmitAnyscaleJob

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
FOLDER_PATH = Path(__file__).parent / "example_dags/ray_scripts"

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
    image_uri="anyscale/ray:2.23.0-py311",
    compute_config="my-compute-config:1",
    working_dir=str(FOLDER_PATH),
    entrypoint="python ray-job.py",
    requirements=["requests", "pandas", "numpy", "torch"],
    max_retries=1,
    job_timeout_seconds=3000,
    poll_interval=120,
    dag=dag,
)


# Defining the task sequence
submit_anyscale_job
```
The below script uses the `RolloutAnyscaleService` operator to deploy a service on Anyscale:

```python
import uuid
from datetime import datetime, timedelta

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
    schedule=None,  # This DAG is not scheduled, only triggered manually
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
    service_rollout_timeout_seconds=600,
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
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Defining the task sequence
deploy_anyscale_service >> terminate_anyscale_service
```

### Changelog
_________

We follow [Semantic Versioning](https://semver.org/) for releases.
Check [CHANGELOG.rst](https://github.com/astronomer/astro-provider-anyscale/blob/main/CHANGELOG.rst)
for the latest changes.


### Contributing Guide
__________________

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview an how to contribute can be found in the [Contributing Guide](https://github.com/astronomer/astro-provider-anyscale/blob/main/CONTRIBUTING.rst)
