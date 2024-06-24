from __future__ import annotations

from datetime import timedelta
from typing import Any

import anyscale
from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context
from anyscale.compute_config.models import ComputeConfig
from anyscale.job.models import JobConfig, JobState
from anyscale.service.models import RayGCSExternalStorageConfig, ServiceConfig, ServiceState

from anyscale_provider.hooks.anyscale import AnyscaleHook
from anyscale_provider.triggers.anyscale import AnyscaleJobTrigger, AnyscaleServiceTrigger


class SubmitAnyscaleJob(BaseOperator):
    """
    Submits a job to Anyscale from Apache Airflow.

    This operator handles the submission and management of jobs on Anyscale. It initializes
    with the necessary parameters to define and configure the job, and provides mechanisms
    for job submission, status tracking, and handling job outcomes.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SubmitAnyscaleJobOperator`

    :param conn_id: Required. The connection ID for Anyscale.
    :param entrypoint: Required. Command that will be run to execute the job, e.g., `python main.py`.
    :param name: Optional. Name of the job. Multiple jobs can be submitted with the same name.
    :param image_uri: Optional. URI of an existing image. Exclusive with `containerfile`.
    :param containerfile: Optional. The file path to a containerfile that will be built into an image before running the workload. Exclusive with `image_uri`.
    :param compute_config: Optional. The name of an existing registered compute config or an inlined ComputeConfig object.
    :param working_dir: Optional. Directory that will be used as the working directory for the application. If a local directory is provided, it will be uploaded to cloud storage automatically. When running inside a workspace, this defaults to the current working directory ('.').
    :param excludes: Optional. A list of file path globs that will be excluded when uploading local files for `working_dir`.
    :param requirements: Optional. A list of requirements or a path to a `requirements.txt` file for the workload. When running inside a workspace, this defaults to the workspace-tracked requirements.
    :param env_vars: Optional. A dictionary of environment variables that will be set for the workload.
    :param py_modules: Optional. A list of local directories that will be uploaded and added to the Python path.
    :param cloud: Optional. The Anyscale Cloud to run this workload on. If not provided, the organization default will be used (or, if running in a workspace, the cloud of the workspace).
    :param project: Optional. The Anyscale project to run this workload in. If not provided, the organization default will be used (or, if running in a workspace, the project of the workspace).
    :param max_retries: Optional. Maximum number of times the job will be retried before being marked failed. Defaults to `1`.
    """

    def __init__(
        self,
        conn_id: str,
        entrypoint: str,
        name: str | None = None,
        image_uri: str | None = None,
        containerfile: str | None = None,
        compute_config: ComputeConfig | dict[str, Any] | str | None = None,
        working_dir: str | None = None,
        excludes: list[str] | None = None,
        requirements: str | list[str] | None = None,
        env_vars: dict[str, str] | None = None,
        py_modules: list[str] | None = None,
        cloud: str | None = None,
        project: str | None = None,
        max_retries: int = 1,
        job_timeout_seconds: int = 3600,
        poll_interval: int = 60,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.entrypoint = entrypoint
        self.name = name
        self.image_uri = image_uri
        self.containerfile = containerfile
        self.compute_config = compute_config
        self.working_dir = working_dir
        self.excludes = excludes
        self.requirements = requirements
        self.env_vars = env_vars
        self.py_modules = py_modules
        self.cloud = cloud
        self.project = project
        self.max_retries = max_retries
        self.job_timeout_seconds = timedelta(seconds=job_timeout_seconds)
        self.poll_interval = poll_interval
        self.job_id: str | None = None

        self.job_params: dict[str, Any] = {
            "entrypoint": entrypoint,
            "name": name,
            "image_uri": image_uri,
            "containerfile": containerfile,
            "compute_config": compute_config,
            "working_dir": working_dir,
            "excludes": excludes,
            "requirements": requirements,
            "env_vars": env_vars,
            "py_modules": py_modules,
            "cloud": cloud,
            "project": project,
            "max_retries": max_retries,
        }

    def on_kill(self) -> None:
        if self.job_id is not None:
            self.hook.terminate_job(self.job_id, 5)
            self.log.info("Termination request received. Submitted request to terminate the anyscale job.")
        return

    @cached_property
    def hook(self) -> AnyscaleHook:
        """Return an instance of the AnyscaleHook."""
        return AnyscaleHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> str | None:
        self.log.info(f"Using Anyscale version {anyscale.__version__}")
        # Submit the job to Anyscale
        job_config = JobConfig(**self.job_params)
        self.job_id = self.hook.submit_job(job_config)
        self.log.info(f"Submitted Anyscale job with ID: {self.job_id}")

        current_status = str(self.hook.get_job_status(self.job_id).state)
        self.log.info(f"Current status for {self.job_id} is: {current_status}")

        if current_status == JobState.SUCCEEDED:
            self.log.info(f"Job {self.job_id} completed successfully.")
        elif current_status == JobState.FAILED:
            raise AirflowException(f"Job {self.job_id} failed.")
        elif current_status in (JobState.STARTING, JobState.RUNNING):
            self.defer(
                trigger=AnyscaleJobTrigger(conn_id=self.conn_id, job_id=self.job_id, poll_interval=self.poll_interval),
                method_name="execute_complete",
                timeout=self.job_timeout_seconds,
            )
        else:
            raise Exception(f"Unexpected state `{current_status}` for job_id `{self.job_id}`.")

        return self.job_id

    def execute_complete(self, context: Context, event: Any) -> None:
        current_job_id = event["job_id"]

        if event["status"] == JobState.FAILED:
            self.log.info(f"Anyscale job {current_job_id} ended with status: {event['status']}")
            raise AirflowException(f"Job {current_job_id} failed with error {event['message']}")
        else:
            self.log.info(f"Anyscale job {current_job_id} completed with status: {event['status']}")
        return None


class RolloutAnyscaleService(BaseOperator):
    """
    Rolls out a service on Anyscale from Apache Airflow.

    This operator handles the deployment of services on Anyscale, including the necessary
    configurations and options. It ensures the service is rolled out according to the
    specified parameters and handles the deployment lifecycle.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RolloutAnyscaleServiceOperator`

    :param conn_id: Required. The connection ID for Anyscale.
    :param name: Required. The name of the service to be deployed.
    :param image_uri: Required. The URI of the container image to use for the service.
    :param containerfile: Optional. Path to the container file. Defaults to None.
    :param compute_config: Optional. The compute configuration for the service. Defaults to None.
    :param working_dir: Optional. The working directory for the service. Defaults to None.
    :param excludes: Optional. Files or directories to exclude. Defaults to None.
    :param requirements: Optional. Python requirements for the service. Defaults to None.
    :param env_vars: Optional. Environment variables for the service. Defaults to None.
    :param py_modules: Optional. Python modules to include. Defaults to None.
    :param applications: Required. List of applications to deploy.
    :param query_auth_token_enabled: Optional. Flag to enable query authentication token. Defaults to False.
    :param http_options: Optional. HTTP options for the service. Defaults to None.
    :param grpc_options: Optional. gRPC options for the service. Defaults to None.
    :param logging_config: Optional. Logging configuration for the service. Defaults to None.
    :param ray_gcs_external_storage_config: Optional. Ray GCS external storage configuration. Defaults to None.
    :param in_place: Optional. Flag for in-place updates. Defaults to False.
    :param canary_percent: Optional[float]. Percentage of canary deployment. Defaults to None.
    :param max_surge_percent: Optional[float]. Maximum percentage of surge during deployment. Defaults to None.
    :param service_rollout_timeout_seconds: Optional[int]. Duration after which the trigger tracking the model deployment times out. Defaults to 600 seconds.
    :param poll_interval: Optional[int]. Interval to poll the service status. Defaults to 60 seconds.
    """

    def __init__(
        self,
        conn_id: str,
        name: str,
        image_uri: str,
        compute_config: ComputeConfig | dict[str, Any] | str,
        applications: list[dict[str, Any]],
        working_dir: str,
        containerfile: str | None = None,
        excludes: list[str] | None = None,
        requirements: str | list[str] | None = None,
        env_vars: dict[str, str] | None = None,
        py_modules: list[str] | None = None,
        query_auth_token_enabled: bool = False,
        http_options: dict[str, Any] | None = None,
        grpc_options: dict[str, Any] | None = None,
        logging_config: dict[str, Any] | None = None,
        ray_gcs_external_storage_config: RayGCSExternalStorageConfig | dict[str, Any] | None = None,
        in_place: bool = False,
        canary_percent: float | None = None,
        max_surge_percent: float | None = None,
        service_rollout_timeout_seconds: int = 600,
        poll_interval: int = 60,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.service_rollout_timeout_seconds = timedelta(seconds=service_rollout_timeout_seconds)
        self.poll_interval = poll_interval
        if not name:
            raise ValueError("Service name is required.")
        if not applications:
            raise ValueError("At least one application must be specified.")
        # Set up explicit parameters
        self.service_params: dict[str, Any] = {
            "name": name,
            "image_uri": image_uri,
            "containerfile": containerfile,
            "compute_config": compute_config,
            "working_dir": working_dir,
            "excludes": excludes,
            "requirements": requirements,
            "env_vars": env_vars,
            "py_modules": py_modules,
            "applications": applications,
            "query_auth_token_enabled": query_auth_token_enabled,
            "http_options": http_options,
            "grpc_options": grpc_options,
            "logging_config": logging_config,
            "ray_gcs_external_storage_config": ray_gcs_external_storage_config,
        }

        self.in_place = in_place
        self.canary_percent = canary_percent
        self.max_surge_percent = max_surge_percent

    @cached_property
    def hook(self) -> AnyscaleHook:
        """Return an instance of the AnyscaleHook."""
        return AnyscaleHook(conn_id=self.conn_id)

    def on_kill(self) -> None:
        if self.service_params["name"] is not None:
            self.hook.terminate_service(self.service_params["name"], 5)
            self.log.info("Termination request received. Submitted request to terminate the anyscale service rollout.")
        return

    def execute(self, context: Context) -> str | None:
        self.log.info(f"Using Anyscale version {anyscale.__version__}")
        svc_config = ServiceConfig(**self.service_params)
        self.log.info(f"Service with config object: {svc_config}")

        # Call the SDK method with the dynamically created service model
        service_id = self.hook.deploy_service(
            config=svc_config,
            in_place=self.in_place,
            canary_percent=self.canary_percent,
            max_surge_percent=self.max_surge_percent,
        )
        self.defer(
            trigger=AnyscaleServiceTrigger(
                conn_id=self.conn_id,
                service_name=self.service_params["name"],
                expected_state=ServiceState.RUNNING,
                canary_percent=self.canary_percent,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
            timeout=(self.service_rollout_timeout_seconds),
        )

        self.log.info(f"Service rollout id: {service_id}")
        return service_id

    def execute_complete(self, context: Context, event: Any) -> None:
        service_id = event["service_name"]
        status = event["status"]

        self.log.info(f"Execution completed for service {service_id} with status: {status}")

        if status == ServiceState.SYSTEM_FAILURE:
            error_message = event.get("message", "")
            error_msg = f"Anyscale service deployment {service_id} failed with error: {error_message}"
            self.log.error(error_msg)
            raise AirflowException(error_msg)
        else:
            self.log.info(f"Anyscale service deployment {service_id} completed successfully")

        return None
