import time
from typing import Any, Dict, List, Optional, Union

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
    :param name: Required. The name of the job to be submitted.
    :param image_uri: Required. The URI of the container image to use for the job.
    :param compute_config: Required. The compute configuration for the job.
    :param working_dir: Required. The working directory for the job.
    :param entrypoint: Required. The entry point script or command for the job.
    :param excludes: Optional. Files or directories to exclude. Defaults to None.
    :param requirements: Optional. Python requirements for the job. Defaults to None.
    :param env_vars: Optional. Environment variables for the job. Defaults to None.
    :param py_modules: Optional. Python modules to include. Defaults to None.
    :param max_retries: Optional. Maximum number of retries for the job. Defaults to 1.

    :raises AirflowException: If job name or entrypoint is not provided.
    """

    def __init__(
        self,
        conn_id: str,
        name: str,
        image_uri: str,
        compute_config: Union[ComputeConfig, Dict[str, Any], str],
        working_dir: str,
        entrypoint: str,
        excludes: Optional[List[str]] = None,
        requirements: Optional[Union[str, List[str]]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        py_modules: Optional[List[str]] = None,
        max_retries: int = 1,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.name = name
        self.image_uri = image_uri
        self.compute_config = compute_config
        self.working_dir = working_dir
        self.excludes = excludes
        self.requirements = requirements
        self.env_vars = env_vars
        self.py_modules = py_modules
        self.entrypoint = entrypoint
        self.max_retries = max_retries

        self.job_id: Optional[str] = None

        self.fields: Dict[str, Any] = {
            "name": name,
            "image_uri": image_uri,
            "compute_config": compute_config,
            "working_dir": working_dir,
            "excludes": excludes,
            "requirements": requirements,
            "env_vars": env_vars,
            "py_modules": py_modules,
            "entrypoint": entrypoint,
            "max_retries": max_retries,
        }

        if not self.name:
            raise AirflowException("Job name is required.")

        # Ensure entrypoint is not empty
        if not self.entrypoint:
            raise AirflowException("Entrypoint must be specified.")

    def on_kill(self) -> None:
        if self.job_id is not None:
            self.hook.terminate_job(self.job_id, 5)
            self.log.info("Termination request received. Submitted request to terminate the anyscale job.")
        return

    @cached_property
    def hook(self) -> AnyscaleHook:
        """Return an instance of the AnyscaleHook."""
        return AnyscaleHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> Optional[str]:

        if not self.hook:
            self.log.info("SDK is not available.")
            raise AirflowException("SDK is not available.")
        else:
            self.log.info(f"Using Anyscale version {anyscale.__version__}")

        # Submit the job to Anyscale
        job_config = JobConfig(**self.fields)
        self.job_id = self.hook.submit_job(job_config)
        self.created_at: float = time.time()
        self.log.info(f"Submitted Anyscale job with ID: {self.job_id}")

        current_status = self.get_current_status(self.job_id)
        self.log.info(f"Current status for {self.job_id} is: {current_status}")

        self.process_job_status(self.job_id, current_status)

        return self.job_id

    def process_job_status(self, job_id: str, current_status: str) -> None:
        if current_status in (JobState.STARTING, JobState.RUNNING):
            self.defer_job_polling(job_id)
        elif current_status == JobState.SUCCEEDED:
            self.log.info(f"Job {job_id} completed successfully.")
        elif current_status == JobState.FAILED:
            raise AirflowException(f"Job {job_id} failed.")
        else:
            raise Exception(f"Unexpected state `{current_status}` for job_id `{job_id}`.")

    def defer_job_polling(self, job_id: str) -> None:
        self.log.info("Deferring the polling to AnyscaleJobTrigger...")
        self.defer(
            trigger=AnyscaleJobTrigger(
                conn_id=self.conn_id, job_id=job_id, job_start_time=self.created_at, poll_interval=60
            ),
            method_name="execute_complete",
        )

    def get_current_status(self, job_id: str) -> str:
        return str(self.hook.get_job_status(job_id=job_id).state)

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

    :raises ValueError: If service name or applications list is not provided.
    :raises AirflowException: If the SDK is not available or the service deployment fails.
    """

    def __init__(
        self,
        conn_id: str,
        name: str,
        image_uri: str,
        compute_config: Union[ComputeConfig, Dict[str, Any], str],
        applications: List[Dict[str, Any]],
        working_dir: str,
        containerfile: Optional[str] = None,
        excludes: Optional[List[str]] = None,
        requirements: Optional[Union[str, List[str]]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        py_modules: Optional[List[str]] = None,
        query_auth_token_enabled: bool = False,
        http_options: Optional[Dict[str, Any]] = None,
        grpc_options: Optional[Dict[str, Any]] = None,
        logging_config: Optional[Dict[str, Any]] = None,
        ray_gcs_external_storage_config: Optional[Union[RayGCSExternalStorageConfig, Dict[str, Any]]] = None,
        in_place: bool = False,
        canary_percent: Optional[float] = None,
        max_surge_percent: Optional[float] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id

        # Set up explicit parameters
        self.service_params: Dict[str, Any] = {
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

        # Ensure name is not empty
        if not self.service_params["name"]:
            raise ValueError("Service name is required.")

        # Ensure at least one application is specified
        if not self.service_params["applications"]:
            raise ValueError("At least one application must be specified.")

    @cached_property
    def hook(self) -> AnyscaleHook:
        """Return an instance of the AnyscaleHook."""
        return AnyscaleHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> Optional[str]:
        if not self.hook:
            self.log.info(f"SDK is not available...")
            raise AirflowException("SDK is not available")

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
                poll_interval=60,
                timeout=600,
            ),
            method_name="execute_complete",
        )

        self.log.info(f"Service rollout id: {service_id}")
        return service_id

    def execute_complete(self, context: Context, event: Any) -> None:
        self.log.info(f"Execution completed...")
        service_id = event["service_name"]

        if event["status"] == ServiceState.SYSTEM_FAILURE:
            self.log.info(f"Anyscale service deployment {service_id} ended with status: {event['status']}")
            raise AirflowException(f"Job {service_id} failed with error {event['message']}")
        else:
            self.log.info(f"Anyscale service deployment {service_id} completed with status: {event['status']}")

        return None
