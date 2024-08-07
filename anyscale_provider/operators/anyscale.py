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

    template_fields = (
        "conn_id",
        "entrypoint",
        "name",
        "image_uri",
        "containerfile",
        "compute_config",
        "working_dir",
        "excludes",
        "requirements",
        "env_vars",
        "py_modules",
        "cloud",
        "project",
        "max_retries",
        "fetch_logs",
        "wait_for_completion",
        "job_timeout_seconds",
        "poll_interval",
    )

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
        fetch_logs: bool = True,
        wait_for_completion: bool = True,
        job_timeout_seconds: float = 3600,
        poll_interval: float = 60,
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
        self.fetch_logs = fetch_logs
        self.wait_for_completion = wait_for_completion
        self.job_timeout_seconds = job_timeout_seconds
        self.poll_interval = poll_interval

        self.job_id: str | None = None

    def on_kill(self) -> None:
        """
        Terminate the Anyscale job if the task is killed.

        This method will be called when the task is killed, and it sends a termination
        request for the currently running job.
        """
        if self.job_id is not None:
            self.hook.terminate_job(self.job_id, 5)
            self.log.info("Termination request received. Submitted request to terminate the anyscale job.")
        return

    @cached_property
    def hook(self) -> AnyscaleHook:
        """Return an instance of the AnyscaleHook."""
        return AnyscaleHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> None:
        """
        Execute the job submission to Anyscale.

        This method submits the job to Anyscale and handles its initial status.
        It defers the execution to a trigger if the job is still running or starting.

        :param context: The Airflow context.
        :return: The job ID if the job is successfully submitted and completed, or None if the job is deferred.
        """

        job_params: dict[str, Any] = {
            "entrypoint": self.entrypoint,
            "name": self.name,
            "image_uri": self.image_uri,
            "containerfile": self.containerfile,
            "compute_config": self.compute_config,
            "working_dir": self.working_dir,
            "excludes": self.excludes,
            "requirements": self.requirements,
            "env_vars": self.env_vars,
            "py_modules": self.py_modules,
            "cloud": self.cloud,
            "project": self.project,
            "max_retries": self.max_retries,
        }

        self.log.info(f"Using Anyscale version {anyscale.__version__}")
        # Submit the job to Anyscale
        job_config = JobConfig(**job_params)
        self.job_id = self.hook.submit_job(job_config)
        if self.do_xcom_push and context is not None:
            context["ti"].xcom_push(key="job_id", value=self.job_id)

        self.log.info(f"Submitted Anyscale job with ID: {self.job_id}")

        if self.wait_for_completion:
            current_state = str(self.hook.get_job_status(self.job_id).state)
            self.log.info(f"Current job state for {self.job_id} is: {current_state}")

            if current_state == JobState.SUCCEEDED:
                self.log.info(f"Job {self.job_id} completed successfully.")
            elif current_state == JobState.FAILED:
                raise AirflowException(f"Job {self.job_id} failed.")
            elif current_state in (JobState.STARTING, JobState.RUNNING):
                self.defer(
                    trigger=AnyscaleJobTrigger(
                        conn_id=self.conn_id,
                        job_id=self.job_id,
                        poll_interval=self.poll_interval,
                        fetch_logs=self.fetch_logs,
                    ),
                    method_name="execute_complete",
                    timeout=timedelta(seconds=self.job_timeout_seconds),
                )
            else:
                raise Exception(f"Unexpected state `{current_state}` for job_id `{self.job_id}`.")

    def execute_complete(self, context: Context, event: Any) -> None:
        """
        Complete the execution of the job based on the trigger event.

        This method is called when the trigger fires and provides the final status
        of the job. It raises an exception if the job failed.

        :param context: The Airflow context.
        :param event: The event data from the trigger.
        :return: None
        """
        current_job_id = event["job_id"]

        if event["state"] == JobState.FAILED:
            self.log.info(f"Anyscale job {current_job_id} ended with state: {event['state']}")
            raise AirflowException(f"Job {current_job_id} failed with error {event['message']}")
        else:
            self.log.info(f"Anyscale job {current_job_id} completed with state: {event['state']}")


class RolloutAnyscaleService(BaseOperator):
    """
    Rolls out a service on Anyscale from Apache Airflow.

    This operator handles the deployment of services on Anyscale, including the necessary
    configurations and options. It ensures the service is rolled out according to the
    specified parameters and handles the deployment lifecycle.

    :param conn_id: Required. The connection ID for Anyscale.
    :param name: Required. Unique name of the service.
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
    :param applications: Required. List of Ray Serve applications to run. At least one application must be specified. For details, see the Ray Serve config file format documentation: https://docs.ray.io/en/latest/serve/production-guide/config.html.
    :param query_auth_token_enabled: Optional. Whether or not queries to this service is gated behind an authentication token. If `True`, an auth token is generated the first time the service is deployed. You can find the token in the UI or by fetching the status of the service.
    :param http_options: Optional. HTTP options that will be passed to Ray Serve. See https://docs.ray.io/en/latest/serve/production-guide/config.html for supported options.
    :param grpc_options: Optional. gRPC options that will be passed to Ray Serve. See https://docs.ray.io/en/latest/serve/production-guide/config.html for supported options.
    :param logging_config: Optional. Logging options that will be passed to Ray Serve. See https://docs.ray.io/en/latest/serve/production-guide/config.html for supported options.
    :param ray_gcs_external_storage_config: Optional. Configuration options for external storage for the Ray Global Control Store (GCS).
    :param in_place: Optional. Flag for in-place updates. Defaults to False.
    :param canary_percent: Optional[float]. Percentage of canary deployment. Defaults to None.
    :param max_surge_percent: Optional[float]. Maximum percentage of surge during deployment. Defaults to None.
    :param service_rollout_timeout_seconds: Optional[int]. Duration after which the trigger tracking the model deployment times out. Defaults to 600 seconds.
    :param poll_interval: Optional[int]. Interval to poll the service status. Defaults to 60 seconds.
    """

    template_fields = (
        "conn_id",
        "name",
        "image_uri",
        "containerfile",
        "compute_config",
        "working_dir",
        "excludes",
        "requirements",
        "env_vars",
        "py_modules",
        "cloud",
        "project",
        "applications",
        "query_auth_token_enabled",
        "http_options",
        "grpc_options",
        "logging_config",
        "ray_gcs_external_storage_config",
        "in_place",
        "canary_percent",
        "max_surge_percent",
        "service_rollout_timeout_seconds",
        "poll_interval",
    )

    def __init__(
        self,
        conn_id: str,
        name: str,
        applications: list[dict[str, Any]],
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
        query_auth_token_enabled: bool = True,
        http_options: dict[str, Any] | None = None,
        grpc_options: dict[str, Any] | None = None,
        logging_config: dict[str, Any] | None = None,
        ray_gcs_external_storage_config: RayGCSExternalStorageConfig | dict[str, Any] | None = None,
        in_place: bool = False,
        canary_percent: int | None = None,
        max_surge_percent: int | None = None,
        service_rollout_timeout_seconds: float = 600,
        poll_interval: float = 60,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.name = name
        self.applications = applications
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
        self.query_auth_token_enabled = query_auth_token_enabled
        self.http_options = http_options
        self.grpc_options = grpc_options
        self.logging_config = logging_config
        self.ray_gcs_external_storage_config = ray_gcs_external_storage_config
        self.service_rollout_timeout_seconds = service_rollout_timeout_seconds
        self.poll_interval = poll_interval
        self.in_place = in_place
        self.canary_percent = canary_percent
        self.max_surge_percent = max_surge_percent

    @cached_property
    def hook(self) -> AnyscaleHook:
        """Return an instance of the AnyscaleHook."""
        return AnyscaleHook(conn_id=self.conn_id)

    def on_kill(self) -> None:
        """
        Terminate the Anyscale service rollout if the task is killed.

        This method will be called when the task is killed, and it sends a termination
        request for the currently running service rollout.
        """
        if self.name is not None:
            self.hook.terminate_service(self.name, 5)
            self.log.info("Termination request received. Submitted request to terminate the anyscale service rollout.")
        return

    def execute(self, context: Context) -> None:
        """
        Execute the service rollout to Anyscale.

        This method deploys the service to Anyscale with the provided configuration
        and parameters. It defers the execution to a trigger if the service is in progress.

        :param context: The Airflow context.
        :return: The service ID if the rollout is successfully initiated, or None if the job is deferred.
        """
        service_params = {
            "name": self.name,
            "image_uri": self.image_uri,
            "containerfile": self.containerfile,
            "compute_config": self.compute_config,
            "working_dir": self.working_dir,
            "excludes": self.excludes,
            "requirements": self.requirements,
            "env_vars": self.env_vars,
            "py_modules": self.py_modules,
            "cloud": self.cloud,
            "project": self.project,
            "applications": self.applications,
            "query_auth_token_enabled": self.query_auth_token_enabled,
            "http_options": self.http_options,
            "grpc_options": self.grpc_options,
            "logging_config": self.logging_config,
            "ray_gcs_external_storage_config": self.ray_gcs_external_storage_config,
        }
        self.log.info(f"Using Anyscale version {anyscale.__version__}")
        svc_config = ServiceConfig(**service_params)
        self.log.info(f"Service with config object: {svc_config}")

        # Call the SDK method with the dynamically created service model
        self.service_id = self.hook.deploy_service(
            config=svc_config,
            in_place=self.in_place,
            canary_percent=self.canary_percent,
            max_surge_percent=self.max_surge_percent,
        )

        if self.do_xcom_push and context is not None:
            context["ti"].xcom_push(key="service_id", value=self.service_id)

        self.log.info(f"Service rollout id: {self.service_id}")

        self.defer(
            trigger=AnyscaleServiceTrigger(
                conn_id=self.conn_id,
                service_name=self.name,
                expected_state=ServiceState.RUNNING,
                canary_percent=self.canary_percent,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=self.service_rollout_timeout_seconds),
        )

    def execute_complete(self, context: Context, event: Any) -> None:
        """
        Complete the execution of the service rollout based on the trigger event.

        This method is called when the trigger fires and provides the final status
        of the service rollout. It raises an exception if the rollout failed.

        :param context: The Airflow context.
        :param event: The event data from the trigger.
        :return: None
        """
        service_name = event["service_name"]
        state = event["state"]

        self.log.info(f"Execution completed for service {service_name} with state: {state}")

        if state == ServiceState.SYSTEM_FAILURE:
            error_message = event.get("message", "")
            error_msg = f"Anyscale service deployment {service_name} failed with error: {error_message}"
            self.log.error(error_msg)
            raise AirflowException(error_msg)
        else:
            self.log.info(f"Anyscale service deployment {service_name} completed successfully")
