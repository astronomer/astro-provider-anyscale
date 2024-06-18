import os
import time
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from anyscale import Anyscale
from anyscale.job.models import JobConfig, JobStatus
from anyscale.service.models import ServiceConfig, ServiceStatus


class AnyscaleHook(BaseHook):
    """
    This hook handles the authentication and session management for Anyscale services.

    .. seealso::
        For more information on how to use this hook, take a look at the guide:
        :ref:`howto/hook:AnyscaleHook`

    :param conn_id: Optional. The connection ID to use for Anyscale. Defaults to "anyscale_default".
    """

    conn_name_attr = "conn_id"
    default_conn_name = "anyscale_default"
    conn_type = "anyscale"
    hook_name = "Anyscale"

    def __init__(self, conn_id: str = default_conn_name, **kwargs: Any) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.log.info(f"Initializing AnyscaleHook with connection_id: {self.conn_id}")

        # Attempt to get the token from the connection
        conn = self.get_connection(self.conn_id)
        token = conn.password

        # If the token is not found in the connection, try to get it from the environment variable
        if not token:
            token = os.getenv("ANYSCALE_CLI_TOKEN")

        if not token:
            raise AirflowException(f"Missing API token for connection id {self.conn_id}")

        self.sdk = Anyscale(auth_token=token)

    @classmethod
    def get_ui_field_behaviour(cls) -> Dict[str, Any]:
        """Return custom field behaviour for the connection form in the UI."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {"password": "Enter API Key here"},
        }

    def submit_job(self, config: JobConfig) -> str:
        """
        Submit a job to Anyscale.

        :param config: Required. Configuration dictionary for the job.
        """
        self.log.info(f"Creating a job with configuration: {config}")
        job_id: str = self.sdk.job.submit(config=config)
        return job_id

    def deploy_service(
        self,
        config: ServiceConfig,
        in_place: bool = False,
        canary_percent: Optional[float] = None,
        max_surge_percent: Optional[float] = None,
    ) -> str:
        """
        Deploy a service to Anyscale.

        :param config: Required. Configuration dictionary for the service.
        :param in_place: Optional. Whether to perform an in-place update. Defaults to False.
        :param canary_percent: Optional. Canary percentage for deployment.
        :param max_surge_percent: Optional. Maximum surge percentage for deployment.
        """
        self.log.info(f"Deploying a service with configuration: {config}")
        service_id: str = self.sdk.service.deploy(
            config=config, in_place=in_place, canary_percent=canary_percent, max_surge_percent=max_surge_percent
        )
        return service_id

    def get_job_status(self, job_id: str) -> JobStatus:
        """
        Fetch the status of a job.

        :param job_id: The ID of the job.
        """
        self.log.info(f"Fetching job status for Job name: {job_id}")
        return self.sdk.job.status(job_id=job_id)

    def get_service_status(self, service_name: str) -> ServiceStatus:
        """
        Fetch the status of a service.

        :param service_name: The name of the service.
        """
        return self.sdk.service.status(name=service_name)

    def terminate_job(self, job_id: str, time_delay: int) -> bool:
        """
        Terminate a running job.

        :param job_id: The ID of the job.
        """
        self.log.info(f"Terminating Job ID: {job_id}")
        try:
            self.sdk.job.terminate(name=job_id)
            # Simulated delay
            time.sleep(time_delay)
        except Exception as e:
            raise AirflowException(f"Job termination failed with error: {e}")
        return True

    def terminate_service(self, service_id: str, time_delay: int) -> bool:
        """
        Terminate a running service.

        :param service_id: The ID of the service.
        :param time_delay:
        """
        self.log.info(f"Terminating Service ID: {service_id}")
        try:
            self.sdk.service.terminate(name=service_id)
            # Simulated delay
            time.sleep(time_delay)
        except Exception as e:
            raise AirflowException(f"Service termination failed with error: {e}")
        return True

    def get_logs(self, job_id: str) -> str:
        """
         Fetch the logs for a job.

        :param job_id: Required. The ID of the job.
        """
        logs: str = self.sdk.job.get_logs(job_id=job_id)
        return logs
