from __future__ import annotations

import os
import time
from functools import cached_property
from typing import Any

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from anyscale import Anyscale
from anyscale.job.models import JobConfig, JobStatus
from anyscale.service.models import ServiceConfig, ServiceStatus


class AnyscaleHook(BaseHook):
    """
    This hook handles authenticating and making calls to the Anyscale SDK

    :param conn_id: Optional. The connection ID to use for Anyscale. Defaults to "anyscale_default".
    """

    conn_name_attr = "conn_id"
    default_conn_name = "anyscale_default"
    conn_type = "anyscale"
    hook_name = "Anyscale"

    def __init__(self, conn_id: str = default_conn_name, **kwargs: Any) -> None:
        super().__init__()
        self.conn_id = conn_id

    @cached_property
    def client(self) -> Anyscale:
        conn = self.get_connection(self.conn_id)
        token = conn.password
        self.log.info(f"Using Anyscale connection_id: {self.conn_id}")

        # If the token is not found in the connection, try to get it from the environment variable
        if not token:
            self.log.info(f"Using token from config or ENV")
            token = conf.get("anyscale", "cli_token", fallback=os.getenv("ANYSCALE_CLI_TOKEN"))

        if not token:
            raise AirflowException(f"Missing API token for connection id {self.conn_id}")

        # Add custom headers if telemetry is enabled - by default telemetry is enabled.
        headers = {}
        telemetry_enabled = conf.getboolean("anyscale", "telemetry_enabled", fallback=True)
        if telemetry_enabled:
            headers["X-Anyscale-Source"] = "airflow"

        return Anyscale(auth_token=token, headers=headers)

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
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
        job_id: str = self.client.job.submit(config=config)
        return job_id

    def deploy_service(
        self,
        config: ServiceConfig,
        in_place: bool = False,
        canary_percent: int | None = None,
        max_surge_percent: int | None = None,
    ) -> str:
        """
        Deploy a service to Anyscale.

        :param config: Required. Configuration dictionary for the service.
        :param in_place: Optional. Whether to perform an in-place update. Defaults to False.
        :param canary_percent: Optional. Canary percentage for deployment.
        :param max_surge_percent: Optional. Maximum surge percentage for deployment.
        """
        self.log.info(f"Deploying a service with configuration: {config}")
        service_id: str = self.client.service.deploy(
            config=config, in_place=in_place, canary_percent=canary_percent, max_surge_percent=max_surge_percent
        )
        return service_id

    def get_job_status(self, job_id: str) -> JobStatus:
        """
        Fetch the status of a job.

        :param job_id: The ID of the job.
        """
        self.log.info(f"Fetching job status for Job ID: {job_id}")
        return self.client.job.status(id=job_id)

    def get_service_status(self, service_name: str) -> ServiceStatus:
        """
        Fetch the status of a service.

        :param service_name: The name of the service.
        """
        self.log.info(f"Fetching service status for Service: {service_name}")
        return self.client.service.status(name=service_name)

    def terminate_job(self, job_id: str, time_delay: int) -> bool:
        """
        Terminate a running job.

        :param job_id: The ID of the job.
        :param time_delay:
        """
        self.log.info(f"Terminating Job ID: {job_id}")
        try:
            self.client.job.terminate(id=job_id)
            # Simulated delay
            time.sleep(time_delay)
        except Exception as e:
            raise AirflowException(f"Job termination failed with error: {e}")
        return True

    def terminate_service(self, service_name: str, time_delay: int) -> bool:
        """
        Terminate a running service.

        :param service_name: The name of the service.
        :param time_delay:
        """
        self.log.info(f"Terminating Service: {service_name}")
        try:
            self.client.service.terminate(name=service_name)
            # Simulated delay
            time.sleep(time_delay)
        except Exception as e:
            raise AirflowException(f"Service termination failed with error: {e}")
        return True

    def get_job_logs(self, job_id: str, run: str | None = None) -> str:
        """
         Fetch the logs for a job.

        :param job_id: Required. The ID of the job.
        """
        self.log.info(f"Fetching logs for Job ID: {job_id} and Run: {run}")
        logs: str = self.client.job.get_logs(id=job_id, run=run)
        return logs
