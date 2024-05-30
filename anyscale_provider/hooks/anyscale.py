import os
import time
import logging
from typing import Any, Dict

import anyscale
from anyscale import Anyscale
from anyscale.job.models import JobConfig
from anyscale.job.models import JobStatus, JobState
from anyscale.service.models import ServiceConfig, ServiceStatus, ServiceVersionState, ServiceState

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.compat.functools import cached_property
from anyscale.sdk.anyscale_client.models import *

logger = logging.getLogger(__name__)

class AnyscaleHook(BaseHook):

    """
    This hook handles the authentication and session management for Anyscale services.

    .. seealso::
        For more information on how to use this hook, take a look at the guide:
        :ref:`howto/hook:AnyscaleHook`

    :param conn_id: Optional. The connection ID to use for Anyscale. Defaults to "anyscale_default".

    :raises AirflowException: If the API token is missing.

    .. attribute:: conn_name_attr
        :annotation: = "conn_id"
        Name attribute for the connection.

    .. attribute:: default_conn_name
        :annotation: = "anyscale_default"
        Default connection name.

    .. attribute:: conn_type
        :annotation: = "anyscale"
        Type of the connection.

    .. attribute:: hook_name
        :annotation: = "Anyscale"
        Name of the hook.

    .. method:: get_ui_field_behaviour
        Return custom field behaviour for the connection form in the UI.

    .. method:: submit_job
        Submit a job to Anyscale.

        :param config: Required. Configuration dictionary for the job.
        :return: The job ID.
        :rtype: str

    .. method:: deploy_service
        Deploy a service to Anyscale.

        :param config: Required. Configuration dictionary for the service.
        :param in_place: Optional. Whether to perform an in-place update. Defaults to False.
        :param canary_percent: Optional. Canary percentage for deployment.
        :param max_surge_percent: Optional. Maximum surge percentage for deployment.
        :return: The service ID.
        :rtype: str

    .. method:: get_job_status
        Fetch the status of a job.

        :param job_id: Required. The ID of the job.
        :return: The status of the job.
        :rtype: str

    .. method:: get_service_status
        Fetch the status of a service.

        :param service_name: Required. The name of the service.
        :return: The status of the service.
        :rtype: str

    .. method:: terminate_job
        Terminate a running job.

        :param job_id: Required. The ID of the job.
        :return: True if the termination was successful.
        :rtype: bool
        :raises AirflowException: If the job termination fails.

    .. method:: terminate_service
        Terminate a running service.

        :param service_id: Required. The ID of the service.
        :return: True if the termination was successful.
        :rtype: bool
        :raises AirflowException: If the service termination fails.

    .. method:: fetch_logs
        Fetch the logs for a job.

        :param job_id: Required. The ID of the job.
        :return: The logs of the job.
        :rtype: str
"""

    conn_name_attr = "conn_id"
    default_conn_name = "anyscale_default"
    conn_type = "anyscale"
    hook_name = "Anyscale"

    def __init__(self, conn_id: str = default_conn_name, **kwargs: Any) -> None:
        super().__init__()
        self.conn_id = conn_id
        logger.info(f"Initializing AnyscaleHook with connection_id: {self.conn_id}")

        conn = self.get_connection(self.conn_id)
        token = conn.password
        if not token:
            raise AirflowException(f"Missing API token for connection id {self.conn_id}")
        self.sdk = Anyscale(auth_token = token)

    @classmethod
    def get_ui_field_behaviour(cls) -> Dict[str, Any]:
        """Return custom field behaviour for the connection form in the UI."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {"password": "Enter API Key here"},
        }

    # Example job interaction methods using environment authentication
    def submit_job(self, config: dict) -> str:
        logger.info("Creating a job with configuration: {}".format(config))
        job_config = JobConfig(**config)
        job_id = self.sdk.job.submit(job_config)
        return job_id
    
    def deploy_service(self,config: dict,
                       in_place: str = False,
                       canary_percent: int = None,
                       max_surge_percent: int = None) -> str:
        logger.info("Deploying a service with configuration: {}".format(config))
        service_config = ServiceConfig(**config)
        logger.info("Deploying a service with config object: {}".format(service_config))
        service_id = self.sdk.service.deploy(config = service_config,
                                             in_place = in_place,
                                             canary_percent = canary_percent,
                                             max_surge_percent = max_surge_percent)
        return service_id

    def get_job_status(self, job_id: str) -> str:
        logger.info("Fetching job status for Job name: {}".format(job_id))
        return self.sdk.job.status(job_id=job_id)
    
    def get_service_status(self,service_name: str) -> str:
        return self.sdk.service.status(name=service_name)
    
    def terminate_job(self, job_id: str, time_delay: int):
        logger.info(f"Terminating Job ID: {job_id}")
        try:
            job_id = self.sdk.job.terminate(name=job_id)
            # Simulated delay
            time.sleep(time_delay)
        except Exception as e:
            AirflowException(f"Job termination failed with error: {e}")
        return True
    
    def terminate_service(self, service_id: str, time_delay: int):
        logger.info(f"Terminating Service ID: {service_id}")
        try:
            service_id = self.sdk.service.terminate(name=service_id)
            # Simulated delay
            time.sleep(time_delay)
        except Exception as e:
            AirflowException(f"Service termination failed with error: {e}")
        return True
    
    def fetch_logs(self, job_id: str):
        return self.sdk.job.logs(job_id=job_id)

    
