import time
import logging
from typing import Any, Dict

from anyscale import AnyscaleSDK
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.compat.functools import cached_property
from anyscale.sdk.anyscale_client.models import *

logger = logging.getLogger(__name__)

class AnyscaleHook(BaseHook):
    """
    This hook handles the authentication and session management for Anyscale services.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "anyscale_default"
    conn_type = "anyscale"
    hook_name = "Anyscale"

    def __init__(self, conn_id: str = default_conn_name, **kwargs: Any) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.sdk = None
        self.sdk_params = kwargs
        logger.info(f"Initializing AnyscaleHook with connection_id: {self.conn_id}")

    @classmethod
    def get_ui_field_behaviour(cls) -> Dict[str, Any]:
        """Return custom field behaviour for the connection form in the UI."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {"password": "Enter API Key here"},
        }

    @cached_property
    def conn(self) -> AnyscaleSDK:
        """Lazy load the Anyscale connection object and authenticate."""
        conn = self.get_connection(self.conn_id)
        token = conn.password
        if not token:
            raise AirflowException(f"Missing API token for connection id {self.conn_id}")
        extras = conn.extra_dejson

        try:
            return AnyscaleSDK(auth_token=token, **{**extras, **self.sdk_params})
        except Exception as e:
            message = f"Unable to authenticate with Anyscale cloud: {e}"
            logger.error(message, exc_info=True)
            raise AirflowException(message)

    def get_sdk(self) -> AnyscaleSDK:
        """Return a connected Anyscale SDK instance."""
        if not self.sdk:
            self.sdk = self.conn
        return self.sdk

    def create_job(self, config: dict) -> str:
        """Create a production job on Anyscale."""
        sdk = self.get_sdk()
        prod_job = sdk.create_job(config)
        return prod_job
    
    def fetch_production_job_logs(self, job_id: str):
        logs_list = []
        sdk = self.get_sdk()
        logs = sdk.fetch_production_job_logs(job_id = job_id)
        logger.info(logs)
        if len(logs)>0:
            for line in logs.split("\n"):
                logs_list.append(line)
        return logs_list
    
    def terminate_job(self, job_id: str):
        sdk = self.get_sdk()
        prod_response = sdk.terminate_job(production_job_id = job_id)

        # Sleep 5 seconds to ensure the command is executed on anyscale
        time.sleep(5)
        if prod_response.result.state.goal_state == 'TERMINATED':
            logger.info(f"Job id : {job_id} is being TERMINATED")
            return True
        else:
            return False

    def get_production_job_status(self, job_id: str) -> str:
        """Retrieve the status of a production job."""
        sdk = self.get_sdk()
        return sdk.get_production_job(production_job_id=job_id).result.state.current_state
    
    def rollout_service(self,apply_service_model: ApplyServiceModel) -> str:
        sdk = self.get_sdk()
        return sdk.rollout_service(apply_service_model=apply_service_model)
    
    def get_service_status(self,service_id: str) -> str:
        sdk = self.get_sdk()
        return sdk.get_service(service_id).result.current_state
