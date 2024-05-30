
import time
import logging
import asyncio
from datetime import datetime, timedelta

from anyscale.job.models import JobState
from anyscale.service.models import ServiceState

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.compat.functools import cached_property

from anyscale_provider.hooks.anyscale import AnyscaleHook

class AnyscaleJobTrigger(BaseTrigger):

    """
    Triggers and monitors the status of a job submitted to Anyscale.

    This trigger periodically checks the status of a submitted job on Anyscale and 
    yields events based on the job's status. It handles timeouts and errors during 
    the polling process.

    .. seealso::
        For more information on how to use this trigger, take a look at the guide:
        :ref:`howto/trigger:AnyscaleJobTrigger`

    :param conn_id: Required. The connection ID for Anyscale.
    :param job_id: Required. The ID of the job to monitor.
    :param job_start_time: Required. The start time of the job.
    :param poll_interval: Optional. Interval in seconds between status checks. Defaults to 60 seconds.
    :param timeout: Optional. Timeout in seconds for job completion. Defaults to 3600 seconds (1 hour).

    :raises AirflowException: If no job_id is provided or an error occurs during polling.
    """

    def __init__(self, conn_id, job_id, job_start_time, poll_interval=60, timeout=3600):  # Default timeout set to one hour
        super().__init__()
        self.conn_id = conn_id
        self.job_id = job_id
        self.job_start_time = job_start_time
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.end_time = time.time() + self.timeout
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
    
    @cached_property
    def hook(self) -> AnyscaleHook:
        """Return an instance of the AnyscaleHook."""
        return AnyscaleHook(conn_id=self.conn_id)

    def serialize(self):
        return ("anyscale_provider.triggers.anyscale.AnyscaleJobTrigger", {
            "conn_id": self.conn_id,
            "job_id": self.job_id,
            "job_start_time": self.job_start_time,
            "poll_interval": self.poll_interval,
            "timeout": self.timeout
        })

    async def run(self):
        if not self.job_id:
            self.log.info("No job_id provided")
            yield TriggerEvent({"status": "error", "message": "No job_id provided to async trigger", "job_id": self.job_id})
        try:
            while not self.is_terminal_status(self.job_id):
                if time.time() > self.end_time:
                    yield TriggerEvent({
                        "status": "timeout",
                        "message": f"Timeout waiting for job {self.job_id} to complete.",
                        "job_id": self.job_id
                    })
                    return
                
                for log in self.hook.fetch_logs(job_id = self.job_id):
                    self.log.info(log)

                await asyncio.sleep(self.poll_interval)
            # Once out of the loop, the job has reached a terminal status
            job_status = self.get_current_status(self.job_id).state
            self.log.info(f"Current status of the job is {job_status}")
            
            yield TriggerEvent({
                "status": job_status,
                "message": f"Job {self.job_id} completed with status {job_status}.",
                "job_id": self.job_id
            })
        except Exception:
            self.log.exception("An error occurred while polling for job status.")
            yield TriggerEvent({
                "status": JobState.FAILED,
                "message": "An error occurred while polling for job status.",
                "job_id": self.job_id
            })

    def get_current_status(self, job_id):
        return self.hook.get_job_status(job_id=job_id)

    def is_terminal_status(self, job_id):
        job_status = self.get_current_status(job_id)
        self.log.info(f"Current job status for {job_id} is: {job_status.state}")
        return job_status.state not in (JobState.STARTING, JobState.RUNNING)



class AnyscaleServiceTrigger(BaseTrigger):

    """
    Triggers and monitors the status of a service deployment on Anyscale.

    This trigger periodically checks the status of a service deployment on Anyscale
    and yields events based on the service's status. It handles timeouts and errors 
    during the monitoring process.

    .. seealso::
        For more information on how to use this trigger, take a look at the guide:
        :ref:`howto/trigger:AnyscaleServiceTrigger`

    :param conn_id: Required. The connection ID for Anyscale.
    :param service_name: Required. The ID of the service to monitor.
    :param expected_state: Required. The expected final state of the service.
    :param poll_interval: Optional. Interval in seconds between status checks. Defaults to 60 seconds.
    :param timeout: Optional. Timeout in seconds for service to reach the expected state. Defaults to 600 seconds (10 minutes).

    :raises AirflowException: If no service_name is provided or an error occurs during monitoring.
    """

    def __init__(self,
                 conn_id: str,
                 service_name: str,
                 expected_state: str,
                 poll_interval: int = 60,
                 timeout: int = 600):
        super().__init__()
        self.conn_id = conn_id
        self.service_name = service_name
        self.expected_state = expected_state
        self.poll_interval = poll_interval
        self.timeout = timeout

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.end_time = time.time() + timeout

    @cached_property
    def hook(self) -> AnyscaleHook:
        """Return an instance of the AnyscaleHook."""
        return AnyscaleHook(conn_id=self.conn_id)

    def serialize(self):
        return ("anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger", {
            "conn_id": self.conn_id,
            "service_name": self.service_name,
            "expected_state": self.expected_state,
            "poll_interval": self.poll_interval,
            "timeout": self.timeout
        })

    async def run(self):
        
        if not self.service_name:
            self.logger.info("No service_name provided")
            yield TriggerEvent({"status": ServiceState.SYSTEM_FAILURE, "message": "No service_name provided to async trigger", "service_name": self.service_name})

        try:
            self.logger.info(f"Monitoring service {self.service_name} every {self.poll_interval} seconds to reach {self.expected_state}")

            while self.check_current_status(self.service_name):
                if time.time() > self.end_time:
                    yield TriggerEvent({
                        "status": ServiceState.UNKNOWN,
                        "message": f"Service {self.service_name} did not reach {self.expected_state} within the timeout period.",
                        "service_name": self.service_name
                    })
                    return
                
                await asyncio.sleep(self.poll_interval)

            current_state = self.get_current_status(self.service_name).state

            if current_state == ServiceState.RUNNING:
                yield TriggerEvent({"status": ServiceState.RUNNING,
                                    "message":"Service deployment succeeded",
                                    "service_name": self.service_name})
                return
            elif self.expected_state != current_state and not self.check_current_status(self.service_name):
                yield TriggerEvent({
                    "status": ServiceState.SYSTEM_FAILURE,
                    "message": f"Service {self.service_name} entered an unexpected state: {current_state}",
                    "service_name": self.service_name
                })
                return

        except Exception as e:
            self.logger.error("An error occurred during monitoring:", exc_info=True)
            yield TriggerEvent({"status": ServiceState.SYSTEM_FAILURE, "message": str(e),"service_name": self.service_name})
    
    def get_current_status(self, service_name: str):
        return self.hook.get_service_status(service_name)
        
    def check_current_status(self, service_name: str) -> bool:
        job_status = self.get_current_status(service_name)
        self.logger.info(f"Current job status for {service_name} is: {job_status.state}")
        return job_status in (ServiceState.STARTING,ServiceState.UPDATING,ServiceState.ROLLING_OUT, ServiceState.UNHEALTHY)
