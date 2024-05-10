
import time
import logging
import asyncio
from datetime import datetime, timedelta

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.compat.functools import cached_property

from anyscale import AnyscaleSDK
from anyscale_provider.hooks.anyscale import AnyscaleHook

class AnyscaleJobTrigger(BaseTrigger):
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
            "job_start_time": self.job_start_time.isoformat(),
            "poll_interval": self.poll_interval,
            "timeout": self.timeout
        })

    async def run(self):
        if not self.job_id:
            self.logger.info("No job_id provided")
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
                await asyncio.sleep(self.poll_interval)
            # Once out of the loop, the job has reached a terminal status
            job_status = self.get_current_status(self.job_id)
            self.logger.info(f"Current status of the job is {job_status}")
            
            yield TriggerEvent({
                "status": job_status,
                "message": f"Job {self.job_id} completed with status {job_status}.",
                "job_id": self.job_id
            })
        except Exception:
            self.logger.exception("An error occurred while polling for job status.")
            yield TriggerEvent({
                "status": "error",
                "message": "An error occurred while polling for job status.",
                "job_id": self.job_id
            })

    def get_current_status(self, job_id):
        return self.hook.get_production_job_status(job_id=job_id)

    def is_terminal_status(self, job_id):
        job_status = self.get_current_status(job_id)
        self.logger.info(f"Current job status for {job_id} is: {job_status}")
        return job_status not in ('RUNNING', 'PENDING', 'AWAITING_CLUSTER_START', 'RESTARTING')



class AnyscaleServiceTrigger(BaseTrigger):
    def __init__(self,
                 conn_id: str,
                 service_id: str,
                 expected_state: str,
                 poll_interval: int = 60,
                 timeout: int = 600):
        super().__init__()
        self.conn_id = conn_id
        self.service_id = service_id
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
            "service_id": self.service_id,
            "expected_state": self.expected_state,
            "poll_interval": self.poll_interval,
            "timeout": self.timeout
        })

    async def run(self):
        
        if not self.service_id:
            self.logger.info("No service_id provided")
            yield TriggerEvent({"status": "error", "message": "No service_id provided to async trigger", "service_id": self.service_id})

        try:
            self.logger.info(f"Monitoring service {self.service_id} every {self.poll_interval} seconds to reach {self.expected_state}")

            while self.check_current_status(self.service_id):
                if time.time() > self.end_time:
                    yield TriggerEvent({
                        "status": "timeout",
                        "message": f"Service {self.service_id} did not reach {self.expected_state} within the timeout period.",
                        "service_id": self.service_id
                    })
                    return
                
                await asyncio.sleep(self.poll_interval)

            current_state = self.get_current_status(self.service_id)

            if current_state == 'RUNNING':
                yield TriggerEvent({"status": "success",
                                    "message":"Service deployment succeeded",
                                    "service_id": self.service_id})
                return
            elif self.expected_state != current_state and not self.check_current_status(self.service_id):
                yield TriggerEvent({
                    "status": "failed",
                    "message": f"Service {self.service_id} entered an unexpected state: {current_state}",
                    "service_id": self.service_id
                })
                return

        except Exception as e:
            self.logger.error("An error occurred during monitoring:", exc_info=True)
            yield TriggerEvent({"status": "error", "message": str(e),"service_id": self.service_id})
    
    def get_current_status(self, service_id: str):
        return self.hook.get_service_status(service_id)
        
    def check_current_status(self, service_id: str) -> bool:
        job_status = self.get_current_status(service_id)
        self.logger.info(f"Current job status for {service_id} is: {job_status}")
        return job_status in ('STARTING','UPDATING','ROLLING_OUT')
