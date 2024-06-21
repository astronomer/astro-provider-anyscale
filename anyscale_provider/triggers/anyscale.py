from __future__ import annotations

import asyncio
from functools import partial
from typing import Any, AsyncIterator

from airflow.compat.functools import cached_property
from airflow.triggers.base import BaseTrigger, TriggerEvent
from anyscale.job.models import JobState
from anyscale.service.models import ServiceState

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
    """

    def __init__(self, conn_id: str, job_id: str, job_start_time: float, poll_interval: int = 60):
        super().__init__()  # type: ignore[no-untyped-call]
        self.conn_id = conn_id
        self.job_id = job_id
        self.job_start_time = job_start_time
        self.poll_interval = poll_interval

    @cached_property
    def hook(self) -> AnyscaleHook:
        """Return an instance of the AnyscaleHook."""
        return AnyscaleHook(conn_id=self.conn_id)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "anyscale_provider.triggers.anyscale.AnyscaleJobTrigger",
            {
                "conn_id": self.conn_id,
                "job_id": self.job_id,
                "job_start_time": self.job_start_time,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:

        try:
            # Loop until reach the terminal state
            # TODO: Make this call async
            while not self._is_terminal_status(self.job_id):
                await asyncio.sleep(self.poll_interval)

            # Fetch and print logs
            loop = asyncio.get_running_loop()
            logs = await loop.run_in_executor(None, partial(self.hook.get_logs, job_id=self.job_id))
            for log in logs.split("\n"):
                self.log.info(log)

            # Once out of the loop, the job has reached a terminal status
            job_status = str(self.hook.get_job_status(self.job_id).state)
            self.log.info(f"Current status of the job is {job_status}")
            yield TriggerEvent(
                {
                    "status": job_status,
                    "message": f"Job {self.job_id} completed with status {job_status}.",
                    "job_id": self.job_id,
                }
            )
        except Exception as e:
            yield TriggerEvent(
                {
                    "status": JobState.FAILED,
                    "message": str(e),
                    "job_id": self.job_id,
                }
            )

    def _is_terminal_status(self, job_id: str) -> bool:
        job_status = str(self.hook.get_job_status(job_id).state)
        self.log.info(f"Current job status for {job_id} is: {job_status}")
        return job_status not in (JobState.STARTING, JobState.RUNNING)


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
    """

    def __init__(
        self,
        conn_id: str,
        service_name: str,
        expected_state: str,
        canary_percent: float | None,
        poll_interval: int = 60,
    ):
        super().__init__()  # type: ignore[no-untyped-call]
        self.conn_id = conn_id
        self.service_name = service_name
        self.expected_state = expected_state
        self.canary_percent = canary_percent
        self.poll_interval = poll_interval

    @cached_property
    def hook(self) -> AnyscaleHook:
        """Return an instance of the AnyscaleHook."""
        return AnyscaleHook(conn_id=self.conn_id)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger",
            {
                "conn_id": self.conn_id,
                "service_name": self.service_name,
                "expected_state": self.expected_state,
                "canary_percent": self.canary_percent,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            while self._check_current_status(self.service_name):
                await asyncio.sleep(self.poll_interval)

            current_state = self._get_current_status(self.service_name)

            if current_state == ServiceState.RUNNING:
                yield TriggerEvent(
                    {
                        "status": ServiceState.RUNNING,
                        "message": "Service deployment succeeded",
                        "service_name": self.service_name,
                    }
                )
                return
            elif self.expected_state != current_state and not self._check_current_status(self.service_name):
                yield TriggerEvent(
                    {
                        "status": ServiceState.SYSTEM_FAILURE,
                        "message": f"Service {self.service_name} entered an unexpected state: {current_state}",
                        "service_name": self.service_name,
                    }
                )
                return

        except Exception as e:
            self.log.error("An error occurred during monitoring:", exc_info=True)
            yield TriggerEvent(
                {"status": ServiceState.SYSTEM_FAILURE, "message": str(e), "service_name": self.service_name}
            )

    def _get_current_status(self, service_name: str) -> str:
        service_status = self.hook.get_service_status(service_name)

        if self.canary_percent is None or self.canary_percent == 100.0:
            return str(service_status.state)
        else:
            if service_status.canary_version and service_status.canary_version.state:
                return str(service_status.canary_version.state)
            else:
                return str(service_status.state)

    def _check_current_status(self, service_name: str) -> bool:
        job_status = self._get_current_status(service_name)
        self.log.info(f"Current job status for {service_name} is: {job_status}")
        return job_status in (
            ServiceState.STARTING,
            ServiceState.UPDATING,
            ServiceState.ROLLING_OUT,
            ServiceState.UNHEALTHY,
        )
