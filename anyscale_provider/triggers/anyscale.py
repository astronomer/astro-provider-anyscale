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

    :param conn_id: Required. The connection ID for Anyscale.
    :param job_id: Required. The ID of the job to monitor.
    :param poll_interval: Optional. Interval in seconds between status checks. Defaults to 60 seconds.
    """

    def __init__(self, conn_id: str, job_id: str, poll_interval: float = 60, fetch_logs: bool = True):
        super().__init__()  # type: ignore[no-untyped-call]
        self.conn_id = conn_id
        self.job_id = job_id
        self.poll_interval = poll_interval
        self.fetch_logs = fetch_logs

    @cached_property
    def hook(self) -> AnyscaleHook:
        """
        Return an instance of the AnyscaleHook.

        :return: AnyscaleHook instance configured with the provided connection ID.
        """
        return AnyscaleHook(conn_id=self.conn_id)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize the trigger configuration for persistence.

        :return: A tuple containing the path to the trigger class and a dictionary of the trigger's parameters.
        """
        return (
            "anyscale_provider.triggers.anyscale.AnyscaleJobTrigger",
            {
                "conn_id": self.conn_id,
                "job_id": self.job_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Monitor the job status periodically until a terminal state is reached or an error occurs.

        :yield: TriggerEvent indicating the current status of the job.
        """
        try:
            # Loop until reach the terminal state
            # TODO: Make this call async
            while not self._is_terminal_state(self.job_id):
                await asyncio.sleep(self.poll_interval)

            if self.fetch_logs:
                job_status = self.hook.get_job_status(self.job_id)
                loop = asyncio.get_event_loop()
                logs = await loop.run_in_executor(
                    None, partial(self.hook.get_job_logs, job_id=self.job_id, run=job_status.runs[-1].name)
                )

                for log in logs.split("\n"):
                    print(log)

            # Once out of the loop, the job has reached a terminal status
            job_status = self.hook.get_job_status(self.job_id)
            job_state = str(job_status.state)
            self.log.info(f"Current job status for {self.job_id} is: {job_state}")
            yield TriggerEvent(
                {
                    "state": job_state,
                    "message": f"Job {self.job_id} completed with state {job_state}.",
                    "job_id": self.job_id,
                }
            )
        except Exception as e:
            yield TriggerEvent(
                {
                    "state": str(JobState.FAILED),
                    "message": str(e),
                    "job_id": self.job_id,
                }
            )

    def _is_terminal_state(self, job_id: str) -> bool:
        """
        Check if the job has reached a terminal state.

        :param job_id: The ID of the job to check the status for.
        :return: True if the job is in a terminal state, False otherwise.
        """
        job_state = self.hook.get_job_status(job_id).state
        self.log.info(f"Current job state for {job_id} is: {job_state}")
        return job_state not in (JobState.STARTING, JobState.RUNNING)


class AnyscaleServiceTrigger(BaseTrigger):
    """
    Triggers and monitors the status of a service deployment on Anyscale.

    This trigger periodically checks the status of a service deployment on Anyscale
    and yields events based on the service's status. It handles timeouts and errors
    during the monitoring process.

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
        poll_interval: float = 60,
    ):
        super().__init__()  # type: ignore[no-untyped-call]
        self.conn_id = conn_id
        self.service_name = service_name
        self.expected_state = expected_state
        self.canary_percent = canary_percent
        self.poll_interval = poll_interval

    @cached_property
    def hook(self) -> AnyscaleHook:
        """
        Return an instance of the AnyscaleHook.

        :return: AnyscaleHook instance configured with the provided connection ID.
        """
        return AnyscaleHook(conn_id=self.conn_id)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize the trigger configuration for persistence.

        :return: A tuple containing the path to the trigger class and a dictionary of the trigger's parameters.
        """
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
        """
        Monitor the service status periodically until the expected state is reached or an error occurs.

        :yield: TriggerEvent indicating the current status of the service.
        """
        self.log.info(
            f"Monitoring service {self.service_name} every {self.poll_interval} seconds to reach {self.expected_state}"
        )
        try:
            while self._check_current_state(self.service_name):
                await asyncio.sleep(self.poll_interval)

            current_state = self._get_current_state(self.service_name)

            if current_state == ServiceState.RUNNING:
                yield TriggerEvent(
                    {
                        "state": ServiceState.RUNNING,
                        "message": "Service deployment succeeded",
                        "service_name": self.service_name,
                    }
                )
                return
            elif self.expected_state != current_state and not self._check_current_state(self.service_name):
                yield TriggerEvent(
                    {
                        "state": ServiceState.SYSTEM_FAILURE,
                        "message": f"Service {self.service_name} entered an unexpected state: {current_state}",
                        "service_name": self.service_name,
                    }
                )
                return

        except Exception as e:
            self.log.error("An error occurred during monitoring:", exc_info=True)
            yield TriggerEvent(
                {"state": ServiceState.SYSTEM_FAILURE, "message": str(e), "service_name": self.service_name}
            )

    def _get_current_state(self, service_name: str) -> str:
        """
        Get the current status of the specified service.

        :param service_name: The name of the service to check the status for.
        :return: The current status of the service.
        """
        service_status = self.hook.get_service_status(service_name)

        if self.canary_percent is None or self.canary_percent == 100.0:
            return str(service_status.state)
        else:
            if service_status.canary_version and service_status.canary_version.state:
                return str(service_status.canary_version.state)
            else:
                return str(service_status.state)

    def _check_current_state(self, service_name: str) -> bool:
        """
        Check if the service is still in a transitional state.

        :param service_name: The name of the service to check the status for.
        :return: True if the service is in a transitional state, False otherwise.
        """
        service_state = self._get_current_state(service_name)
        self.log.info(f"Current service state for {service_name} is: {service_state}")
        return service_state in (
            ServiceState.STARTING,
            ServiceState.UPDATING,
            ServiceState.ROLLING_OUT,
            ServiceState.UNHEALTHY,
        )
