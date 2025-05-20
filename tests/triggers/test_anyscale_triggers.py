import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from airflow.exceptions import AirflowNotFoundException
from airflow.triggers.base import TriggerEvent
from anyscale.job.models import JobConfig, JobRunStatus, JobState, JobStatus
from anyscale.service.models import ServiceState

from anyscale_provider.triggers.anyscale import AnyscaleJobTrigger, AnyscaleServiceTrigger


class TestAnyscaleJobTrigger(unittest.TestCase):
    def setUp(self):
        self.trigger = AnyscaleJobTrigger(conn_id="anyscale_default", job_id="123")

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_job_status")
    def test_is_terminal_status(self, mock_get_status):
        mock_get_status.return_value = JobStatus(
            creator_id="Astro",
            state=JobState.SUCCEEDED,
            name="test",
            config=JobConfig(entrypoint="122"),
            id="1",
            runs=[],
        )
        self.assertTrue(self.trigger._is_terminal_state("123"))

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_job_status")
    def test_is_not_terminal_status(self, mock_get_status):
        mock_get_status.return_value = JobStatus(
            creator_id="Astro", state=JobState.RUNNING, name="test", config=JobConfig(entrypoint="122"), id="1", runs=[]
        )
        self.assertFalse(self.trigger._is_terminal_state("123"))

    @patch("asyncio.sleep", return_value=None)
    @patch(
        "anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.get_current_status",
        side_effect=["RUNNING", "RUNNING", "SUCCEEDED"],
    )
    async def test_run_successful_completion(self, mock_get_status, mock_sleep):
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].payload["status"], "SUCCEEDED")

    @patch("time.time", side_effect=[100, 200, 300, 400, 10000])  # Simulating time passing and timeout
    @patch("asyncio.sleep", return_value=None)
    async def test_run_timeout(self, mock_sleep, mock_time):
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].payload["status"], "timeout")

    @patch(
        "anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.is_terminal_status",
        side_effect=Exception("Error occurred"),
    )
    async def test_run_exception(self, mock_is_terminal_status):
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].payload["status"], JobState.FAILED)
        self.assertIn("Error occurred", events[0].payload["message"])

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_job_logs")
    @patch(
        "anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.get_current_status",
        side_effect=["RUNNING", "SUCCEEDED"],
    )
    @patch("asyncio.sleep", return_value=None)
    async def test_run_with_logs(self, mock_sleep, mock_get_status, mock_get_job_logs):
        mock_get_job_logs.return_value = "log line 1\nlog line 2"
        events = []
        async for event in self.trigger.run():
            events.append(event)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].payload["status"], "SUCCEEDED")

    async def test_run_no_job_id_provided(self):
        trigger = AnyscaleJobTrigger(conn_id="default_conn", job_id="")
        events = []
        async for event in trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].payload["status"], "error")
        self.assertIn("No job_id provided to async trigger", events[0].payload["message"])

    @patch("airflow.models.connection.Connection.get_connection_from_secrets")
    def test_hook_method(self, mock_get_connection):
        # Configure the mock to raise AirflowNotFoundException
        mock_get_connection.side_effect = AirflowNotFoundException("The conn_id `default_conn` isn't defined")

        trigger = AnyscaleJobTrigger(conn_id="default_conn", job_id="123")

        with self.assertRaises(AirflowNotFoundException) as context:
            trigger.hook.client

        self.assertIn("The conn_id `default_conn` isn't defined", str(context.exception))

    def test_serialize(self):
        trigger = AnyscaleJobTrigger(conn_id="default_conn", job_id="123")

        result = trigger.serialize()
        expected_output = (
            "anyscale_provider.triggers.anyscale.AnyscaleJobTrigger",
            {"conn_id": "default_conn", "job_id": "123", "poll_interval": 60},
        )

        # Check if the result is a tuple
        self.assertTrue(isinstance(result, tuple))

        # Check if the tuple contains a string and a dictionary
        self.assertTrue(isinstance(result[0], str))
        self.assertTrue(isinstance(result[1], dict))

        # Check if the result matches the expected output
        self.assertEqual(result, expected_output)

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_job_status")
    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_job_logs")
    @patch("asyncio.sleep", return_value=None)
    async def test_anyscale_run_trigger(self, mocked_sleep, mocked_get_job_logs, mocked_get_job_status):
        """Test AnyscaleJobTrigger run method with mocked details."""
        mocked_get_job_status.return_value.state = JobState.SUCCEEDED
        mocked_get_job_logs.return_value = "log line 1\nlog line 2"

        trigger = AnyscaleJobTrigger(
            conn_id="test_conn",
            job_id="1234",
            poll_interval=1,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        self.assertFalse(task.done())

        await asyncio.sleep(2)
        result = await task

        self.assertEqual(result.payload["status"], JobState.SUCCEEDED)
        self.assertEqual(result.payload["message"], "Job 1234 completed with status JobState.SUCCEEDED.")
        self.assertEqual(result.payload["job_id"], "1234")

    @patch("anyscale_provider.triggers.anyscale.AnyscaleHook.get_job_status")
    @patch("anyscale_provider.triggers.anyscale.AnyscaleJobTrigger._is_terminal_state")
    def test_run_success(self, mock_terminal_state, mock_hook):
        trigger = AnyscaleJobTrigger(conn_id="test_conn", job_id="test_job", poll_interval=1, fetch_logs=False)
        mock_terminal_state.return_value = True
        mock_hook.return_value = JobStatus(
            creator_id="Astro",
            id="test_job",
            state=JobState.SUCCEEDED,
            name="",
            config=JobConfig(entrypoint="122"),
            runs=[],
        )

        async def run_test():
            generator = trigger.run()
            result = await generator.asend(None)
            assert result == TriggerEvent(
                {"state": "SUCCEEDED", "message": "Job test_job completed with state SUCCEEDED.", "job_id": "test_job"}
            )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_test())

    @patch("anyscale_provider.triggers.anyscale.asyncio.get_event_loop")
    @patch("anyscale_provider.triggers.anyscale.AnyscaleHook.get_job_status")
    @patch("anyscale_provider.triggers.anyscale.AnyscaleJobTrigger._is_terminal_state")
    def test_run_success_fetch_log(self, mock_terminal_state, mock_hook, mock_asyncio_loop):
        trigger = AnyscaleJobTrigger(conn_id="test_conn", job_id="test_job", poll_interval=1, fetch_logs=True)
        mock_terminal_state.return_value = True
        mock_hook.return_value = JobStatus(
            creator_id="Astro",
            id="test_job",
            state=JobState.SUCCEEDED,
            name="",
            config=JobConfig(entrypoint="122"),
            runs=[JobRunStatus(name="test", state="SUCCEEDED")],
        )
        mock_loop = AsyncMock()
        mock_asyncio_loop.return_value = mock_loop
        mock_loop.run_in_executor.side_effect = "hello\n"

        async def run_test():
            generator = trigger.run()
            result = await generator.asend(None)
            assert result == TriggerEvent(
                {"state": "SUCCEEDED", "message": "Job test_job completed with state SUCCEEDED.", "job_id": "test_job"}
            )
            mock_asyncio_loop.assert_called_once()
            mock_loop.run_in_executor.assert_called_once()
            mock_loop.run_in_executor.return_value = []

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_test())

    @patch("anyscale_provider.triggers.anyscale.AnyscaleHook.get_job_status")
    @patch("anyscale_provider.triggers.anyscale.AnyscaleJobTrigger._is_terminal_state")
    def test_run_error(self, mock_terminal_state, mock_hook):
        trigger = AnyscaleJobTrigger(conn_id="test_conn", job_id="test_job", poll_interval=1, fetch_logs=False)
        mock_terminal_state.return_value = True
        mock_hook.return_value = JobStatus(
            creator_id="Astro",
            id="test_job",
            state=JobState.FAILED,
            name="",
            config=JobConfig(entrypoint="122"),
            runs=[],
        )

        async def run_test():
            generator = trigger.run()
            result = await generator.asend(None)
            assert result == TriggerEvent(
                {"state": "FAILED", "message": "Job test_job completed with state FAILED.", "job_id": "test_job"}
            )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_test())


class TestAnyscaleServiceTrigger(unittest.TestCase):
    def setUp(self):
        self.trigger = AnyscaleServiceTrigger(
            conn_id="default_conn", service_name="service123", expected_state="RUNNING", canary_percent=None
        )

    @patch("anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger._get_current_state")
    def test_check_current_state(self, mock_get_status):
        mock_get_status.return_value = "STARTING"
        self.assertTrue(self.trigger._check_current_state("service123"))

    @patch("asyncio.sleep", return_value=None)
    @patch(
        "anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger.get_current_status",
        side_effect=["STARTING", "UPDATING", "RUNNING"],
    )
    async def test_run_successful(self, mock_get_status, mock_sleep):
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["status"], ServiceState.RUNNING)
        self.assertIn("Service deployment succeeded", events[0]["message"])

    @patch("time.time", side_effect=[100, 200, 300, 400, 10000])  # Simulating time passing and timeout
    @patch("asyncio.sleep", return_value=None)
    async def test_run_timeout(self, mock_sleep, mock_time):
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["status"], ServiceState.UNKNOWN)
        self.assertIn("did not reach RUNNING within the timeout period", events[0]["message"])

    @patch(
        "anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger.check_current_status",
        side_effect=Exception("Error occurred"),
    )
    async def test_run_exception(self, mock_check_current_status):
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["status"], ServiceState.SYSTEM_FAILURE)
        self.assertIn("Error occurred", events[0]["message"])

    @patch("airflow.models.connection.Connection.get_connection_from_secrets")
    def test_hook_method(self, mock_get_connection):
        # Configure the mock to raise AirflowNotFoundException
        mock_get_connection.side_effect = AirflowNotFoundException("The conn_id `default_conn` isn't defined")

        trigger = AnyscaleServiceTrigger(
            conn_id="default_conn", service_name="AstroService", expected_state=ServiceState.RUNNING, canary_percent=0.0
        )

        with self.assertRaises(AirflowNotFoundException) as context:
            trigger.hook.client

        self.assertIn("The conn_id `default_conn` isn't defined", str(context.exception))

    def test_serialize(self):

        trigger = AnyscaleServiceTrigger(
            conn_id="default_conn", service_name="AstroService", expected_state=ServiceState.RUNNING, canary_percent=0.0
        )

        result = trigger.serialize()
        expected_output = (
            "anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger",
            {
                "conn_id": "default_conn",
                "service_name": "AstroService",
                "expected_state": ServiceState.RUNNING,
                "cloud": None,
                "project": None,
                "canary_percent": 0.0,
                "poll_interval": 60,
            },
        )

        # Check if the result is a tuple
        self.assertTrue(isinstance(result, tuple))

        # Check if the tuple contains a string and a dictionary
        self.assertTrue(isinstance(result[0], str))
        self.assertTrue(isinstance(result[1], dict))

        # Check if the result matches the expected output
        self.assertEqual(result, expected_output)

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_service_status")
    def test_get_current_status_canary_0_percent(self, mock_get_service_status):
        # Mock the return value of get_service_status
        mock_service_status = MagicMock()
        mock_service_status.state = ServiceState.RUNNING
        mock_service_status.canary_version.state = ServiceState.RUNNING
        mock_get_service_status.return_value = mock_service_status

        # Initialize the trigger with canary_percent set to 0.0
        trigger = AnyscaleServiceTrigger(
            conn_id="default_conn", service_name="AstroService", expected_state=ServiceState.RUNNING, canary_percent=0.0
        )

        # Mock the hook property to return our mocked hook
        with patch.object(AnyscaleServiceTrigger, "hook", new_callable=PropertyMock) as mock_hook:
            mock_hook.return_value.get_service_status = mock_get_service_status

            # Call the method to test
            status = trigger._get_current_state("AstroService")

            # Verify the result
            self.assertEqual(status, "RUNNING")

            # Ensure the mock was called correctly
            mock_get_service_status.assert_called_once_with("AstroService", cloud=None, project=None)

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_service_status")
    def test_get_current_status_canary_100_percent(self, mock_get_service_status):
        # Mock the return value of get_service_status
        mock_service_status = MagicMock()
        mock_service_status.state = ServiceState.TERMINATED
        mock_service_status.canary_version.state = ServiceState.RUNNING
        mock_get_service_status.return_value = mock_service_status

        # Initialize the trigger with canary_percent set to 100.0
        trigger = AnyscaleServiceTrigger(
            conn_id="default_conn",
            service_name="AstroService",
            expected_state=ServiceState.RUNNING,
            canary_percent=100.0,
        )

        # Mock the hook property to return our mocked hook
        with patch.object(AnyscaleServiceTrigger, "hook", new_callable=PropertyMock) as mock_hook:
            mock_hook.return_value.get_service_status = mock_get_service_status

            # Call the method to test
            status = trigger._get_current_state("AstroService")

            # Verify the result
            self.assertEqual(status, "TERMINATED")

            # Ensure the mock was called correctly
            mock_get_service_status.assert_called_once_with("AstroService", cloud=None, project=None)

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_service_status")
    def test_get_current_status_cloud(self, mock_get_service_status):
        # Mock the return value of get_service_status
        mock_service_status = MagicMock()
        mock_service_status.state = ServiceState.TERMINATED
        mock_service_status.canary_version.state = ServiceState.RUNNING
        mock_get_service_status.return_value = mock_service_status

        # Initialize the trigger with canary_percent set to 100.0
        trigger = AnyscaleServiceTrigger(
            conn_id="default_conn",
            service_name="AstroService",
            expected_state=ServiceState.RUNNING,
            canary_percent=100.0,
            cloud="AstroCloud",
        )

        # Mock the hook property to return our mocked hook
        with patch.object(AnyscaleServiceTrigger, "hook", new_callable=PropertyMock) as mock_hook:
            mock_hook.return_value.get_service_status = mock_get_service_status

            # Call the method to test
            status = trigger._get_current_state("AstroService")

            # Verify the result
            self.assertEqual(status, "TERMINATED")

            # Ensure the mock was called correctly
            mock_get_service_status.assert_called_once_with("AstroService", cloud="AstroCloud", project=None)

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_service_status")
    def test_get_current_status_project(self, mock_get_service_status):
        # Mock the return value of get_service_status
        mock_service_status = MagicMock()
        mock_service_status.state = ServiceState.TERMINATED
        mock_service_status.canary_version.state = ServiceState.RUNNING
        mock_get_service_status.return_value = mock_service_status

        # Initialize the trigger with canary_percent set to 100.0
        trigger = AnyscaleServiceTrigger(
            conn_id="default_conn",
            service_name="AstroService",
            expected_state=ServiceState.RUNNING,
            canary_percent=100.0,
            project="AstroProject",
        )

        # Mock the hook property to return our mocked hook
        with patch.object(AnyscaleServiceTrigger, "hook", new_callable=PropertyMock) as mock_hook:
            mock_hook.return_value.get_service_status = mock_get_service_status

            # Call the method to test
            status = trigger._get_current_state("AstroService")

            # Verify the result
            self.assertEqual(status, "TERMINATED")

            # Ensure the mock was called correctly
            mock_get_service_status.assert_called_once_with("AstroService", cloud=None, project="AstroProject")

    @patch("anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger._get_current_state")
    @patch("anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger._check_current_state")
    def test_run_success(self, mock_check_current_state, mock_get_current_state):
        trigger = AnyscaleServiceTrigger(
            conn_id="default_conn",
            service_name="AstroService",
            expected_state=ServiceState.RUNNING,
            canary_percent=100.0,
        )
        mock_check_current_state.return_value = False
        mock_get_current_state.return_value = ServiceState.RUNNING

        async def run_test():
            generator = trigger.run()
            result = await generator.asend(None)
            assert result == TriggerEvent(
                {
                    "state": ServiceState.RUNNING,
                    "message": "Service deployment succeeded",
                    "service_name": "AstroService",
                }
            )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_test())

    @patch("anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger._get_current_state")
    @patch("anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger._check_current_state")
    def test_run_failure(self, mock_check_current_state, mock_get_current_state):
        trigger = AnyscaleServiceTrigger(
            conn_id="default_conn",
            service_name="AstroService",
            expected_state=ServiceState.RUNNING,
            canary_percent=100.0,
        )
        mock_check_current_state.return_value = False
        mock_get_current_state.return_value = ServiceState.UNKNOWN

        async def run_test():
            generator = trigger.run()
            result = await generator.asend(None)
            assert result == TriggerEvent(
                {
                    "state": ServiceState.SYSTEM_FAILURE,
                    "message": "Service AstroService entered an unexpected state: UNKNOWN",
                    "service_name": "AstroService",
                }
            )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_test())

    @patch("anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger._get_current_state")
    @patch("anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger._check_current_state")
    def test_run_service_exception(self, mock_check_current_state, mock_get_current_state):
        trigger = AnyscaleServiceTrigger(
            conn_id="default_conn",
            service_name="AstroService",
            expected_state=ServiceState.RUNNING,
            canary_percent=100.0,
        )
        mock_check_current_state.return_value = False
        mock_get_current_state.side_effect = Exception("Unknown error")

        async def run_test():
            generator = trigger.run()
            result = await generator.asend(None)
            assert result == TriggerEvent(
                {"state": ServiceState.SYSTEM_FAILURE, "message": "Unknown error", "service_name": "AstroService"}
            )

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_test())


if __name__ == "__main__":
    unittest.main()
