import unittest
from unittest.mock import MagicMock, PropertyMock, patch

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.utils.context import Context
from anyscale.job.models import JobState
from anyscale.service.models import ServiceState

from anyscale_provider.operators.anyscale import RolloutAnyscaleService, SubmitAnyscaleJob
from anyscale_provider.triggers.anyscale import AnyscaleJobTrigger, AnyscaleServiceTrigger


class TestSubmitAnyscaleJob(unittest.TestCase):
    def setUp(self):
        self.operator = SubmitAnyscaleJob(
            conn_id="test_conn",
            name="test_job",
            image_uri="test_image_uri",
            compute_config={},
            working_dir="/test/dir",
            entrypoint="test_entrypoint",
            task_id="submit_job_test",
        )

    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook", new_callable=MagicMock)
    def test_execute_successful(self, mock_hook):
        mock_hook.get_job_status.return_value.state = JobState.SUCCEEDED
        job_result_mock = MagicMock()
        job_result_mock.id = "123"
        mock_hook.submit_job.return_value = "123"

        self.operator.execute(Context(ti=MagicMock()))
        mock_hook.submit_job.assert_called_once()

    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook")
    def test_execute_fail_on_status(self, mock_hook):
        mock_hook.submit_job.return_value = "123"
        mock_hook.get_job_status.return_value.state = JobState.FAILED

        with self.assertRaises(AirflowException) as context:
            self.operator.execute(Context(ti=MagicMock()))

        self.assertTrue("Job 123 failed." in str(context.exception))

    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook")
    def test_on_kill(self, mock_hook):
        self.operator.job_id = "123"
        self.operator.on_kill()
        mock_hook.terminate_job.assert_called_once_with("123", 5)

    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook")
    def test_execute_complete(self, mock_hook):
        event = {"state": JobState.SUCCEEDED, "job_id": "123", "message": "Job completed successfully"}
        self.assertEqual(self.operator.execute_complete(Context(), event), "123")

    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook")
    def test_execute_complete_failure(self, mock_hook):
        event = {"state": JobState.FAILED, "job_id": "123", "message": "Job failed with error"}
        with self.assertRaises(AirflowException) as context:
            self.operator.execute_complete(Context(), event)
        self.assertTrue("Job 123 failed with error" in str(context.exception))

    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook", new_callable=PropertyMock)
    def test_check_anyscale_hook(self, mock_hook_property):
        # Access the hook property
        self.operator.hook.client
        # Verify that the hook property was accessed
        mock_hook_property.assert_called_once()

    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook", new_callable=PropertyMock)
    def test_execute_with_no_hook(self, mock_hook_property):
        # Simulate the hook not being available by raising an AirflowException
        mock_hook_property.side_effect = AirflowException("SDK is not available.")

        # Execute the operator and expect it to raise an AirflowException
        with self.assertRaises(AirflowException) as context:
            self.operator.execute(Context())

        self.assertTrue("SDK is not available." in str(context.exception))

    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook", new_callable=MagicMock)
    def test_job_state_failed(self, mock_hook):
        job_result_mock = MagicMock()
        job_result_mock.id = "123"
        mock_hook.submit_job.return_value = "123"
        mock_hook.get_job_status.return_value.state = JobState.FAILED
        with self.assertRaises(AirflowException) as context:
            self.operator.execute(Context(ti=MagicMock()))
        self.assertTrue("Job 123 failed." in str(context.exception))

    @patch("airflow.models.BaseOperator.defer")
    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook", new_callable=MagicMock)
    def test_defer_job_polling(self, mock_hook, mock_defer):
        # Mock the submit_job method to return a job ID
        mock_hook.submit_job.return_value = "123"
        # Mock the get_job_status method to return a starting state
        mock_hook.get_job_status.return_value.state = JobState.STARTING

        # Call the execute method which internally calls process_job_status and defer_job_polling
        self.operator.execute(Context(ti=MagicMock()))

        # Check that the defer method was called with the correct arguments
        mock_defer.assert_called_once()
        args, kwargs = mock_defer.call_args
        self.assertIsInstance(kwargs["trigger"], AnyscaleJobTrigger)
        self.assertEqual(kwargs["trigger"].job_id, "123")
        self.assertEqual(kwargs["trigger"].conn_id, "test_conn")
        self.assertEqual(kwargs["method_name"], "execute_complete")

    @patch("anyscale_provider.operators.anyscale.JobConfig")
    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook", new_callable=MagicMock)
    def test_extra_job_params(self, mock_hook, mock_job_config):
        # Set up the operator with extra_job_params
        operator = SubmitAnyscaleJob(
            conn_id="test_conn",
            name="test_job",
            entrypoint="test_entrypoint",
            extra_job_params={"ray_version": "2.47.1", "timeout_s": 42},
            task_id="test_extra_params",
        )

        mock_hook.submit_job.return_value = "123"
        mock_hook.get_job_status.return_value.state = JobState.SUCCEEDED

        # Execute the operator
        operator.execute(Context(ti=MagicMock()))

        # Verify that JobConfig was called with the extra parameters
        mock_job_config.assert_called_once()
        call_args = mock_job_config.call_args[1]  # Get keyword arguments

        # Check that the extra_job_params were merged into the job_params
        self.assertEqual(call_args["ray_version"], "2.47.1")
        self.assertEqual(call_args["timeout_s"], 42)
        self.assertEqual(call_args["entrypoint"], "test_entrypoint")
        self.assertEqual(call_args["name"], "test_job")


class TestRolloutAnyscaleService(unittest.TestCase):
    def setUp(self):
        self.operator = RolloutAnyscaleService(
            conn_id="test_conn",
            name="test_service",
            image_uri="test_image_uri",
            working_dir="/test/dir",
            applications=[{"name": "app1", "import_path": "module.optional_submodule:app"}],
            compute_config="config123",
            task_id="rollout_service_test",
            cloud="test_cloud",
            project="test_project",
        )

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook")
    def test_execute_successful(self, mock_hook):
        mock_hook.return_value.deploy_service.return_value = "service123"
        with self.assertRaises(TaskDeferred):
            self.operator.execute(Context(ti=MagicMock()))

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.defer")
    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook", new_callable=MagicMock)
    def test_defer_trigger_called(self, mock_hook, mock_defer):
        mock_hook.return_value.deploy_service.return_value = "service123"

        self.operator.execute(Context(ti=MagicMock()))

        # Extract the actual call arguments
        actual_call_args = mock_defer.call_args

        # Define the expected trigger and method_name
        expected_trigger = AnyscaleServiceTrigger(
            conn_id="test_conn",
            service_name="test_service",
            expected_state=ServiceState.RUNNING,
            canary_percent=None,
            cloud="test_cloud",
            project="test_project",
            poll_interval=60,
        )

        expected_method_name = "execute_complete"

        # Perform individual assertions
        actual_trigger = actual_call_args.kwargs["trigger"]
        actual_method_name = actual_call_args.kwargs["method_name"]

        self.assertEqual(actual_trigger.conn_id, expected_trigger.conn_id)
        self.assertEqual(actual_trigger.service_name, expected_trigger.service_name)
        self.assertEqual(actual_trigger.expected_state, expected_trigger.expected_state)
        self.assertEqual(actual_trigger.cloud, expected_trigger.cloud)
        self.assertEqual(actual_trigger.project, expected_trigger.project)
        self.assertEqual(actual_trigger.poll_interval, expected_trigger.poll_interval)
        self.assertEqual(actual_method_name, expected_method_name)

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook")
    def test_execute_complete_failed(self, mock_hook):
        event = {"state": ServiceState.SYSTEM_FAILURE, "service_name": "service123", "message": "Deployment failed"}
        with self.assertRaises(AirflowException) as cm:
            self.operator.execute_complete(Context(), event)
        self.assertIn("Anyscale service deployment service123 failed with error", str(cm.exception))

    def test_execute_complete_success(self):
        event = {"state": ServiceState.RUNNING, "service_name": "service123", "message": "Deployment succeeded"}
        self.operator.execute_complete(Context(), event)
        self.assertEqual(self.operator.name, "test_service")

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook", new_callable=PropertyMock)
    def test_check_anyscale_hook(self, mock_hook_property):
        self.operator.hook.client
        mock_hook_property.assert_called_once()


if __name__ == "__main__":
    unittest.main()
