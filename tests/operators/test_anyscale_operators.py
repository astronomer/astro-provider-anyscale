import unittest
from unittest.mock import MagicMock, PropertyMock, patch

from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from anyscale.job.models import JobState
from anyscale.service.models import ServiceState

from anyscale_provider.operators.anyscale import RolloutAnyscaleService, SubmitAnyscaleJob


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
            do_xcom_push=False,
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
    def test_execute_failure(self, mock_hook):
        # This test ensures that when a job submission returns an UNKNOWN state, it raises an exception
        mock_hook.submit_job.return_value = "123"
        mock_hook.get_job_status.return_value.state = JobState.UNKNOWN

        with self.assertRaises(AirflowException) as context:
            self.operator.execute(Context(ti=MagicMock()))

        self.assertTrue("Job 123 failed." in str(context.exception))

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
            do_xcom_push=False,
        )

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook")
    def test_execute_successful(self, mock_hook):
        # Mock successful service deployment
        mock_hook.deploy_service.return_value = "service123"
        mock_hook.get_service_status.return_value.state = ServiceState.RUNNING

        result = self.operator.execute(Context(ti=MagicMock()))

        # Verify deploy_service was called
        mock_hook.deploy_service.assert_called_once()
        # Verify the service_id was returned
        self.assertEqual(result, "service123")

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook")
    def test_execute_failed(self, mock_hook):
        # Mock service deployment that results in a failure state
        mock_hook.deploy_service.return_value = "service123"
        mock_hook.get_service_status.return_value.state = ServiceState.SYSTEM_FAILURE

        with self.assertRaises(AirflowException) as context:
            self.operator.execute(Context(ti=MagicMock()))

        self.assertTrue("Service test_service failed." in str(context.exception))

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook", new_callable=PropertyMock)
    def test_check_anyscale_hook(self, mock_hook_property):
        self.operator.hook.client
        mock_hook_property.assert_called_once()


if __name__ == "__main__":
    unittest.main()
