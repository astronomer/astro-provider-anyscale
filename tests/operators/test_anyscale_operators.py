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
        mock_hook_property.side_effect = AirflowException("SDK is not available.")

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

        operator.execute(Context(ti=MagicMock()))

        mock_job_config.assert_called_once()
        call_args = mock_job_config.call_args[1]

        self.assertEqual(call_args["ray_version"], "2.47.1")
        self.assertEqual(call_args["timeout_s"], 42)
        self.assertEqual(call_args["entrypoint"], "test_entrypoint")
        self.assertEqual(call_args["name"], "test_job")

    @patch("anyscale_provider.operators.anyscale.time.sleep")
    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook")
    def test_execute_job_timeout(self, mock_hook, mock_sleep):
        operator = SubmitAnyscaleJob(
            conn_id="test_conn",
            name="test_job",
            entrypoint="test_entrypoint",
            task_id="test_timeout",
            job_timeout_seconds=120,  # 2 minutes
            poll_interval=60,  # 1 minute
        )

        mock_hook.submit_job.return_value = "123"
        mock_hook.get_job_status.return_value.state = JobState.RUNNING

        with self.assertRaises(AirflowException) as context:
            operator.execute(Context(ti=MagicMock()))

        self.assertTrue("Job 123 was not completed after 120" in str(context.exception))

    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook")
    def test_execute_without_wait_for_completion_success(self, mock_hook):
        operator = SubmitAnyscaleJob(
            conn_id="test_conn",
            name="test_job",
            entrypoint="test_entrypoint",
            task_id="test_no_wait",
            wait_for_completion=False,
        )

        mock_hook.submit_job.return_value = "123"
        mock_hook.get_job_status.return_value.state = JobState.SUCCEEDED

        result = operator.execute(Context(ti=MagicMock()))

        self.assertEqual(result, "123")
        mock_hook.submit_job.assert_called_once()
        mock_hook.get_job_status.assert_called_once_with("123")

    @patch("anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook")
    def test_execute_without_wait_for_completion_failure(self, mock_hook):
        operator = SubmitAnyscaleJob(
            conn_id="test_conn",
            name="test_job",
            entrypoint="test_entrypoint",
            task_id="test_no_wait_fail",
            wait_for_completion=False,
        )

        mock_hook.submit_job.return_value = "123"
        mock_hook.get_job_status.return_value.state = JobState.FAILED

        with self.assertRaises(AirflowException) as context:
            operator.execute(Context(ti=MagicMock()))

        self.assertTrue("Job 123 failed." in str(context.exception))


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
        mock_hook.deploy_service.return_value = "service123"
        mock_hook.get_service_status.return_value.state = ServiceState.RUNNING

        result = self.operator.execute(Context(ti=MagicMock()))

        mock_hook.deploy_service.assert_called_once()
        self.assertEqual(result, "service123")

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook")
    def test_execute_failed(self, mock_hook):
        mock_hook.deploy_service.return_value = "service123"
        mock_hook.get_service_status.return_value.state = ServiceState.SYSTEM_FAILURE

        with self.assertRaises(AirflowException) as context:
            self.operator.execute(Context(ti=MagicMock()))

        self.assertTrue("Service test_service failed." in str(context.exception))

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook", new_callable=PropertyMock)
    def test_check_anyscale_hook(self, mock_hook_property):
        self.operator.hook.client
        mock_hook_property.assert_called_once()

    @patch("anyscale_provider.operators.anyscale.time.sleep")
    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook")
    def test_execute_service_timeout(self, mock_hook, mock_sleep):
        # Test that service times out when it doesn't complete within service_rollout_timeout_seconds
        operator = RolloutAnyscaleService(
            conn_id="test_conn",
            name="test_service",
            applications=[{"name": "app1", "import_path": "module.optional_submodule:app"}],
            task_id="test_timeout",
            service_rollout_timeout_seconds=120,  # 2 minutes
            poll_interval=60,  # 1 minute
        )

        mock_hook.deploy_service.return_value = "service123"
        # Service stays in STARTING state and never completes
        mock_hook.get_service_status.return_value.state = ServiceState.STARTING

        with self.assertRaises(AirflowException) as context:
            operator.execute(Context(ti=MagicMock()))

        self.assertTrue("Service test_service was not completed after 120" in str(context.exception))

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook")
    def test_execute_without_wait_for_completion_success(self, mock_hook):
        operator = RolloutAnyscaleService(
            conn_id="test_conn",
            name="test_service",
            applications=[{"name": "app1", "import_path": "module.optional_submodule:app"}],
            task_id="test_no_wait",
            wait_for_completion=False,
        )

        mock_hook.deploy_service.return_value = "service123"
        mock_hook.get_service_status.return_value.state = ServiceState.RUNNING

        result = operator.execute(Context(ti=MagicMock()))

        self.assertEqual(result, "service123")
        mock_hook.deploy_service.assert_called_once()
        mock_hook.get_service_status.assert_called_once_with(service_name="test_service")

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook")
    def test_execute_without_wait_for_completion_failure(self, mock_hook):
        operator = RolloutAnyscaleService(
            conn_id="test_conn",
            name="test_service",
            applications=[{"name": "app1", "import_path": "module.optional_submodule:app"}],
            task_id="test_no_wait_fail",
            wait_for_completion=False,
        )

        mock_hook.deploy_service.return_value = "service123"
        mock_hook.get_service_status.return_value.state = ServiceState.SYSTEM_FAILURE

        with self.assertRaises(AirflowException) as context:
            operator.execute(Context(ti=MagicMock()))

        self.assertTrue("Service test_service failed." in str(context.exception))

    @patch("anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook")
    def test_on_kill(self, mock_hook):
        # Test that on_kill properly terminates the service
        self.operator.on_kill()
        mock_hook.terminate_service.assert_called_once_with("test_service", 5)


if __name__ == "__main__":
    unittest.main()
