import json
import pytest
from unittest import mock
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException
from airflow.models import Connection
from anyscale.job.models import JobConfig, JobStatus, JobState, JobRunStatus
from anyscale.service.models import ServiceConfig, ServiceStatus, ServiceState
from anyscale_provider.hooks.anyscale import AnyscaleHook

API_KEY = "api_key_value"

class TestAnyscaleHook:

    def setup_method(self):
        with mock.patch("anyscale_provider.hooks.anyscale.Anyscale"):
            with mock.patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_connection") as m:
                m.return_value = Connection(
                    conn_id='anyscale_default',
                    conn_type='http',
                    host='localhost',
                    password=API_KEY,
                    extra=json.dumps({})
                )
                self.hook = AnyscaleHook()

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_connection')
    @patch("anyscale_provider.hooks.anyscale.Anyscale")
    def test_api_key_required(self, mock_anyscale, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id='anyscale_default',
            conn_type='http',
            host='localhost',
            password=None,
            extra=json.dumps({})
        )
        with pytest.raises(AirflowException) as ctx:
            AnyscaleHook()
        assert str(ctx.value) == "Missing API token for connection id anyscale_default"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_connection')
    @patch("anyscale_provider.hooks.anyscale.Anyscale")
    def test_successful_initialization(self, mock_anyscale, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id='anyscale_default',
            conn_type='http',
            host='localhost',
            password=API_KEY,
            extra=json.dumps({})
        )
        hook = AnyscaleHook()
        assert hook.get_connection('anyscale_default').password == API_KEY

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.submit_job')
    def test_submit_job(self, mock_submit_job):
        job_config = JobConfig(name="test_job", entrypoint="python script.py")
        mock_submit_job.return_value = "test_job_id"

        result = self.hook.submit_job(job_config)

        mock_submit_job.assert_called_once_with(job_config)
        assert result == "test_job_id"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.submit_job')
    def test_submit_job_error(self, mock_submit_job):
        job_config = JobConfig(name="test_job", entrypoint="python script.py")
        mock_submit_job.side_effect = AirflowException("Submit job failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.submit_job(job_config)

        assert str(exc.value) == "Submit job failed"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.deploy_service')
    def test_deploy_service(self, mock_deploy_service):
        service_config = ServiceConfig(name="test_service", applications=[{"name": "app1", "import_path": "module.optional_submodule:app"}])
        mock_deploy_service.return_value = "test_service_id"

        result = self.hook.deploy_service(service_config, in_place=False, canary_percent=10, max_surge_percent=20)

        mock_deploy_service.assert_called_once_with(service_config, in_place=False, canary_percent=10, max_surge_percent=20)
        assert result == "test_service_id"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.deploy_service')
    def test_deploy_service_error(self, mock_deploy_service):
        service_config = ServiceConfig(name="test_service", applications=[{"name": "app1", "import_path": "module.optional_submodule:app"}])
        mock_deploy_service.side_effect = AirflowException("Deploy service failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.deploy_service(service_config, in_place=False, canary_percent=10, max_surge_percent=20)

        assert str(exc.value) == "Deploy service failed"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_job_status')
    def test_get_job_status(self, mock_get_job_status):
        job_config = JobConfig(name="test_job", entrypoint="python script.py")
        mock_get_job_status.return_value = JobStatus(id="test_job_id", name="test_job_id",
                                                     config=job_config, state=JobState.SUCCEEDED,
                                                     runs=[JobRunStatus(name="test", state=JobState.SUCCEEDED)])

        result = self.hook.get_job_status("test_job_id")

        mock_get_job_status.assert_called_once_with("test_job_id")
        assert result.id == "test_job_id"
        assert result.state == JobState.SUCCEEDED

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_job_status')
    def test_get_job_status_error(self, mock_get_job_status):
        mock_get_job_status.side_effect = AirflowException("Get job status failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.get_job_status("test_job_id")

        assert str(exc.value) == "Get job status failed"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.terminate_job')
    def test_terminate_job(self, mock_terminate_job):
        mock_terminate_job.return_value = True

        result = self.hook.terminate_job("test_job_id", time_delay=1)

        mock_terminate_job.assert_called_once_with("test_job_id", time_delay=1)
        assert result is True

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.terminate_job')
    def test_terminate_job_error(self, mock_terminate_job):
        mock_terminate_job.side_effect = AirflowException("Job termination failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.terminate_job("test_job_id", time_delay=1)

        assert str(exc.value) == "Job termination failed"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.terminate_service')
    def test_terminate_service(self, mock_terminate_service):
        mock_terminate_service.return_value = True

        result = self.hook.terminate_service("test_service_id", time_delay=1)

        mock_terminate_service.assert_called_once_with("test_service_id", time_delay=1)
        assert result is True

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.terminate_service')
    def test_terminate_service_error(self, mock_terminate_service):
        mock_terminate_service.side_effect = AirflowException("Service termination failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.terminate_service("test_service_id", time_delay=1)

        assert str(exc.value) == "Service termination failed"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_logs')
    def test_get_logs(self, mock_get_logs):
        mock_get_logs.return_value = "job logs"

        result = self.hook.get_logs("test_job_id")

        mock_get_logs.assert_called_once_with("test_job_id")
        assert result == "job logs"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_logs')
    def test_get_logs_empty(self, mock_get_logs):
        mock_get_logs.return_value = ""

        result = self.hook.get_logs("test_job_id")

        mock_get_logs.assert_called_once_with("test_job_id")
        assert result == ""

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_logs')
    def test_get_logs_error(self, mock_get_logs):
        mock_get_logs.side_effect = AirflowException("Failed to get logs")

        with pytest.raises(AirflowException) as exc:
            self.hook.get_logs("test_job_id")

        assert str(exc.value) == "Failed to get logs"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_service_status')
    def test_get_service_status(self, mock_get_service_status):
        mock_service_status = ServiceStatus(id="test_service_id", name="test_service", query_url="http://example.com", state=ServiceState.RUNNING)
        mock_get_service_status.return_value = mock_service_status

        result = self.hook.get_service_status("test_service_name")

        mock_get_service_status.assert_called_once_with("test_service_name")
        assert result.id == "test_service_id"
        assert result.name == "test_service"
        assert result.query_url == "http://example.com"
        assert result.state == ServiceState.RUNNING

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_service_status')
    def test_get_service_status_error(self, mock_get_service_status):
        mock_get_service_status.side_effect = AirflowException("Get service status failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.get_service_status("test_service_name")

        assert str(exc.value) == "Get service status failed"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_connection')
    @patch("anyscale_provider.hooks.anyscale.Anyscale")
    def test_init_with_env_token(self, mock_anyscale, mock_get_connection):
        with mock.patch.dict("os.environ", {"ANYSCALE_CLI_TOKEN": API_KEY}):
            mock_get_connection.return_value = Connection(
                conn_id='anyscale_default',
                conn_type='http',
                host='localhost',
                password=None,  # No password in connection
                extra=json.dumps({})
            )
            # Mock the Anyscale class to return an instance with the expected auth_token
            mock_instance = mock_anyscale.return_value
            mock_instance.auth_token = API_KEY
            
            hook = AnyscaleHook()
            assert hook.sdk.auth_token == API_KEY

    @patch("anyscale_provider.hooks.anyscale.time.sleep", return_value=None)
    def test_terminate_job_with_delay(self, mock_sleep):
        with patch.object(self.hook.sdk.job, 'terminate', return_value=None) as mock_terminate:
            result = self.hook.terminate_job("test_job_id", time_delay=1)
            mock_terminate.assert_called_once_with(name="test_job_id")
            mock_sleep.assert_called_once_with(1)
            assert result is True

    @patch("anyscale_provider.hooks.anyscale.time.sleep", return_value=None)
    def test_terminate_service_with_delay(self, mock_sleep):
        with patch.object(self.hook.sdk.service, 'terminate', return_value=None) as mock_terminate:
            result = self.hook.terminate_service("test_service_id", time_delay=1)
            mock_terminate.assert_called_once_with(name="test_service_id")
            mock_sleep.assert_called_once_with(1)
            assert result is True