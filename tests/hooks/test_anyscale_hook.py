import json
from unittest import mock
from unittest.mock import patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection
from anyscale import Anyscale
from anyscale.job.models import JobConfig, JobRunStatus, JobState, JobStatus
from anyscale.service.models import ServiceConfig, ServiceState, ServiceStatus

from anyscale_provider.hooks.anyscale import AnyscaleHook

API_KEY = "api_key_value"


@pytest.fixture
def mock_anyscale():
    return Anyscale()


class TestAnyscaleHook:

    def setup_method(self):
        with mock.patch("anyscale_provider.hooks.anyscale.Anyscale"):
            with mock.patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_connection") as m:
                m.return_value = Connection(
                    conn_id="anyscale_default",
                    conn_type="http",
                    host="localhost",
                    password=API_KEY,
                    extra=json.dumps({}),
                )
                self.hook = AnyscaleHook()

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_connection")
    def test_successful_initialization(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id="anyscale_default", conn_type="http", host="localhost", password=API_KEY, extra=json.dumps({})
        )
        hook = AnyscaleHook()
        assert hook.get_connection("anyscale_default").password == API_KEY

    @patch.dict("os.environ", {"ANYSCALE_CLI_TOKEN": "INVALID-TOKEN"})
    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_connection")
    def test_invalid_token(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id="anyscale_default", conn_type="http", host="localhost", password=None, extra=json.dumps({})
        )

        with pytest.raises(AirflowException) as ctx:
            AnyscaleHook().client
        assert str(ctx.value) == "Unable to access Anyscale using the connection <anyscale_default>"

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_connection")
    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_init_with_env_token(self, mock_client, mock_get_connection):
        mock_client.return_value = mock_anyscale
        with mock.patch.dict("os.environ", {"ANYSCALE_CLI_TOKEN": API_KEY}):
            mock_get_connection.return_value = Connection(
                conn_id="anyscale_default",
                conn_type="http",
                host="localhost",
                password=None,  # No password in connection
                extra=json.dumps({}),
            )
            mock_client.auth_token = API_KEY

            hook = AnyscaleHook().client
            assert hook.auth_token == API_KEY

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_submit_job(self, mock_client):
        mock_client.return_value = mock_anyscale
        job_config = JobConfig(name="test_job", entrypoint="python script.py")

        mock_client.job.submit.return_value = "test_job_id"

        result = self.hook.submit_job(job_config)

        mock_client.job.submit.assert_called_once_with(config=job_config)
        assert result == "test_job_id"

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_submit_job_error(self, mock_client):
        mock_client.return_value = mock_anyscale
        job_config = JobConfig(name="test_job", entrypoint="python script.py")

        mock_client.job.submit.side_effect = AirflowException("Submit job failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.submit_job(job_config)

        mock_client.job.submit.assert_called_once_with(config=job_config)
        assert str(exc.value) == "Submit job failed"

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_deploy_service(self, mock_client):
        mock_client.return_value = mock_anyscale
        service_config = ServiceConfig(
            name="test_service", applications=[{"name": "app1", "import_path": "module.optional_submodule:app"}]
        )

        mock_client.service.deploy.return_value = "test_service_id"

        result = self.hook.deploy_service(service_config, in_place=False, canary_percent=10, max_surge_percent=20)

        mock_client.service.deploy.assert_called_once_with(
            config=service_config, in_place=False, canary_percent=10, max_surge_percent=20
        )
        assert result == "test_service_id"

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_deploy_service_error(self, mock_client):
        mock_client.return_value = mock_anyscale
        service_config = ServiceConfig(
            name="test_service", applications=[{"name": "app1", "import_path": "module.optional_submodule:app"}]
        )

        mock_client.service.deploy.side_effect = AirflowException("Deploy service failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.deploy_service(service_config, in_place=False, canary_percent=10, max_surge_percent=20)

        mock_client.service.deploy.assert_called_once_with(
            config=service_config, in_place=False, canary_percent=10, max_surge_percent=20
        )
        assert str(exc.value) == "Deploy service failed"

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_get_job_status(self, mock_client):
        mock_client.return_value = mock_anyscale
        job_config = JobConfig(name="test_job", entrypoint="python script.py")

        # Create a mock SDK instance with a mock job status method
        mock_client.job.status.return_value = JobStatus(
            id="test_job_id",
            name="test_job",
            config=job_config,
            state=JobState.SUCCEEDED,
            runs=[JobRunStatus(name="test", state=JobState.SUCCEEDED)],
        )

        result = self.hook.get_job_status("test_job_id")

        mock_client.job.status.assert_called_once_with(id="test_job_id")
        assert result.id == "test_job_id"
        assert result.state == JobState.SUCCEEDED

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_get_job_status_error(self, mock_client):
        mock_client.return_value = mock_anyscale
        mock_client.job.status.side_effect = AirflowException("Get job status failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.get_job_status("test_job_id")

        mock_client.job.status.assert_called_once_with(id="test_job_id")
        assert str(exc.value) == "Get job status failed"

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_terminate_job(self, mock_client):
        mock_client.return_value = mock_anyscale
        mock_client.job.terminate.return_value = None

        result = self.hook.terminate_job("test_job_id", time_delay=1)

        mock_client.job.terminate.assert_called_once_with(id="test_job_id")
        assert result is True

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_terminate_job_error(self, mock_client):
        mock_client.return_value = mock_anyscale
        mock_client.job.terminate.side_effect = Exception("Terminate job failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.terminate_job("test_job_id", time_delay=1)

        mock_client.job.terminate.assert_called_once_with(id="test_job_id")
        assert str(exc.value) == "Job termination failed with error: Terminate job failed"

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_terminate_service(self, mock_client):
        mock_client.return_value = mock_anyscale
        mock_client.service.terminate.return_value = None

        result = self.hook.terminate_service("test_service", time_delay=1)

        mock_client.service.terminate.assert_called_once_with(name="test_service")
        assert result is True

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_terminate_service_error(self, mock_client):
        mock_client.return_value = mock_anyscale
        mock_client.service.terminate.side_effect = Exception("Terminate service failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.terminate_service("test_service", time_delay=1)
            mock_client.service.terminate.assert_called_once_with(name="test_service")
            assert str(exc.value) == "Service termination failed with error: Terminate service failed"

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_get_job_logs(self, mock_client):
        mock_client.return_value = mock_anyscale
        mock_client.job.get_logs.return_value = "job logs"

        result = self.hook.get_job_logs("test_job_id")

        mock_client.job.get_logs.assert_called_once_with(id="test_job_id", run=None)
        assert result == "job logs"

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    def test_get_job_logs_empty(self, mock_client):
        mock_client.return_value = mock_anyscale
        mock_client.job.get_logs.return_value = ""

        result = self.hook.get_job_logs("test_job_id")

        mock_client.job.get_logs.assert_called_once_with(id="test_job_id", run=None)
        assert result == ""

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_service_status")
    def test_get_service_status(self, mock_get_service_status):
        mock_service_status = ServiceStatus(
            id="test_service_id", name="test_service", query_url="http://example.com", state=ServiceState.RUNNING
        )
        mock_get_service_status.return_value = mock_service_status

        result = self.hook.get_service_status("test_service_name")

        mock_get_service_status.assert_called_once_with("test_service_name")
        assert result.id == "test_service_id"
        assert result.name == "test_service"
        assert result.query_url == "http://example.com"
        assert result.state == ServiceState.RUNNING

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_service_status")
    def test_get_service_status_error(self, mock_get_service_status):
        mock_get_service_status.side_effect = AirflowException("Get service status failed")

        with pytest.raises(AirflowException) as exc:
            self.hook.get_service_status("test_service_name")

        assert str(exc.value) == "Get service status failed"

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    @patch("anyscale_provider.hooks.anyscale.time.sleep", return_value=None)
    def test_terminate_job_with_delay(self, mock_sleep, mock_client):
        mock_client.return_value = mock_anyscale
        with patch.object(self.hook.client.job, "terminate", return_value=None) as mock_terminate:
            result = self.hook.terminate_job("test_job_id", time_delay=1)
            mock_terminate.assert_called_once_with(id="test_job_id")
            mock_sleep.assert_called_once_with(1)
            assert result is True

    @patch("anyscale_provider.hooks.anyscale.AnyscaleHook.client")
    @patch("anyscale_provider.hooks.anyscale.time.sleep", return_value=None)
    def test_terminate_service_with_delay(self, mock_sleep, mock_client):
        mock_client.return_value = mock_anyscale
        with patch.object(self.hook.client.service, "terminate", return_value=None) as mock_terminate:
            result = self.hook.terminate_service("test_service", time_delay=1)
            mock_terminate.assert_called_once_with(name="test_service")
            mock_sleep.assert_called_once_with(1)
            assert result is True
