import json
import pytest
from unittest import mock
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException

from airflow.models import Connection

from anyscale import Anyscale
from anyscale.job.models import JobConfig
from anyscale.job.models import JobStatus, JobState, JobRunStatus
from anyscale.service.models import ServiceConfig, ServiceStatus, ServiceVersionState, ServiceState
from anyscale_provider.hooks.anyscale import AnyscaleHook


API_KEY = "api_key_value"  # Use a mock API key value

class TestAnyscaleHook:

    def setup_method(self):
        with mock.patch("anyscale_provider.hooks.anyscale.Anyscale"):
            with mock.patch("anyscale_provider.hooks.anyscale.AnyscaleHook.get_connection") as m:
                m.return_value = Connection(
                    conn_id='anyscale_default',
                    conn_type='http',
                    host='localhost',
                    password=API_KEY,  # Set the password to the API key
                    extra=json.dumps({})
                )
                self.hook = AnyscaleHook()
    
    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_connection')
    @patch("anyscale_provider.hooks.anyscale.Anyscale")
    def test_api_key_required(self, mock_anyscale, mock_get_connection):
        # Set up the mock connection to return a Connection without the API key
        mock_get_connection.return_value = Connection(
            conn_id='anyscale_default',
            conn_type='http',
            host='localhost',
            password=None,  # Simulate missing password
            extra=json.dumps({})
        )
        with pytest.raises(AirflowException) as ctx:
            AnyscaleHook()
        assert str(ctx.value) == "Missing API token for connection id anyscale_default"

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_connection')
    @patch("anyscale_provider.hooks.anyscale.Anyscale")
    def test_successful_initialization(self, mock_anyscale, mock_get_connection):
        # Set up the mock connection to return a valid Connection
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
        job_config = JobConfig(name="test_job",entrypoint="python script.py")
        mock_submit_job.return_value = {"job_id": "test_job_id"}
        
        result = self.hook.submit_job(job_config)
        
        mock_submit_job.assert_called_once_with(job_config)
        assert result == {"job_id": "test_job_id"}

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_job_status')
    def test_get_job_status(self, mock_get_job_status):
        job_config = JobConfig(name="test_job",entrypoint="python script.py")
        mock_get_job_status.return_value = JobStatus(id = "test_job_id",name="test_job_id",
                                                    config = job_config, state=JobState.SUCCEEDED,
                                                    runs = [JobRunStatus(name="test", state = JobState.SUCCEEDED)] )
        
        result = self.hook.get_job_status("test_job_id")
        
        mock_get_job_status.assert_called_once_with("test_job_id")
        assert result.id == "test_job_id"
        assert result.state == JobState.SUCCEEDED

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.terminate_job')
    def test_terminate_job(self, mock_terminate_job):
        mock_terminate_job.return_value = {"status": "terminated"}
        
        result = self.hook.terminate_job("test_job_id")
        
        mock_terminate_job.assert_called_once_with("test_job_id")
        assert result == {"status": "terminated"}