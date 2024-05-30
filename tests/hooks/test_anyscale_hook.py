import unittest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException
from anyscale_provider.hooks.anyscale import AnyscaleHook

class TestAnyscaleHook(unittest.TestCase):

    def setUp(self):
        self.hook = AnyscaleHook(conn_id='test_conn')

    @patch.object(AnyscaleHook, 'get_sdk')
    def test_submit_job(self, mock_get_sdk):
        config = {'job_type': 'Production'}
        mock_get_sdk.return_value.job.submit.return_value = 'job1234'
        job_id = self.hook.submit_job(config)
        self.assertEqual(job_id, 'job1234')

    @patch.object(AnyscaleHook, 'get_sdk')
    def test_deploy_service(self, mock_get_sdk):
        config = {'service_type': 'Production'}
        mock_get_sdk.return_value.service.deploy.return_value = 'service1234'
        service_id = self.hook.deploy_service(config)
        self.assertEqual(service_id, 'service1234')

    @patch.object(AnyscaleHook, 'get_sdk')
    def test_get_job_status(self, mock_get_sdk):
        mock_get_sdk.return_value.job.status.return_value = 'Running'
        status = self.hook.get_job_status('job1234')
        self.assertEqual(status, 'Running')

    @patch.object(AnyscaleHook, 'get_sdk')
    def test_get_service_status(self, mock_get_sdk):
        mock_get_sdk.return_value.service.status.return_value = 'Running'
        status = self.hook.get_service_status('service_name')
        self.assertEqual(status, 'Running')

    @patch.object(AnyscaleHook, 'get_sdk')
    def test_terminate_job_success(self, mock_get_sdk):
        mock_get_sdk.return_value.job.terminate.return_value = 'job1234'
        result = self.hook.terminate_job('job1234', time_delay=1)
        self.assertTrue(result)

    @patch.object(AnyscaleHook, 'get_sdk')
    def test_terminate_service_success(self, mock_get_sdk):
        mock_get_sdk.return_value.service.terminate.return_value = 'service1234'
        result = self.hook.terminate_service('service1234', time_delay=1)
        self.assertTrue(result)

    @patch.object(AnyscaleHook, 'get_sdk')
    def test_fetch_logs(self, mock_get_sdk):
        logs_output = "Log line 1\nLog line 2"
        mock_get_sdk.return_value.job.logs.return_value = logs_output
        logs = self.hook.fetch_logs('job1234')
        self.assertEqual(len(logs.split('\n')), 2)
        self.assertEqual(logs, "Log line 1\nLog line 2")