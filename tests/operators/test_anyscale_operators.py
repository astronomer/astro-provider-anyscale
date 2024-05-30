import unittest
from unittest.mock import patch, MagicMock
from airflow.utils.context import Context
from airflow.exceptions import AirflowException, TaskDeferred
from anyscale.job.models import JobState
from anyscale.service.models import ServiceState
from anyscale_provider.operators.anyscale import SubmitAnyscaleJob
from anyscale_provider.operators.anyscale import RolloutAnyscaleService
from anyscale_provider.triggers.anyscale import AnyscaleServiceTrigger


class TestSubmitAnyscaleJob(unittest.TestCase):
    def setUp(self):
        self.operator = SubmitAnyscaleJob(
            conn_id='test_conn', 
            name='test_job', 
            image_uri='test_image_uri', 
            compute_config={}, 
            working_dir='/test/dir', 
            entrypoint='test_entrypoint', 
            task_id='submit_job_test'
        )

    @patch('include.operators.anyscale.SubmitAnyscaleJob.get_current_status')
    @patch('include.operators.anyscale.SubmitAnyscaleJob.hook', new_callable=MagicMock)
    def test_execute_successful(self, mock_hook, mock_get_status):
        job_result_mock = MagicMock()
        job_result_mock.id = '123'
        mock_hook.submit_job.return_value = '123'
        mock_get_status.return_value = JobState.SUCCEEDED
        
        job_id = self.operator.execute(Context())
        
        mock_hook.submit_job.assert_called_once()  
        mock_get_status.assert_called_with('123')  
        self.assertEqual(job_id, '123')  

    @patch('include.operators.anyscale.SubmitAnyscaleJob.process_job_status')
    @patch('include.operators.anyscale.SubmitAnyscaleJob.get_current_status')
    @patch('include.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_execute_fail_on_status(self, mock_hook, mock_get_current_status, mock_process_job_status):
        mock_hook.submit_job.return_value = '123'
        mock_get_current_status.return_value = JobState.FAILED
        mock_process_job_status.side_effect = AirflowException("Job 123 failed.")
        
        with self.assertRaises(AirflowException) as context:
            self.operator.execute(Context())
        
        self.assertTrue("Job 123 failed." in str(context.exception))

    @patch('include.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_on_kill(self, mock_hook):
        self.operator.job_id = '123'
        self.operator.on_kill()        
        mock_hook.terminate_job.assert_called_once_with('123', 5)

    @patch('include.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_process_job_status_unexpected_state(self, mock_hook):
        with self.assertRaises(Exception):
            self.operator.process_job_status(None, 'UNKNOWN_STATE')

    @patch('include.operators.anyscale.SubmitAnyscaleJob.defer_job_polling')
    @patch('include.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_defer_job_polling_called(self, mock_hook, mock_defer_job_polling):
        mock_hook.get_job_status.return_value = JobState.PENDING
        self.operator.process_job_status('123', JobState.PENDING)
        mock_defer_job_polling.assert_called_once()

    @patch('include.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_execute_complete(self, mock_hook):
        event = {'status': JobState.SUCCEEDED, 'job_id': '123', 'message': 'Job completed successfully'}
        self.operator.execute_complete(Context(), event)
        self.assertEqual(self.operator.job_id, '123')

    @patch('include.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_execute_complete_failure(self, mock_hook):
        event = {'status': JobState.FAILED, 'job_id': '123', 'message': 'Job failed with error'}
        with self.assertRaises(AirflowException) as context:
            self.operator.execute_complete(Context(), event)
        self.assertTrue("Job 123 failed with error" in str(context.exception))
        

class TestRolloutAnyscaleService(unittest.TestCase):
    def setUp(self):
        self.operator = RolloutAnyscaleService(
            conn_id='test_conn',
            name='test_service',
            image_uri='test_image_uri',
            applications=[{'name': 'app1'}],
            build_id='build123',
            compute_config_id='config123',
            task_id='rollout_service_test'
        )

    @patch('include.operators.anyscale.RolloutAnyscaleService.hook')
    def test_execute_successful(self, mock_hook):
        mock_hook.return_value.deploy_service.return_value = 'service123'
        with self.assertRaises(TaskDeferred):
            self.operator.execute(Context())

    @patch('include.operators.anyscale.RolloutAnyscaleService.hook', new_callable=MagicMock)
    def test_execute_fail_sdk_unavailable(self, mock_hook):
        mock_hook.return_value = None

        with self.assertRaises(AirflowException) as cm:
            self.operator.execute(Context())

        self.assertEqual(str(cm.exception), "SDK is not available")

    @patch('include.operators.anyscale.RolloutAnyscaleService.defer')
    @patch('include.operators.anyscale.RolloutAnyscaleService.hook')
    def test_defer_trigger_called(self, mock_hook, mock_defer):
        mock_hook.return_value.deploy_service.return_value = 'service123'
        self.operator.execute(Context())
        mock_defer.assert_called_once_with(
            trigger=AnyscaleServiceTrigger(
                conn_id='test_conn',
                service_id='test_service',
                expected_state=ServiceState.RUNNING,
                poll_interval=60,
                timeout=600
            ),
            method_name="execute_complete"
        )

    @patch('include.operators.anyscale.RolloutAnyscaleService.hook')
    def test_execute_complete_failed(self, mock_hook):
        event = {'status': ServiceState.SYSTEM_FAILURE, 'service_id': 'service123', 'message': 'Deployment failed'}
        with self.assertRaises(AirflowException) as cm:
            self.operator.execute_complete(Context(), event)
        self.assertIn("Job service123 failed with error Deployment failed", str(cm.exception))

    @patch('include.operators.anyscale.RolloutAnyscaleService.hook')
    def test_execute_complete_success(self, mock_hook):
        event = {'status': ServiceState.RUNNING, 'service_id': 'service123', 'message': 'Deployment succeeded'}
        self.operator.execute_complete(Context(), event)
        self.assertEqual(self.operator.service_params['name'], 'test_service')



if __name__ == '__main__':
    unittest.main()
