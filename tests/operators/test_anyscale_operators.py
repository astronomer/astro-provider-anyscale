import unittest
from unittest.mock import patch, MagicMock,PropertyMock
from airflow.utils.context import Context
from airflow.exceptions import AirflowException, TaskDeferred
from anyscale.job.models import JobState
from anyscale.service.models import ServiceState
from anyscale_provider.operators.anyscale import SubmitAnyscaleJob
from anyscale_provider.operators.anyscale import RolloutAnyscaleService
from anyscale_provider.triggers.anyscale import AnyscaleJobTrigger,AnyscaleServiceTrigger


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

    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.get_current_status')
    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook', new_callable=MagicMock)
    def test_execute_successful(self, mock_hook, mock_get_status):
        job_result_mock = MagicMock()
        job_result_mock.id = '123'
        mock_hook.submit_job.return_value = '123'
        mock_get_status.return_value = JobState.SUCCEEDED
        
        job_id = self.operator.execute(Context())
        
        mock_hook.submit_job.assert_called_once()  
        mock_get_status.assert_called_with('123')  
        self.assertEqual(job_id, '123')  

    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.process_job_status')
    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.get_current_status')
    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_execute_fail_on_status(self, mock_hook, mock_get_current_status, mock_process_job_status):
        mock_hook.submit_job.return_value = '123'
        mock_get_current_status.return_value = JobState.FAILED
        mock_process_job_status.side_effect = AirflowException("Job 123 failed.")
        
        with self.assertRaises(AirflowException) as context:
            self.operator.execute(Context())
        
        self.assertTrue("Job 123 failed." in str(context.exception))

    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_on_kill(self, mock_hook):
        self.operator.job_id = '123'
        self.operator.on_kill()        
        mock_hook.terminate_job.assert_called_once_with('123', 5)

    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_process_job_status_unexpected_state(self, mock_hook):
        with self.assertRaises(Exception):
            self.operator.process_job_status(None, 'UNKNOWN_STATE')

    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.defer_job_polling')
    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_defer_job_polling_called(self, mock_hook, mock_defer_job_polling):
        mock_hook.get_job_status.return_value = JobState.STARTING
        self.operator.process_job_status('123', JobState.STARTING)
        mock_defer_job_polling.assert_called_once()

    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_execute_complete(self, mock_hook):
        event = {'status': JobState.SUCCEEDED, 'job_id': '123', 'message': 'Job completed successfully'}
        self.assertEqual(self.operator.execute_complete(Context(), event), None)

    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook')
    def test_execute_complete_failure(self, mock_hook):
        event = {'status': JobState.FAILED, 'job_id': '123', 'message': 'Job failed with error'}
        with self.assertRaises(AirflowException) as context:
            self.operator.execute_complete(Context(), event)
        self.assertTrue("Job 123 failed with error" in str(context.exception))
    
    def test_no_job_name(self):
        with self.assertRaises(AirflowException) as context:
            SubmitAnyscaleJob(
                conn_id='test_conn', 
                name='',  # No job name
                image_uri='test_image_uri', 
                compute_config={}, 
                working_dir='/test/dir', 
                entrypoint='test_entrypoint', 
                task_id='submit_job_test'
            )
        self.assertTrue("Job name is required." in str(context.exception))

    def test_no_entrypoint_provided(self):
        with self.assertRaises(AirflowException) as context:
            SubmitAnyscaleJob(
                conn_id='test_conn', 
                name='test_job', 
                image_uri='test_image_uri', 
                compute_config={}, 
                working_dir='/test/dir', 
                entrypoint='',  # No entrypoint
                task_id='submit_job_test'
            )
        self.assertTrue("Entrypoint must be specified." in str(context.exception))

    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook', new_callable=PropertyMock)
    def test_check_anyscale_hook(self, mock_hook_property):
        # Access the hook property
        hook = self.operator.hook
        # Verify that the hook property was accessed
        mock_hook_property.assert_called_once()

    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook', new_callable=PropertyMock)
    def test_execute_with_no_hook(self, mock_hook_property):
        # Simulate the hook not being available by raising an AirflowException
        mock_hook_property.side_effect = AirflowException("SDK is not available.")
        
        # Execute the operator and expect it to raise an AirflowException
        with self.assertRaises(AirflowException) as context:
            self.operator.execute(Context())

        self.assertTrue("SDK is not available." in str(context.exception))

    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.get_current_status')
    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook', new_callable=MagicMock)
    def test_job_state_failed(self, mock_hook, mock_get_status):
        job_result_mock = MagicMock()
        job_result_mock.id = '123'
        mock_hook.submit_job.return_value = '123'
        mock_get_status.return_value = JobState.FAILED
        
        with self.assertRaises(AirflowException) as context:
            self.operator.execute(Context())
        self.assertTrue("Job 123 failed." in str(context.exception))

    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook', new_callable=PropertyMock)
    def test_get_current_status(self, mock_hook_property):
        mock_hook = MagicMock()
        mock_job_status = MagicMock(state=JobState.SUCCEEDED)
        mock_hook.get_job_status.return_value = mock_job_status
        mock_hook_property.return_value = mock_hook

        # Call the method to test
        status = self.operator.get_current_status('123')

        # Verify the result
        self.assertEqual(status, 'SUCCEEDED')

        # Ensure the mock was called correctly
        mock_hook.get_job_status.assert_called_once_with(job_id='123')
    
    @patch('airflow.models.BaseOperator.defer')
    @patch('anyscale_provider.operators.anyscale.SubmitAnyscaleJob.hook', new_callable=MagicMock)
    def test_defer_job_polling(self, mock_hook, mock_defer):
        # Mock the submit_job method to return a job ID
        mock_hook.submit_job.return_value = '123'
        # Mock the get_job_status method to return a starting state
        mock_hook.get_job_status.return_value.state = JobState.STARTING

        # Call the execute method which internally calls process_job_status and defer_job_polling
        self.operator.execute(Context())

        # Check that the defer method was called with the correct arguments
        mock_defer.assert_called_once()
        args, kwargs = mock_defer.call_args
        self.assertIsInstance(kwargs['trigger'], AnyscaleJobTrigger)
        self.assertEqual(kwargs['trigger'].job_id, '123')
        self.assertEqual(kwargs['trigger'].conn_id, 'test_conn')
        self.assertEqual(kwargs['method_name'], 'execute_complete')
        

class TestRolloutAnyscaleService(unittest.TestCase):
    def setUp(self):
        self.operator = RolloutAnyscaleService(
            conn_id='test_conn',
            name='test_service',
            image_uri='test_image_uri',
            working_dir='/test/dir', 
            applications=[{'name': 'app1', 'import_path': 'module.optional_submodule:app'}],
            compute_config='config123',
            task_id='rollout_service_test'
        )

    @patch('anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook')
    def test_execute_successful(self, mock_hook):
        mock_hook.return_value.deploy_service.return_value = 'service123'
        with self.assertRaises(TaskDeferred):
            self.operator.execute(Context())

    @patch('anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook', new_callable=MagicMock)
    def test_execute_fail_sdk_unavailable(self, mock_hook):
        self.operator.hook = None

        with self.assertRaises(AirflowException) as cm:
            self.operator.execute(Context())

        self.assertEqual(str(cm.exception), "SDK is not available")

    @patch('anyscale_provider.operators.anyscale.RolloutAnyscaleService.defer')
    @patch('anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook', new_callable=MagicMock)
    def test_defer_trigger_called(self, mock_hook, mock_defer):
        mock_hook.return_value.deploy_service.return_value = 'service123'
        
        self.operator.execute(Context())
        
        # Extract the actual call arguments
        actual_call_args = mock_defer.call_args
        
        # Define the expected trigger and method_name
        expected_trigger = AnyscaleServiceTrigger(
            conn_id='test_conn',
            service_name='test_service',
            expected_state=ServiceState.RUNNING,
            canary_percent=None,
            poll_interval=60,
            timeout=600
        )
        
        expected_method_name = "execute_complete"
        
        # Perform individual assertions
        actual_trigger = actual_call_args.kwargs['trigger']
        actual_method_name = actual_call_args.kwargs['method_name']
    
        self.assertEqual(actual_trigger.conn_id, expected_trigger.conn_id)
        self.assertEqual(actual_trigger.service_name, expected_trigger.service_name)
        self.assertEqual(actual_trigger.expected_state, expected_trigger.expected_state)
        self.assertEqual(actual_trigger.poll_interval, expected_trigger.poll_interval)
        self.assertEqual(actual_trigger.timeout, expected_trigger.timeout)
        self.assertEqual(actual_method_name, expected_method_name)

    @patch('anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook')
    def test_execute_complete_failed(self, mock_hook):
        event = {'status': ServiceState.SYSTEM_FAILURE, 'service_name': 'service123', 'message': 'Deployment failed'}
        with self.assertRaises(AirflowException) as cm:
            self.operator.execute_complete(Context(), event)
        self.assertIn("Job service123 failed with error Deployment failed", str(cm.exception))

    @patch('anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook')
    def test_execute_complete_success(self, mock_hook):
        event = {'status': ServiceState.RUNNING, 'service_name': 'service123', 'message': 'Deployment succeeded'}
        self.operator.execute_complete(Context(), event)
        self.assertEqual(self.operator.service_params['name'], 'test_service')

    @patch('anyscale_provider.operators.anyscale.RolloutAnyscaleService.hook', new_callable=PropertyMock)
    def test_check_anyscale_hook(self, mock_hook_property):
        hook = self.operator.hook
        mock_hook_property.assert_called_once()
    
    def test_no_service_name(self):
        with self.assertRaises(ValueError) as cm:
            RolloutAnyscaleService(
                conn_id='test_conn',
                name='',  # No service name
                image_uri='test_image_uri',
                working_dir='/test/dir', 
                applications=[{'name': 'app1', 'import_path': 'module.optional_submodule:app'}],
                compute_config='config123',
                task_id='rollout_service_test'
            )
        self.assertIn("Service name is required", str(cm.exception))

    def test_no_applications(self):
        with self.assertRaises(ValueError) as cm:
            RolloutAnyscaleService(
                conn_id='test_conn',
                name='test_service',
                image_uri='test_image_uri',
                working_dir='/test/dir', 
                applications=[],  # No applications
                compute_config='config123',
                task_id='rollout_service_test'
            )
        self.assertIn("At least one application must be specified", str(cm.exception))

if __name__ == '__main__':
    unittest.main()
