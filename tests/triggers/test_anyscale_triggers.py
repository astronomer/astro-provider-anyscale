import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

from anyscale.job.models import JobState

from anyscale_provider.triggers.anyscale import AnyscaleJobTrigger,AnyscaleServiceTrigger

class TestAnyscaleJobTrigger(unittest.TestCase):
    def setUp(self):
        self.trigger = AnyscaleJobTrigger(conn_id='default_conn',
                                          job_id='123',
                                          job_start_time=datetime.now())

    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.get_current_status')
    def test_is_terminal_status(self, mock_get_status):
        mock_get_status.return_value = 'COMPLETED'
        self.assertTrue(self.trigger.is_terminal_status('123'))

    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.is_terminal_status')
    def test_is_not_terminal_status(self, mock_get_status):
        mock_get_status.return_value = False
        self.assertFalse(self.trigger.is_terminal_status('123'))

    @patch('asyncio.sleep', return_value=None)
    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.get_current_status', side_effect=['RUNNING', 'RUNNING', 'COMPLETED'])
    async def test_run_successful_completion(self, mock_get_status, mock_sleep):
        async for event in self.trigger.run():
            self.assertIn('status', event)
            self.assertEqual(event['status'], 'COMPLETED')

    @patch('time.time', side_effect=[100, 200, 300, 400, 10000])  # Simulating time passing and timeout
    @patch('asyncio.sleep', return_value=None)
    async def test_run_timeout(self, mock_sleep, mock_time):
        async for event in self.trigger.run():
            self.assertEqual(event['status'], 'timeout')

class TestAnyscaleServiceTrigger(unittest.TestCase):
    def setUp(self):
        self.trigger = AnyscaleServiceTrigger(conn_id='default_conn',
                                              service_name='service123',
                                              expected_state='RUNNING',
                                              canary_percent=None)

    @patch('anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger.get_current_status')
    def test_check_current_status(self, mock_get_status):
        mock_get_status.return_value = "STARTING"
        self.assertTrue(self.trigger.check_current_status('service123'))

    @patch('asyncio.sleep', return_value=None)
    @patch('anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger.get_current_status', side_effect=['STARTING', 'UPDATING', 'RUNNING'])
    async def test_run_successful(self, mock_get_status, mock_sleep):
        async for event in self.trigger.run():
            self.assertEqual(event['status'], 'success')
            self.assertIn('Service deployment succeeded', event['message'])

    @patch('time.time', side_effect=[100, 200, 300, 400, 10000])  # Simulating time passing and timeout
    @patch('asyncio.sleep', return_value=None)
    async def test_run_timeout(self, mock_sleep, mock_time):
        async for event in self.trigger.run():
            self.assertEqual(event['status'], 'timeout')

if __name__ == '__main__':
    unittest.main()
