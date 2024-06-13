import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
from datetime import datetime
import os
import pytest
from typing import Any, Dict, AsyncIterator, Tuple, Optional
from pathlib import Path
from airflow.exceptions import AirflowNotFoundException

from anyscale.job.models import JobState
from anyscale.service.models import ServiceState

from anyscale_provider.hooks.anyscale import AnyscaleHook
from anyscale_provider.triggers.anyscale import AnyscaleJobTrigger, AnyscaleServiceTrigger
from airflow.triggers.base import TriggerEvent
from airflow.models.connection import Connection

class TestAnyscaleJobTrigger(unittest.TestCase):
    def setUp(self):
        self.trigger = AnyscaleJobTrigger(conn_id='anyscale_default',
                                          job_id='123',
                                          job_start_time=datetime.now().timestamp())

    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.get_current_status')
    def test_is_terminal_status(self, mock_get_status):
        mock_get_status.return_value = 'SUCCEEDED'
        self.assertTrue(self.trigger.is_terminal_status('123'))

    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.get_current_status')
    def test_is_not_terminal_status(self, mock_get_status):
        mock_get_status.return_value = 'RUNNING'
        self.assertFalse(self.trigger.is_terminal_status('123'))

    @patch('asyncio.sleep', return_value=None)
    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.get_current_status', side_effect=['RUNNING', 'RUNNING', 'SUCCEEDED'])
    async def test_run_successful_completion(self, mock_get_status, mock_sleep):
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['status'], 'SUCCEEDED')

    @patch('time.time', side_effect=[100, 200, 300, 400, 10000])  # Simulating time passing and timeout
    @patch('asyncio.sleep', return_value=None)
    async def test_run_timeout(self, mock_sleep, mock_time):
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['status'], 'timeout')

    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.is_terminal_status', side_effect=Exception("Error occurred"))
    async def test_run_exception(self, mock_is_terminal_status):
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['status'], JobState.FAILED)
        self.assertIn('Error occurred', events[0]['message'])

    @patch('anyscale_provider.hooks.anyscale.AnyscaleHook.get_job_status')
    async def test_get_current_status(self, mock_get_job_status):
        mock_get_job_status.return_value = MagicMock(state=JobState.SUCCEEDED)
        trigger = AnyscaleJobTrigger(conn_id='default_conn',
                                     job_id='',
                                     job_start_time=datetime.now().timestamp())
        events = []
        async for event in trigger.run():
            events.append(event)
        
        status = self.trigger.get_current_status('123')
        self.assertEqual(status, JobState.SUCCEEDED)
        mock_get_job_status.assert_called_once_with(job_id='123')

    async def test_run_no_job_id_provided(self):
        trigger = AnyscaleJobTrigger(conn_id='default_conn',
                                     job_id='',
                                     job_start_time=datetime.now().timestamp())
        events = []
        async for event in trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['status'], 'error')
        self.assertIn('No job_id provided', events[0]['message'])
    
    @patch('airflow.models.connection.Connection.get_connection_from_secrets')
    def test_hook_method(self, mock_get_connection):
        # Configure the mock to raise AirflowNotFoundException
        mock_get_connection.side_effect = AirflowNotFoundException("The conn_id `default_conn` isn't defined")

        trigger = AnyscaleJobTrigger(conn_id='default_conn',
                                     job_id='123',
                                     job_start_time=datetime.now().timestamp())
        
        with self.assertRaises(AirflowNotFoundException) as context:
            result = trigger.hook()

        self.assertIn("The conn_id `default_conn` isn't defined", str(context.exception))
    
    def test_serialize(self):
        time = datetime.now().timestamp()
        trigger = AnyscaleJobTrigger(conn_id='default_conn',
                                     job_id='123',
                                     job_start_time=time)
        
        result = trigger.serialize()
        expected_output = ("anyscale_provider.triggers.anyscale.AnyscaleJobTrigger", {
            "conn_id": 'default_conn',
            "job_id": '123',
            "job_start_time": time,
            "poll_interval": 60,
            "timeout": 3600
        })

        # Check if the result is a tuple
        self.assertTrue(isinstance(result, tuple))
        
        # Check if the tuple contains a string and a dictionary
        self.assertTrue(isinstance(result[0], str))
        self.assertTrue(isinstance(result[1], dict))
        
        # Check if the result matches the expected output
        self.assertEqual(result, expected_output)
    
    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.is_terminal_status', new_callable=AsyncMock)
    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.hook')
    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.get_current_status')
    async def test_run_method_no_job_id(self, mock_get_current_status, mock_hook, mock_is_terminal_status):
        trigger = AnyscaleJobTrigger(conn_id='default_conn', job_id=None, job_start_time=datetime.now().timestamp())
        
        events = []
        async for event in trigger.run():
            events.append(event)
        
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["status"], "error")
        self.assertEqual(events[0]["message"], "No job_id provided to async trigger")

    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.is_terminal_status', new_callable=AsyncMock)
    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.hook')
    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.get_current_status')
    async def test_run_method_timeout(self, mock_get_current_status, mock_hook, mock_is_terminal_status):
        mock_is_terminal_status.return_value = False
        trigger = AnyscaleJobTrigger(conn_id='default_conn', job_id='123', job_start_time=datetime.now().timestamp(), poll_interval=1, timeout=1)
        
        events = []
        async for event in trigger.run():
            events.append(event)
        
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["status"], "timeout")
        self.assertIn("Timeout waiting for job 123 to complete.", events[0]["message"])

    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.is_terminal_status', new_callable=AsyncMock)
    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.hook')
    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.get_current_status')
    async def test_run_method_success(self, mock_get_current_status, mock_hook, mock_is_terminal_status):
        mock_is_terminal_status.side_effect = [False, True]
        mock_get_current_status.return_value = "success"
        
        mock_hook_instance = MagicMock()
        mock_hook.return_value = mock_hook_instance
        mock_hook_instance.get_logs.return_value = "log line 1\nlog line 2"
        
        trigger = AnyscaleJobTrigger(conn_id='default_conn', job_id='123', job_start_time=datetime.now().timestamp(), poll_interval=1, timeout=10)
        
        events = []
        async for event in trigger.run():
            events.append(event)
        
        self.assertEqual(events[-1]["status"], "success")
        self.assertIn("Job 123 completed with status success.", events[-1]["message"])

    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.is_terminal_status', new_callable=AsyncMock)
    @patch('anyscale_provider.triggers.anyscale.AnyscaleJobTrigger.hook')
    async def test_run_method_exception(self, mock_hook, mock_is_terminal_status):
        mock_is_terminal_status.side_effect = Exception("Test exception")
        
        trigger = AnyscaleJobTrigger(conn_id='default_conn', job_id='123', job_start_time=datetime.now().timestamp(), poll_interval=1, timeout=10)
        
        events = []
        async for event in trigger.run():
            events.append(event)
        
        self.assertEqual(events[-1]["status"], "failed")
        self.assertIn("An error occurred while polling for job status.", events[-1]["message"])

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
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['status'], ServiceState.RUNNING)
        self.assertIn('Service deployment succeeded', events[0]['message'])

    @patch('time.time', side_effect=[100, 200, 300, 400, 10000])  # Simulating time passing and timeout
    @patch('asyncio.sleep', return_value=None)
    async def test_run_timeout(self, mock_sleep, mock_time):
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['status'], ServiceState.UNKNOWN)
        self.assertIn('did not reach RUNNING within the timeout period', events[0]['message'])

    @patch('anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger.check_current_status', side_effect=Exception("Error occurred"))
    async def test_run_exception(self, mock_check_current_status):
        events = []
        async for event in self.trigger.run():
            events.append(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['status'], ServiceState.SYSTEM_FAILURE)
        self.assertIn('Error occurred', events[0]['message'])
    
    @patch('airflow.models.connection.Connection.get_connection_from_secrets')
    def test_hook_method(self, mock_get_connection):
        # Configure the mock to raise AirflowNotFoundException
        mock_get_connection.side_effect = AirflowNotFoundException("The conn_id `default_conn` isn't defined")

        trigger = AnyscaleServiceTrigger(conn_id='default_conn',
                                         service_name="AstroService",
                                         expected_state=ServiceState.RUNNING,
                                         canary_percent=0.0)
        
        with self.assertRaises(AirflowNotFoundException) as context:
            result = trigger.hook()

        self.assertIn("The conn_id `default_conn` isn't defined", str(context.exception))
    
    def test_serialize(self):
        
        trigger = AnyscaleServiceTrigger(conn_id='default_conn',
                                         service_name="AstroService",
                                         expected_state=ServiceState.RUNNING,
                                         canary_percent=0.0)
        
        result = trigger.serialize()
        expected_output = ("anyscale_provider.triggers.anyscale.AnyscaleServiceTrigger", {
            "conn_id": 'default_conn',
            "service_name": "AstroService",
            "expected_state": ServiceState.RUNNING,
            "canary_percent": 0.0,
            "poll_interval": 60,
            "timeout": 600
        })

        # Check if the result is a tuple
        self.assertTrue(isinstance(result, tuple))
        
        # Check if the tuple contains a string and a dictionary
        self.assertTrue(isinstance(result[0], str))
        self.assertTrue(isinstance(result[1], dict))
        
        # Check if the result matches the expected output
        self.assertEqual(result, expected_output)

if __name__ == '__main__':
    unittest.main()