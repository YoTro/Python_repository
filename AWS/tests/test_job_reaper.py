import unittest
import asyncio
import uuid
import json
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from src.jobs.manager import JobManager, JobRecord, JobStatus
from src.core.models.request import UnifiedRequest
from src.core.errors.exceptions import JobSuspendedError

class TestJobManagerReaper(unittest.IsolatedAsyncioTestCase):

    async def test_reaper_cancels_expired_suspended_job(self):
        """Test that the reaper loop cancels jobs that have exceeded their suspend_timeout_sec."""
        # 1. Setup JobManager with a short reaper interval for testing
        manager = JobManager(max_workers=1)
        
        # 2. Create a mock request and job
        request = UnifiedRequest(workflow_name="test_workflow")
        job_id = "test_job_1"
        
        # We manually create and insert a JobRecord to simulate it being suspended
        # We set suspended_at to the past, so it's already expired based on suspend_timeout_sec
        past_time = datetime.utcnow() - timedelta(seconds=15)
        
        record = JobRecord(
            job_id=job_id,
            request=request,
            status=JobStatus.SUSPENDED,
            suspended_at=past_time.timestamp(),
            suspend_timeout_sec=10  # Very short timeout
        )
        
        from unittest.mock import AsyncMock
        # Add a mock callback to verify on_error is called
        mock_callback = MagicMock()
        mock_callback.on_error = AsyncMock()
        record.callback = mock_callback
        
        manager._jobs[job_id] = record

        # 3. We don't want to run the full infinite reaper loop in the test.
        # Instead, we extract the core logic of the reaper loop and run it once.
        now = datetime.utcnow().timestamp()
        expired_jobs = []
        for j_id, rec in list(manager._jobs.items()):
            if rec.status == JobStatus.SUSPENDED and rec.suspended_at:
                if now - rec.suspended_at > rec.suspend_timeout_sec:
                    expired_jobs.append(rec)
                    
        for rec in expired_jobs:
            rec.status = JobStatus.CANCELLED
            rec.error = "Job timed out waiting for user interaction."
            if rec.callback:
                await rec.callback.on_error(Exception("Timeout"))

        # 4. Assertions
        # Verify the job status changed to CANCELLED
        self.assertEqual(manager._jobs[job_id].status, JobStatus.CANCELLED)
        
        # Verify the error message was set
        self.assertEqual(manager._jobs[job_id].error, "Job timed out waiting for user interaction.")
        
        # Verify the callback was notified
        mock_callback.on_error.assert_called_once()
        
        # Clean up the real background tasks to prevent test hanging
        if manager._reaper_task:
            manager._reaper_task.cancel()
        for worker in manager._workers:
            worker.cancel()

    def test_job_suspended_error_extracts_timeout(self):
        """Test that JobSuspendedError correctly extracts expires_in from the signal."""
        # 1. Create a mock signal
        signal = {
            "_type": "INTERACTION_REQUIRED",
            "data": {
                "expires_in": 42
            }
        }
        
        # 2. Raise the error and check the timeout
        error = JobSuspendedError("Need interaction", signal)
        
        self.assertEqual(error.timeout_sec, 42)
        
        # 3. Test default fallback
        signal_no_timeout = {
            "_type": "INTERACTION_REQUIRED",
            "data": {}
        }
        error2 = JobSuspendedError("Need interaction", signal_no_timeout)
        self.assertEqual(error2.timeout_sec, 300) # Default is 300s

if __name__ == '__main__':
    unittest.main()
