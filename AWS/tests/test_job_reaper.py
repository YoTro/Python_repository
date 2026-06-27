import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from src.core.errors.exceptions import JobSuspendedError
from src.core.models.request import UnifiedRequest
from src.jobs.manager import JobManager, JobRecord, JobStatus


class TestJobManagerReaper(unittest.IsolatedAsyncioTestCase):
    def _make_manager(self):
        """Build a JobManager and cancel its background tasks so tests don't hang."""
        manager = JobManager(max_workers=1)
        if manager._reaper_task:
            manager._reaper_task.cancel()
        for worker in manager._workers:
            worker.cancel()
        return manager

    @staticmethod
    def _suspended_record(job_id, *, reason, timeout=10):
        from unittest.mock import AsyncMock

        record = JobRecord(
            job_id=job_id,
            request=UnifiedRequest(workflow_name="test_workflow"),
            status=JobStatus.SUSPENDED,
            suspended_at=(datetime.utcnow() - timedelta(seconds=timeout + 5)).timestamp(),
            suspend_timeout_sec=timeout,
            suspend_reason=reason,
        )
        record.callback = MagicMock()
        record.callback.on_error = AsyncMock()
        return record

    async def test_reaper_cancels_expired_interaction_job(self):
        """An expired interaction wait is cancelled with the user-responsibility message."""
        manager = self._make_manager()
        record = self._suspended_record("interaction_job", reason="interaction")
        manager._jobs[record.job_id] = record

        cancelled = await manager._cancel_expired_suspended()

        self.assertIn(record, cancelled)
        self.assertEqual(record.status, JobStatus.CANCELLED)
        self.assertEqual(record.error, "Job timed out waiting for user interaction.")
        record.callback.on_error.assert_called_once()

    async def test_reaper_uses_batch_backstop_message(self):
        """An expired batch wait is cancelled with the system-responsibility message."""
        manager = self._make_manager()
        record = self._suspended_record("batch_job", reason="batch")
        manager._jobs[record.job_id] = record

        cancelled = await manager._cancel_expired_suspended()

        self.assertIn(record, cancelled)
        self.assertEqual(record.status, JobStatus.CANCELLED)
        self.assertEqual(record.error, "Batch job did not complete within the maximum wait window.")
        record.callback.on_error.assert_called_once()

    async def test_reaper_ignores_unexpired_batch_job(self):
        """A batch wait within its backstop window is left running for BatchPoller."""
        manager = self._make_manager()
        record = self._suspended_record("fresh_batch", reason="batch", timeout=10)
        record.suspended_at = datetime.utcnow().timestamp()  # just suspended
        manager._jobs[record.job_id] = record

        cancelled = await manager._cancel_expired_suspended()

        self.assertEqual(cancelled, [])
        self.assertEqual(record.status, JobStatus.SUSPENDED)
        record.callback.on_error.assert_not_called()

    def test_job_suspended_error_extracts_timeout(self):
        """Test that JobSuspendedError correctly extracts expires_in from the signal."""
        # 1. Create a mock signal
        signal = {"_type": "INTERACTION_REQUIRED", "data": {"expires_in": 42}}

        # 2. Raise the error and check the timeout
        error = JobSuspendedError("Need interaction", signal)

        self.assertEqual(error.timeout_sec, 42)

        # 3. Test default fallback
        signal_no_timeout = {"_type": "INTERACTION_REQUIRED", "data": {}}
        error2 = JobSuspendedError("Need interaction", signal_no_timeout)
        self.assertEqual(error2.timeout_sec, 300)  # Default is 300s


if __name__ == "__main__":
    unittest.main()
