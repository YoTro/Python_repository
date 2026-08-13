"""Error → callback mapping tests for JobManager._run_job."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.jobs.manager import _BATCH_SUSPEND_BACKSTOP_SEC, JobManager, JobRecord, JobStatus


class _FakeSlot:
    """Minimal async context manager that does nothing."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@asynccontextmanager
async def _fake_concurrent_slot(self, entry_type=None, chat_id=None):
    async with _FakeSlot():
        yield


@pytest.fixture
def manager():
    with patch.object(JobManager, "_start_workers", return_value=None):
        mgr = JobManager()
    mgr._reaper_task = None
    return mgr


@pytest.fixture
def record():
    req = MagicMock()
    req.workflow_name = "test_workflow"
    req.intent = None
    req.entry_type = "cli_workflow"
    req.chat_id = "test_chat"
    req.tenant_id = "default"
    req.user_id = "test_user"
    req.params = {}
    return JobRecord(job_id="test_job", request=req)


async def _run_job_with_error(mgr, rec, error):
    mgr._jobs[rec.job_id] = rec
    with patch("src.gateway.rate_limit.RateLimiter.concurrent_slot", _fake_concurrent_slot):
        with patch.object(mgr, "_run_workflow_mode") as mock_run:
            mock_run.side_effect = error
            await mgr._run_job(rec.job_id)


class TestBatchPendingError:
    @pytest.mark.asyncio
    async def test_maps_to_suspended_not_error(self, manager, record):
        from src.jobs.manager import BatchPendingError

        record.callback = MagicMock()
        record.callback.on_progress = AsyncMock()
        record.callback.on_error = AsyncMock()

        await _run_job_with_error(
            manager,
            record,
            BatchPendingError(
                "batch submitted",
                batch_job_id="batch_123",
                handle=MagicMock(provider="gemini"),
                requests=[],
                items_snapshot=[],
                output_field="analysis",
            ),
        )

        assert record.status == JobStatus.SUSPENDED
        assert record.suspend_reason == "batch"
        assert record.suspend_timeout_sec == _BATCH_SUSPEND_BACKSTOP_SEC
        record.callback.on_progress.assert_called_once()
        record.callback.on_error.assert_not_called()

    @pytest.mark.asyncio
    async def test_notifies_with_batch_id(self, manager, record):
        from src.jobs.manager import BatchPendingError

        record.callback = MagicMock()
        record.callback.on_progress = AsyncMock()

        await _run_job_with_error(
            manager,
            record,
            BatchPendingError(
                "batch submitted",
                batch_job_id="batch_abc",
                handle=MagicMock(provider="gemini"),
                requests=[],
                items_snapshot=[],
                output_field="analysis",
            ),
        )

        assert "batch_abc" in record.callback.on_progress.call_args.kwargs["message"]


class TestRetryableError:
    @pytest.mark.asyncio
    async def test_maps_to_failed_with_on_error(self, manager, record):
        from src.jobs.manager import RetryableError

        record.callback = MagicMock()
        record.callback.on_error = AsyncMock()

        await _run_job_with_error(
            manager,
            record,
            RetryableError("rate limited", http_status=429, provider="test"),
        )

        assert record.status == JobStatus.FAILED
        record.callback.on_error.assert_called_once()


class TestRuntimeErrorConcurrentLimit:
    @pytest.mark.asyncio
    async def test_notify_for_concurrent_limit(self, manager, record):
        record.callback = MagicMock()
        record.callback.notify = AsyncMock()
        record.callback.on_error = AsyncMock()

        await _run_job_with_error(
            manager,
            record,
            RuntimeError("concurrent limit reached for entry_type=cli_workflow"),
        )

        assert record.status == JobStatus.FAILED
        record.callback.notify.assert_called_once()
        record.callback.on_error.assert_not_called()


class TestGenericException:
    @pytest.mark.asyncio
    async def test_maps_to_failed_with_on_error(self, manager, record):
        record.callback = MagicMock()
        record.callback.on_error = AsyncMock()

        await _run_job_with_error(manager, record, ValueError("something broke"))

        assert record.status == JobStatus.FAILED
        record.callback.on_error.assert_called_once()
