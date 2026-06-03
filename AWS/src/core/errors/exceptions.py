from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.core.errors.codes import ErrorCode

"""
Unified exception hierarchy for the AWS project.

  AWSBaseError
  ├── ScraperError          HTTP / TLS / network failures
  ├── ExtractorError        Data extraction / parsing failures
  ├── ConfigError           Configuration issues
  ├── WorkflowError         Workflow execution failures
  │   └── StepError         Individual step failures
  ├── RetryableError        Transient failures (rate limit, timeout)
  ├── FatalError            Non-recoverable failures
  ├── CheckpointError       Checkpoint save / load failures
  └── JobSuspendedError     Human-in-the-loop suspension

All exceptions accept an optional ``code`` (ErrorCode) that maps the failure
to a canonical platform error code defined in src/core/errors/codes.py.
Use codes.is_retryable() / codes.is_auth_error() to drive retry logic instead
of comparing raw HTTP status codes or provider-specific strings.
"""


class AWSBaseError(Exception):
    """Base exception for all AWS project errors."""

    def __init__(
        self,
        message: str = "",
        details: dict | None = None,
        code: ErrorCode | None = None,
    ):
        self.message = message
        self.details = details or {}
        self.code = code
        super().__init__(self.message)


class ScraperError(AWSBaseError):
    """HTTP, TLS fingerprinting, or network-level failure."""

    pass


class ExtractorError(AWSBaseError):
    """Data extraction or parsing failure within an extractor."""

    pass


class ConfigError(AWSBaseError):
    """Configuration loading or validation failure."""

    pass


class WorkflowError(AWSBaseError):
    """Workflow-level execution failure."""

    pass


class StepError(WorkflowError):
    """Individual step failure within a workflow."""

    def __init__(self, message: str = "", step_name: str = "", step_index: int = -1, **kwargs):
        self.step_name = step_name
        self.step_index = step_index
        super().__init__(
            message, details={"step_name": step_name, "step_index": step_index, **kwargs}
        )


class RetryableError(AWSBaseError):
    """
    Transient failure that can be retried.
    Examples: rate limiting, network timeout, temporary blocking.

    ``http_status`` and ``provider`` are optional; when supplied, ``code`` is
    auto-derived via classify_http() if not passed explicitly.
    Use codes.default_retry_after(err.code) as a fallback when no
    Retry-After header is available.
    """

    def __init__(
        self,
        message: str = "",
        retry_after_seconds: float = 0,
        http_status: int | None = None,
        provider: str = "",
        code: ErrorCode | None = None,
        **kwargs,
    ):
        self.retry_after_seconds = retry_after_seconds
        self.http_status = http_status
        self.provider = provider

        # Auto-derive canonical code from HTTP status when not explicitly set
        if code is None and http_status is not None:
            from src.core.errors.codes import classify_http

            code = classify_http(http_status, provider)

        super().__init__(
            message,
            details={
                "retry_after_seconds": retry_after_seconds,
                "http_status": http_status,
                **kwargs,
            },
            code=code,
        )


class FatalError(AWSBaseError):
    """
    Non-recoverable failure. Do not retry.
    Examples: invalid configuration, missing credentials, unsupported operation.
    """

    pass


class CheckpointError(AWSBaseError):
    """Checkpoint save or load failure."""

    pass


class BatchPendingError(AWSBaseError):
    """
    Raised by a Step when it submits a provider batch job and needs
    the workflow to suspend until results arrive.

    ActivityRunner catches this, writes BATCH_SUBMITTED (with full reconstruction
    payload) to the event log, then re-raises so WorkflowEngine → JobManager
    can transition the job to SUSPENDED.
    """

    def __init__(
        self,
        message: str,
        batch_job_id: str,
        handle: Any,  # BatchJobHandle instance
        requests: list | None = None,  # [{"custom_id": str, "item_idx": int}]
        items_snapshot: list | None = None,  # full items list at submission time
        output_field: str | None = None,
        schema_path: str | None = None,  # "module.ClassName" or None
    ):
        self.batch_job_id = batch_job_id
        self.handle = handle
        self.requests = requests or []
        self.items_snapshot = items_snapshot or []
        self.output_field = output_field
        self.schema_path = schema_path
        super().__init__(message, details={"batch_job_id": batch_job_id})


class JobSuspendedError(AWSBaseError):
    """
    Raised when a job needs to be suspended for human intervention
    (e.g., waiting for QR code scan).
    """

    def __init__(self, message: str, signal: dict):
        super().__init__(message)
        self.signal = signal

        # Dynamically extract timeout from the signal's data payload (default 300s)
        data = signal.get("data", {})
        self.timeout_sec = data.get("expires_in", 300)
