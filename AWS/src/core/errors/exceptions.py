from __future__ import annotations
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
  └── CheckpointError       Checkpoint save / load failures
"""


class AWSBaseError(Exception):
    """Base exception for all AWS project errors."""

    def __init__(self, message: str = "", details: dict = None):
        self.message = message
        self.details = details or {}
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
        super().__init__(message, details={"step_name": step_name, "step_index": step_index, **kwargs})


class RetryableError(AWSBaseError):
    """
    Transient failure that can be retried.
    Examples: rate limiting, network timeout, temporary blocking.
    """

    def __init__(self, message: str = "", retry_after_seconds: float = 0, **kwargs):
        self.retry_after_seconds = retry_after_seconds
        super().__init__(message, details={"retry_after_seconds": retry_after_seconds, **kwargs})


class FatalError(AWSBaseError):
    """
    Non-recoverable failure. Do not retry.
    Examples: invalid configuration, missing credentials, unsupported operation.
    """
    pass


class CheckpointError(AWSBaseError):
    """Checkpoint save or load failure."""
    pass
