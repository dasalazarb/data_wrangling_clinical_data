class PipelineError(Exception):
    """Base pipeline exception."""


class ValidationError(PipelineError):
    """Raised when a critical validation fails."""
