"""Standard error types for platform operations."""

from typing import Any


class PudatoError(Exception):
    """Base exception for all Pudato errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class HandlerError(PudatoError):
    """Error raised by a handler during command processing."""

    pass


class ValidationError(PudatoError):
    """Error raised when command validation fails."""

    pass


class BackendError(PudatoError):
    """Error raised by a backend service (S3, DynamoDB, etc.)."""

    pass


class ConfigurationError(PudatoError):
    """Error raised for configuration issues."""

    pass


class MessageError(PudatoError):
    """Error raised for message parsing/publishing issues."""

    pass
