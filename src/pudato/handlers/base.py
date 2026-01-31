"""Base handler protocol and implementation for service handlers."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any, Protocol, runtime_checkable

import structlog

from pudato.protocol import Command, DataReference, Event, ExecutionRecord, Result

logger = structlog.get_logger()


@runtime_checkable
class Handler(Protocol):
    """Protocol that all service handlers must implement."""

    service_type: str
    """The service type this handler processes (e.g., 'storage', 'query')."""

    def handle(self, command: Command) -> Result:
        """Process a command and return a result."""
        ...

    def supported_actions(self) -> list[str]:
        """Return list of actions this handler supports."""
        ...


class BaseHandler(ABC):
    """Base class for service handlers with standard lifecycle.

    Provides:
    - Automatic timing of command execution
    - Structured logging with correlation IDs
    - Helper methods for creating results
    - Standard error handling

    Subclasses must implement:
    - service_type: class attribute
    - _handle(): the actual command processing logic
    - supported_actions(): list of supported action names
    """

    service_type: str = ""

    def __init__(self) -> None:
        self._log = logger.bind(handler=self.service_type)

    def handle(self, command: Command) -> Result:
        """Process a command with timing and error handling.

        This method wraps _handle() with:
        - Execution timing
        - Structured logging
        - Exception catching
        """
        log = self._log.bind(
            correlation_id=command.correlation_id,
            action=command.action,
        )
        log.info("handling_command")

        start_time = time.perf_counter()

        try:
            if command.action not in self.supported_actions():
                return self._error(
                    command,
                    [f"Unsupported action: {command.action}"],
                    duration_ms=0,
                )

            result = self._handle(command)
            duration_ms = int((time.perf_counter() - start_time) * 1000)

            # Ensure duration is set
            if result.duration_ms == 0:
                result = Result(
                    status=result.status,
                    data=result.data,
                    errors=result.errors,
                    correlation_id=result.correlation_id,
                    duration_ms=duration_ms,
                    handler=result.handler,
                    inputs=result.inputs,
                    outputs=result.outputs,
                    executions=result.executions,
                )

            log.info(
                "command_handled",
                status=result.status,
                duration_ms=result.duration_ms,
            )
            return result

        except Exception as e:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            log.exception("command_failed", error=str(e))
            return self._error(command, [str(e)], duration_ms)

    @abstractmethod
    def _handle(self, command: Command) -> Result:
        """Implement command processing logic.

        Subclasses should implement this method to handle specific actions.
        The base class handle() method wraps this with timing and error handling.
        """
        ...

    @abstractmethod
    def supported_actions(self) -> list[str]:
        """Return list of actions this handler supports."""
        ...

    def _success(
        self,
        command: Command,
        data: dict[str, Any] | None = None,
        duration_ms: int = 0,
        inputs: list[DataReference] | None = None,
        outputs: list[DataReference] | None = None,
        executions: list[ExecutionRecord] | None = None,
    ) -> Result:
        """Create a success result for a command."""
        return Result.success(
            correlation_id=command.correlation_id,
            data=data,
            duration_ms=duration_ms,
            handler=self.service_type,
            inputs=inputs,
            outputs=outputs,
            executions=executions,
        )

    def _error(
        self,
        command: Command,
        errors: list[str],
        duration_ms: int = 0,
    ) -> Result:
        """Create an error result for a command."""
        return Result.error(
            correlation_id=command.correlation_id,
            errors=errors,
            duration_ms=duration_ms,
            handler=self.service_type,
        )

    def _pending(
        self,
        command: Command,
        data: dict[str, Any] | None = None,
    ) -> Result:
        """Create a pending result for async operations."""
        return Result.pending(
            correlation_id=command.correlation_id,
            data=data,
            handler=self.service_type,
        )

    def create_event(
        self,
        event_type: str,
        command: Command,
        payload: dict[str, Any] | None = None,
    ) -> Event:
        """Create an event related to a command.

        Events are used for cross-service notifications (e.g., catalog
        updating metadata when a new object is stored).
        """
        return Event(
            type=event_type,
            payload=payload or {},
            correlation_id=command.correlation_id,
            source=self.service_type,
        )
