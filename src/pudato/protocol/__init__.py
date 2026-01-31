"""Platform-agnostic message protocol for inter-service communication."""

from pudato.protocol.messages import (
    Command,
    DataReference,
    Event,
    ExecutionRecord,
    Result,
)

__all__ = ["Command", "DataReference", "Event", "ExecutionRecord", "Result"]
