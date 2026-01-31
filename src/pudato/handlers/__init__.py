"""Service handlers (serverless functions) for platform operations."""

from pudato.handlers.base import BaseHandler, Handler
from pudato.handlers.catalog import CatalogHandler
from pudato.handlers.query import QueryHandler
from pudato.handlers.registry import RegistryHandler
from pudato.handlers.storage import StorageHandler
from pudato.handlers.table import TableHandler
from pudato.handlers.transform import TransformHandler

__all__ = [
    "BaseHandler",
    "CatalogHandler",
    "Handler",
    "QueryHandler",
    "RegistryHandler",
    "StorageHandler",
    "TableHandler",
    "TransformHandler",
]
