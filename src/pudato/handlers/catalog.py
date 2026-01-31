"""Catalog handler for metadata and discovery operations."""

from __future__ import annotations

from pathlib import Path

from pudato.backends.catalog import (
    CatalogBackend,
    DataAsset,
    FileCatalogBackend,
    InMemoryCatalogBackend,
)
from pudato.handlers.base import BaseHandler
from pudato.protocol import Command, Result


class CatalogHandler(BaseHandler):
    """Handler for catalog/metadata operations.

    Translates standardized Command messages into catalog operations.
    Provides asset registration, discovery, and lineage tracking.

    Supported actions:
    - register: Register or update a data asset
    - unregister: Remove an asset from the catalog
    - get: Get asset metadata by name
    - search: Search for assets by query, type, or tags
    - list: List all registered assets
    - lineage: Get upstream/downstream lineage for an asset
    - stats: Get catalog statistics
    """

    service_type = "catalog"

    def __init__(self, backend: CatalogBackend) -> None:
        super().__init__()
        self._backend = backend

    def supported_actions(self) -> list[str]:
        """Return list of supported catalog actions."""
        return [
            "register",
            "unregister",
            "get",
            "search",
            "list",
            "lineage",
            "stats",
        ]

    def _handle(self, command: Command) -> Result:
        """Route command to appropriate catalog operation."""
        match command.action:
            case "register":
                return self._register(command)
            case "unregister":
                return self._unregister(command)
            case "get":
                return self._get(command)
            case "search":
                return self._search(command)
            case "list":
                return self._list(command)
            case "lineage":
                return self._lineage(command)
            case "stats":
                return self._stats(command)
            case _:
                return self._error(command, [f"Unsupported action: {command.action}"])

    def _register(self, command: Command) -> Result:
        """Handle register command."""
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        asset_type = payload.get("asset_type")
        if not asset_type:
            return self._error(command, ["Missing required field: asset_type"])

        location = payload.get("location", "")

        try:
            asset = DataAsset(
                name=name,
                asset_type=asset_type,
                location=location,
                description=payload.get("description", ""),
                columns=payload.get("columns", []),
                tags=payload.get("tags", []),
                upstream=payload.get("upstream", []),
                downstream=payload.get("downstream", []),
                created_by=payload.get("created_by", ""),
                metadata=payload.get("metadata", {}),
            )

            self._backend.register(asset)

            return self._success(
                command,
                data={
                    "name": name,
                    "registered": True,
                    "asset_type": asset_type,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to register asset: {e!s}"])

    def _unregister(self, command: Command) -> Result:
        """Handle unregister command."""
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        try:
            found = self._backend.unregister(name)

            return self._success(
                command,
                data={"name": name, "unregistered": found},
            )
        except Exception as e:
            return self._error(command, [f"Failed to unregister asset: {e!s}"])

    def _get(self, command: Command) -> Result:
        """Handle get command."""
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        try:
            asset = self._backend.get(name)

            if asset is None:
                return self._success(
                    command,
                    data={"name": name, "found": False},
                )

            return self._success(
                command,
                data={
                    "name": asset.name,
                    "found": True,
                    "asset_type": asset.asset_type,
                    "location": asset.location,
                    "description": asset.description,
                    "columns": asset.columns,
                    "tags": asset.tags,
                    "upstream": asset.upstream,
                    "downstream": asset.downstream,
                    "created_at": asset.created_at,
                    "updated_at": asset.updated_at,
                    "created_by": asset.created_by,
                    "metadata": asset.metadata,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to get asset: {e!s}"])

    def _search(self, command: Command) -> Result:
        """Handle search command."""
        payload = command.payload

        try:
            results = self._backend.search(
                query=payload.get("query"),
                asset_type=payload.get("asset_type"),
                tags=payload.get("tags"),
            )

            return self._success(
                command,
                data={
                    "count": len(results),
                    "assets": [
                        {
                            "name": a.name,
                            "asset_type": a.asset_type,
                            "location": a.location,
                            "description": a.description,
                            "tags": a.tags,
                        }
                        for a in results
                    ],
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to search assets: {e!s}"])

    def _list(self, command: Command) -> Result:
        """Handle list command."""
        try:
            assets = self._backend.list_all()

            return self._success(
                command,
                data={
                    "count": len(assets),
                    "assets": [
                        {
                            "name": a.name,
                            "asset_type": a.asset_type,
                            "location": a.location,
                        }
                        for a in assets
                    ],
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to list assets: {e!s}"])

    def _lineage(self, command: Command) -> Result:
        """Handle lineage command."""
        payload = command.payload

        name = payload.get("name")
        if not name:
            return self._error(command, ["Missing required field: name"])

        direction = payload.get("direction", "both")
        if direction not in ("upstream", "downstream", "both"):
            return self._error(
                command,
                [f"Invalid direction: {direction}. Must be upstream, downstream, or both"],
            )

        try:
            lineage = self._backend.get_lineage(name, direction)

            return self._success(
                command,
                data={
                    "name": name,
                    "direction": direction,
                    **lineage,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to get lineage: {e!s}"])

    def _stats(self, command: Command) -> Result:
        """Handle stats command."""
        try:
            stats = self._backend.get_stats()

            return self._success(
                command,
                data={
                    "total_assets": stats.total_assets,
                    "by_type": stats.by_type,
                    "by_tag": stats.by_tag,
                },
            )
        except Exception as e:
            return self._error(command, [f"Failed to get stats: {e!s}"])


def create_memory_catalog_handler() -> CatalogHandler:
    """Factory function to create CatalogHandler with in-memory backend.

    Returns:
        CatalogHandler configured with in-memory backend.
    """
    backend = InMemoryCatalogBackend()
    return CatalogHandler(backend=backend)


def create_file_catalog_handler(catalog_path: Path | str) -> CatalogHandler:
    """Factory function to create CatalogHandler with file-based backend.

    Args:
        catalog_path: Path to the catalog JSON file.

    Returns:
        CatalogHandler configured with file-based backend.
    """
    backend = FileCatalogBackend(catalog_path=catalog_path)
    return CatalogHandler(backend=backend)
