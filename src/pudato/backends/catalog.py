"""Catalog backend protocol and implementations.

Provides metadata management and discovery for data assets.
For MVP, uses a simple in-memory or JSON-file based catalog.
Future: Integrate with dbt manifest, Iceberg catalog, or Glue.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import structlog

logger = structlog.get_logger()


@dataclass
class DataAsset:
    """Metadata for a data asset (table, view, model, seed, etc.)."""

    name: str
    asset_type: str  # "table", "view", "model", "seed", "source"
    location: str  # database.schema.table or file path
    description: str = ""
    columns: list[dict[str, Any]] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    # Lineage
    upstream: list[str] = field(default_factory=list)  # assets this depends on
    downstream: list[str] = field(default_factory=list)  # assets that depend on this
    # Tracking
    created_at: str = ""
    updated_at: str = ""
    created_by: str = ""
    # Extra metadata
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CatalogStats:
    """Statistics about the catalog."""

    total_assets: int
    by_type: dict[str, int]
    by_tag: dict[str, int]


class CatalogBackend(Protocol):
    """Protocol for catalog/metadata operations.

    Provides asset registration, discovery, and lineage tracking.
    MVP uses simple file-based storage; production could use
    Glue Catalog, Iceberg REST catalog, or dedicated metadata store.
    """

    def register(self, asset: DataAsset) -> None:
        """Register or update a data asset in the catalog."""
        ...

    def unregister(self, name: str) -> bool:
        """Remove an asset from the catalog. Returns True if found."""
        ...

    def get(self, name: str) -> DataAsset | None:
        """Get a data asset by name."""
        ...

    def search(
        self,
        query: str | None = None,
        asset_type: str | None = None,
        tags: list[str] | None = None,
    ) -> list[DataAsset]:
        """Search for assets matching criteria."""
        ...

    def list_all(self) -> list[DataAsset]:
        """List all registered assets."""
        ...

    def get_lineage(self, name: str, direction: str = "both") -> dict[str, list[str]]:
        """Get lineage for an asset.

        Args:
            name: Asset name
            direction: "upstream", "downstream", or "both"

        Returns:
            Dict with upstream and/or downstream asset names
        """
        ...

    def get_stats(self) -> CatalogStats:
        """Get catalog statistics."""
        ...


class InMemoryCatalogBackend:
    """Simple in-memory catalog backend.

    Suitable for testing and local development.
    Data is lost when the process exits.
    """

    def __init__(self) -> None:
        self._assets: dict[str, DataAsset] = {}
        self._log = logger.bind(backend="catalog_memory")

    def register(self, asset: DataAsset) -> None:
        """Register or update a data asset."""
        now = datetime.now(timezone.utc).isoformat()

        if asset.name in self._assets:
            asset.updated_at = now
            if not asset.created_at:
                asset.created_at = self._assets[asset.name].created_at
        else:
            asset.created_at = now
            asset.updated_at = now

        self._assets[asset.name] = asset
        self._log.debug("asset_registered", name=asset.name, type=asset.asset_type)

    def unregister(self, name: str) -> bool:
        """Remove an asset from the catalog."""
        if name in self._assets:
            del self._assets[name]
            self._log.debug("asset_unregistered", name=name)
            return True
        return False

    def get(self, name: str) -> DataAsset | None:
        """Get a data asset by name."""
        return self._assets.get(name)

    def search(
        self,
        query: str | None = None,
        asset_type: str | None = None,
        tags: list[str] | None = None,
    ) -> list[DataAsset]:
        """Search for assets matching criteria."""
        results = []

        for asset in self._assets.values():
            # Filter by type
            if asset_type and asset.asset_type != asset_type:
                continue

            # Filter by tags (all specified tags must be present)
            if tags and not all(tag in asset.tags for tag in tags):
                continue

            # Filter by query (search in name and description)
            if query:
                query_lower = query.lower()
                if (
                    query_lower not in asset.name.lower()
                    and query_lower not in asset.description.lower()
                ):
                    continue

            results.append(asset)

        return sorted(results, key=lambda a: a.name)

    def list_all(self) -> list[DataAsset]:
        """List all registered assets."""
        return sorted(self._assets.values(), key=lambda a: a.name)

    def get_lineage(self, name: str, direction: str = "both") -> dict[str, list[str]]:
        """Get lineage for an asset."""
        asset = self._assets.get(name)
        if not asset:
            return {"upstream": [], "downstream": []}

        result = {}
        if direction in ("upstream", "both"):
            result["upstream"] = list(asset.upstream)
        if direction in ("downstream", "both"):
            result["downstream"] = list(asset.downstream)

        return result

    def get_stats(self) -> CatalogStats:
        """Get catalog statistics."""
        by_type: dict[str, int] = {}
        by_tag: dict[str, int] = {}

        for asset in self._assets.values():
            by_type[asset.asset_type] = by_type.get(asset.asset_type, 0) + 1
            for tag in asset.tags:
                by_tag[tag] = by_tag.get(tag, 0) + 1

        return CatalogStats(
            total_assets=len(self._assets),
            by_type=by_type,
            by_tag=by_tag,
        )


class FileCatalogBackend:
    """File-based catalog backend using JSON.

    Persists catalog to a JSON file for durability across restarts.
    Suitable for local development and small deployments.
    """

    def __init__(self, catalog_path: Path | str) -> None:
        self._path = Path(catalog_path)
        self._assets: dict[str, DataAsset] = {}
        self._log = logger.bind(backend="catalog_file", path=str(self._path))
        self._load()

    def _load(self) -> None:
        """Load catalog from file."""
        if self._path.exists():
            try:
                with open(self._path) as f:
                    data = json.load(f)
                    for name, asset_dict in data.get("assets", {}).items():
                        self._assets[name] = DataAsset(**asset_dict)
                self._log.debug("catalog_loaded", asset_count=len(self._assets))
            except Exception as e:
                self._log.warning("catalog_load_failed", error=str(e))

    def _save(self) -> None:
        """Save catalog to file."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "assets": {name: asdict(asset) for name, asset in self._assets.items()},
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(self._path, "w") as f:
            json.dump(data, f, indent=2)
        self._log.debug("catalog_saved", asset_count=len(self._assets))

    def register(self, asset: DataAsset) -> None:
        """Register or update a data asset."""
        now = datetime.now(timezone.utc).isoformat()

        if asset.name in self._assets:
            asset.updated_at = now
            if not asset.created_at:
                asset.created_at = self._assets[asset.name].created_at
        else:
            asset.created_at = now
            asset.updated_at = now

        self._assets[asset.name] = asset
        self._save()
        self._log.debug("asset_registered", name=asset.name, type=asset.asset_type)

    def unregister(self, name: str) -> bool:
        """Remove an asset from the catalog."""
        if name in self._assets:
            del self._assets[name]
            self._save()
            self._log.debug("asset_unregistered", name=name)
            return True
        return False

    def get(self, name: str) -> DataAsset | None:
        """Get a data asset by name."""
        return self._assets.get(name)

    def search(
        self,
        query: str | None = None,
        asset_type: str | None = None,
        tags: list[str] | None = None,
    ) -> list[DataAsset]:
        """Search for assets matching criteria."""
        results = []

        for asset in self._assets.values():
            if asset_type and asset.asset_type != asset_type:
                continue
            if tags and not all(tag in asset.tags for tag in tags):
                continue
            if query:
                query_lower = query.lower()
                if (
                    query_lower not in asset.name.lower()
                    and query_lower not in asset.description.lower()
                ):
                    continue
            results.append(asset)

        return sorted(results, key=lambda a: a.name)

    def list_all(self) -> list[DataAsset]:
        """List all registered assets."""
        return sorted(self._assets.values(), key=lambda a: a.name)

    def get_lineage(self, name: str, direction: str = "both") -> dict[str, list[str]]:
        """Get lineage for an asset."""
        asset = self._assets.get(name)
        if not asset:
            return {"upstream": [], "downstream": []}

        result = {}
        if direction in ("upstream", "both"):
            result["upstream"] = list(asset.upstream)
        if direction in ("downstream", "both"):
            result["downstream"] = list(asset.downstream)

        return result

    def get_stats(self) -> CatalogStats:
        """Get catalog statistics."""
        by_type: dict[str, int] = {}
        by_tag: dict[str, int] = {}

        for asset in self._assets.values():
            by_type[asset.asset_type] = by_type.get(asset.asset_type, 0) + 1
            for tag in asset.tags:
                by_tag[tag] = by_tag.get(tag, 0) + 1

        return CatalogStats(
            total_assets=len(self._assets),
            by_type=by_type,
            by_tag=by_tag,
        )
