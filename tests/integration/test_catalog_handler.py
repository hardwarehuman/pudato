"""Integration tests for catalog handler."""

import json
import os
import tempfile
from pathlib import Path

import pytest

from pudato.handlers.catalog import (
    CatalogHandler,
    create_file_catalog_handler,
    create_memory_catalog_handler,
)
from pudato.protocol import Command


@pytest.fixture
def catalog_handler() -> CatalogHandler:
    """Create CatalogHandler with in-memory backend."""
    return create_memory_catalog_handler()


@pytest.fixture
def file_catalog_handler(tmp_path: Path) -> CatalogHandler:
    """Create CatalogHandler with file-based backend."""
    catalog_path = tmp_path / "catalog.json"
    return create_file_catalog_handler(catalog_path)


@pytest.mark.integration
class TestCatalogHandler:
    """Test CatalogHandler with in-memory backend."""

    def test_register_asset(self, catalog_handler: CatalogHandler):
        """Test registering a data asset."""
        command = Command(
            type="catalog",
            action="register",
            payload={
                "name": "raw_orders",
                "asset_type": "table",
                "location": "main.raw_orders",
                "description": "Raw order data from source system",
                "tags": ["raw", "orders"],
                "columns": [
                    {"name": "order_id", "type": "INTEGER"},
                    {"name": "customer_id", "type": "INTEGER"},
                    {"name": "total", "type": "DECIMAL"},
                ],
            },
        )

        result = catalog_handler.handle(command)

        assert result.status == "success"
        assert result.data["name"] == "raw_orders"
        assert result.data["registered"] is True
        assert result.data["asset_type"] == "table"

    def test_register_missing_name(self, catalog_handler: CatalogHandler):
        """Test error handling for missing name."""
        command = Command(
            type="catalog",
            action="register",
            payload={"asset_type": "table"},
        )

        result = catalog_handler.handle(command)

        assert result.status == "error"
        assert "Missing required field: name" in result.errors[0]

    def test_get_asset(self, catalog_handler: CatalogHandler):
        """Test retrieving an asset by name."""
        # Register first
        catalog_handler.handle(
            Command(
                type="catalog",
                action="register",
                payload={
                    "name": "dim_customers",
                    "asset_type": "model",
                    "location": "main.dim_customers",
                    "description": "Customer dimension table",
                    "tags": ["dimension", "customers"],
                    "upstream": ["raw_customers"],
                },
            )
        )

        command = Command(
            type="catalog",
            action="get",
            payload={"name": "dim_customers"},
        )

        result = catalog_handler.handle(command)

        assert result.status == "success"
        assert result.data["found"] is True
        assert result.data["name"] == "dim_customers"
        assert result.data["asset_type"] == "model"
        assert result.data["description"] == "Customer dimension table"
        assert "dimension" in result.data["tags"]
        assert "raw_customers" in result.data["upstream"]

    def test_get_asset_not_found(self, catalog_handler: CatalogHandler):
        """Test get for non-existent asset."""
        command = Command(
            type="catalog",
            action="get",
            payload={"name": "nonexistent"},
        )

        result = catalog_handler.handle(command)

        assert result.status == "success"
        assert result.data["found"] is False

    def test_unregister_asset(self, catalog_handler: CatalogHandler):
        """Test unregistering an asset."""
        # Register first
        catalog_handler.handle(
            Command(
                type="catalog",
                action="register",
                payload={
                    "name": "temp_table",
                    "asset_type": "table",
                },
            )
        )

        command = Command(
            type="catalog",
            action="unregister",
            payload={"name": "temp_table"},
        )

        result = catalog_handler.handle(command)

        assert result.status == "success"
        assert result.data["unregistered"] is True

    def test_search_by_type(self, catalog_handler: CatalogHandler):
        """Test searching assets by type."""
        # Register various assets
        for asset in [
            {"name": "raw_orders", "asset_type": "table"},
            {"name": "raw_customers", "asset_type": "table"},
            {"name": "stg_orders", "asset_type": "model"},
            {"name": "dim_customers", "asset_type": "model"},
        ]:
            catalog_handler.handle(
                Command(type="catalog", action="register", payload=asset)
            )

        command = Command(
            type="catalog",
            action="search",
            payload={"asset_type": "model"},
        )

        result = catalog_handler.handle(command)

        assert result.status == "success"
        assert result.data["count"] == 2
        names = [a["name"] for a in result.data["assets"]]
        assert "stg_orders" in names
        assert "dim_customers" in names

    def test_search_by_tags(self, catalog_handler: CatalogHandler):
        """Test searching assets by tags."""
        for asset in [
            {"name": "fact_orders", "asset_type": "model", "tags": ["fact", "orders"]},
            {"name": "dim_customers", "asset_type": "model", "tags": ["dimension", "customers"]},
            {"name": "fact_revenue", "asset_type": "model", "tags": ["fact", "revenue"]},
        ]:
            catalog_handler.handle(
                Command(type="catalog", action="register", payload=asset)
            )

        command = Command(
            type="catalog",
            action="search",
            payload={"tags": ["fact"]},
        )

        result = catalog_handler.handle(command)

        assert result.status == "success"
        assert result.data["count"] == 2

    def test_search_by_query(self, catalog_handler: CatalogHandler):
        """Test searching assets by text query."""
        for asset in [
            {"name": "raw_orders", "asset_type": "table", "description": "Order data"},
            {"name": "raw_products", "asset_type": "table", "description": "Product catalog"},
            {"name": "stg_orders", "asset_type": "model", "description": "Staged orders"},
        ]:
            catalog_handler.handle(
                Command(type="catalog", action="register", payload=asset)
            )

        command = Command(
            type="catalog",
            action="search",
            payload={"query": "order"},
        )

        result = catalog_handler.handle(command)

        assert result.status == "success"
        assert result.data["count"] == 2  # raw_orders and stg_orders

    def test_list_all(self, catalog_handler: CatalogHandler):
        """Test listing all assets."""
        for name in ["asset_a", "asset_b", "asset_c"]:
            catalog_handler.handle(
                Command(
                    type="catalog",
                    action="register",
                    payload={"name": name, "asset_type": "table"},
                )
            )

        command = Command(
            type="catalog",
            action="list",
            payload={},
        )

        result = catalog_handler.handle(command)

        assert result.status == "success"
        assert result.data["count"] == 3

    def test_lineage(self, catalog_handler: CatalogHandler):
        """Test getting lineage for an asset."""
        # Register assets with lineage
        catalog_handler.handle(
            Command(
                type="catalog",
                action="register",
                payload={
                    "name": "stg_orders",
                    "asset_type": "model",
                    "upstream": ["raw_orders"],
                    "downstream": ["fact_orders", "dim_order_status"],
                },
            )
        )

        command = Command(
            type="catalog",
            action="lineage",
            payload={"name": "stg_orders"},
        )

        result = catalog_handler.handle(command)

        assert result.status == "success"
        assert result.data["name"] == "stg_orders"
        assert "raw_orders" in result.data["upstream"]
        assert "fact_orders" in result.data["downstream"]

    def test_lineage_direction(self, catalog_handler: CatalogHandler):
        """Test getting lineage with specific direction."""
        catalog_handler.handle(
            Command(
                type="catalog",
                action="register",
                payload={
                    "name": "test_model",
                    "asset_type": "model",
                    "upstream": ["source_a", "source_b"],
                    "downstream": ["target_x"],
                },
            )
        )

        # Upstream only
        result = catalog_handler.handle(
            Command(
                type="catalog",
                action="lineage",
                payload={"name": "test_model", "direction": "upstream"},
            )
        )

        assert result.status == "success"
        assert "upstream" in result.data
        assert "downstream" not in result.data

    def test_stats(self, catalog_handler: CatalogHandler):
        """Test getting catalog statistics."""
        for asset in [
            {"name": "raw_a", "asset_type": "table", "tags": ["raw"]},
            {"name": "raw_b", "asset_type": "table", "tags": ["raw"]},
            {"name": "stg_a", "asset_type": "model", "tags": ["staging"]},
            {"name": "fact_a", "asset_type": "model", "tags": ["mart", "fact"]},
        ]:
            catalog_handler.handle(
                Command(type="catalog", action="register", payload=asset)
            )

        command = Command(
            type="catalog",
            action="stats",
            payload={},
        )

        result = catalog_handler.handle(command)

        assert result.status == "success"
        assert result.data["total_assets"] == 4
        assert result.data["by_type"]["table"] == 2
        assert result.data["by_type"]["model"] == 2
        assert result.data["by_tag"]["raw"] == 2

    def test_unsupported_action(self, catalog_handler: CatalogHandler):
        """Test error handling for unsupported action."""
        command = Command(
            type="catalog",
            action="unsupported_action",
            payload={},
        )

        result = catalog_handler.handle(command)

        assert result.status == "error"
        assert "Unsupported action" in result.errors[0]


@pytest.mark.integration
class TestFileCatalogBackend:
    """Test CatalogHandler with file-based backend for persistence."""

    def test_persistence(self, tmp_path: Path):
        """Test that catalog persists across handler instances."""
        catalog_path = tmp_path / "catalog.json"

        # Create handler and register asset
        handler1 = create_file_catalog_handler(catalog_path)
        handler1.handle(
            Command(
                type="catalog",
                action="register",
                payload={
                    "name": "persistent_asset",
                    "asset_type": "table",
                    "description": "Should persist",
                },
            )
        )

        # Create new handler instance (simulates restart)
        handler2 = create_file_catalog_handler(catalog_path)
        result = handler2.handle(
            Command(
                type="catalog",
                action="get",
                payload={"name": "persistent_asset"},
            )
        )

        assert result.status == "success"
        assert result.data["found"] is True
        assert result.data["description"] == "Should persist"


@pytest.mark.integration
class TestCatalogHandlerLambda:
    """Test Catalog handler via Lambda entry point."""

    def test_lambda_handler_catalog(self):
        """Test Lambda handler processes catalog commands."""
        from pudato.runtime.lambda_handler import handle

        os.environ["HANDLER_TYPE"] = "catalog"
        os.environ.pop("CATALOG_PATH", None)  # Use in-memory

        command = Command(
            type="catalog",
            action="register",
            payload={
                "name": "lambda_asset",
                "asset_type": "table",
            },
        )

        event = {
            "Records": [
                {
                    "messageId": "test-catalog-123",
                    "body": json.dumps({"Message": command.to_json()}),
                    "receiptHandle": "test-handle",
                }
            ]
        }

        result = handle(event)

        assert result["processed"] == 1
        assert result["results"][0]["status"] == "success"
