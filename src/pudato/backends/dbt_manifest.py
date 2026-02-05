"""dbt manifest parser for extracting table-level lineage."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import structlog

from pudato.protocol import DataReference

logger = structlog.get_logger()


def _get_enum_or_str(value: Any) -> str:
    """Extract string value from enum or return string directly.

    dbt_artifacts_parser returns some fields as either Enum objects (with .value)
    or plain strings depending on the manifest version. This helper handles both.
    """
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


def parse_lineage_from_manifest(
    project_dir: Path,
) -> tuple[list[DataReference], list[DataReference]]:
    """Extract inputs/outputs from dbt manifest after execution.

    Reads manifest.json and run_results.json from the target directory
    to determine which models were executed and their dependencies.

    Args:
        project_dir: Path to dbt project directory

    Returns:
        Tuple of (inputs, outputs) as DataReference lists.
        - outputs: Tables/models that were written
        - inputs: Upstream tables/sources that were read
    """
    target_dir = project_dir / "target"
    manifest_path = target_dir / "manifest.json"
    run_results_path = target_dir / "run_results.json"

    if not manifest_path.exists():
        logger.warning("manifest_not_found", path=str(manifest_path))
        return [], []

    try:
        # Import here to make dbt-artifacts-parser optional
        from dbt_artifacts_parser.parser import (  # type: ignore[import-untyped]
            parse_manifest,
            parse_run_results,
        )
    except ImportError:
        logger.warning("dbt_artifacts_parser_not_installed")
        return [], []

    # Load and parse manifest
    with open(manifest_path) as f:
        manifest = parse_manifest(json.load(f))

    # Get executed nodes from run_results if available
    executed_node_ids: set[str] = set()
    if run_results_path.exists():
        with open(run_results_path) as f:
            run_results = parse_run_results(json.load(f))
            for result in run_results.results:
                # dbt_artifacts_parser returns status as either an Enum or string
                # depending on manifest version. We handle both cases.
                status = _get_enum_or_str(result.status)
                if status == "success":
                    executed_node_ids.add(result.unique_id)

    # If no run results, fall back to all model nodes
    if not executed_node_ids:
        for unique_id, node in manifest.nodes.items():
            rt = _get_enum_or_str(getattr(node, "resource_type", ""))
            if rt == "model":
                executed_node_ids.add(unique_id)

    inputs: list[DataReference] = []
    outputs: list[DataReference] = []
    seen_inputs: set[str] = set()
    seen_outputs: set[str] = set()

    for node_id in executed_node_ids:
        node = manifest.nodes.get(node_id)
        if not node:
            continue

        rt = _get_enum_or_str(getattr(node, "resource_type", ""))

        # Only process models and seeds as outputs
        if rt not in ("model", "seed"):
            continue

        # Add output (the table this node creates)
        relation_name = getattr(node, "relation_name", None)
        if relation_name and relation_name not in seen_outputs:
            seen_outputs.add(relation_name)
            outputs.append(
                DataReference(
                    ref_type="table",
                    location=_clean_relation_name(relation_name),
                    metadata={"dbt_node": node_id},
                )
            )

        # Add inputs from depends_on
        depends_on = getattr(node, "depends_on", None)
        if depends_on:
            dep_nodes = getattr(depends_on, "nodes", []) or []
            for dep_id in dep_nodes:
                if dep_id in seen_inputs:
                    continue

                # Look up the dependency node
                dep_node = manifest.nodes.get(dep_id)
                if dep_node:
                    dep_relation = getattr(dep_node, "relation_name", None)
                    if dep_relation:
                        seen_inputs.add(dep_id)
                        inputs.append(
                            DataReference(
                                ref_type="table",
                                location=_clean_relation_name(dep_relation),
                                metadata={"dbt_node": dep_id},
                            )
                        )

    logger.info(
        "parsed_dbt_lineage",
        inputs_count=len(inputs),
        outputs_count=len(outputs),
    )

    return inputs, outputs


def _clean_relation_name(relation_name: str) -> str:
    """Clean dbt relation name by removing quotes.

    dbt relation names look like: "database"."schema"."table"
    We convert to: database.schema.table
    """
    return relation_name.replace('"', "")
