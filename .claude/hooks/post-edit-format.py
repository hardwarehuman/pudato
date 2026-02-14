# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///

"""Post-edit hook to auto-format files after Claude edits."""

import json
import os
import subprocess
import sys
from pathlib import Path


def format_python(file_path: str, cwd: str) -> None:
    """Format Python files with ruff."""
    try:
        subprocess.run(
            ["ruff", "format", file_path],
            cwd=cwd,
            capture_output=True,
        )
        subprocess.run(
            ["ruff", "check", "--fix", file_path],
            cwd=cwd,
            capture_output=True,
        )
    except FileNotFoundError:
        pass


def format_prettier(file_path: str, cwd: str) -> None:
    """Format files with prettier."""
    try:
        subprocess.run(
            ["npx", "prettier", "--write", file_path],
            cwd=cwd,
            capture_output=True,
        )
    except FileNotFoundError:
        pass


def main() -> None:
    input_data = json.load(sys.stdin)

    tool_name = input_data.get("tool_name")
    tool_input = input_data.get("tool_input", {})
    file_path = tool_input.get("file_path")

    if tool_name not in ("Write", "Edit", "MultiEdit"):
        return

    if not file_path:
        return

    cwd = os.environ.get("CLAUDE_PROJECT_DIR", os.getcwd())
    ext = Path(file_path).suffix

    if ext in (".py", ".pyi"):
        format_python(file_path, cwd)
    elif ext in (".json5", ".yaml", ".yml", ".md"):
        format_prettier(file_path, cwd)


if __name__ == "__main__":
    main()
