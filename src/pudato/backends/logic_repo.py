"""Logic repo backend for cloning/fetching external data logic repositories.

Wraps git via subprocess (matching the DbtBackend pattern) to:
1. Clone or fetch an external git repo containing data logic
2. Check out a specific branch or commit
3. Extract the commit hash for version tracking (logic_version)

The resolve_logic_dir() convenience function is the primary entry point
for handler factories.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

import structlog

logger = structlog.get_logger()


@dataclass
class LogicRepoConfig:
    """Configuration for logic repo operations."""

    repo_url: str
    clone_dir: Path
    branch: str = "main"
    target_version: str | None = None


@dataclass
class LogicRepoResult:
    """Result of a logic repo sync operation."""

    success: bool
    project_dir: Path
    commit_hash: str | None
    branch: str | None
    error: str | None = None


class LogicRepoBackend:
    """Backend for managing external data logic repositories.

    Clones/fetches a git repo and resolves the commit hash for version
    tracking. Uses subprocess to call git, matching the DbtBackend pattern.

    Usage:
        config = LogicRepoConfig(repo_url="...", clone_dir=Path("/tmp/repo"))
        backend = LogicRepoBackend(config)
        result = backend.sync()
        # result.project_dir -> path to use as dbt project dir
        # result.commit_hash -> 40-char SHA to use as logic_version
    """

    def __init__(self, config: LogicRepoConfig) -> None:
        self._config = config
        self._log = logger.bind(
            backend="logic_repo",
            repo_url=config.repo_url,
            clone_dir=str(config.clone_dir),
        )

    def sync(self) -> LogicRepoResult:
        """Sync the logic repo: clone if missing, fetch+checkout if present.

        Returns:
            LogicRepoResult with project_dir, commit_hash, and status.
        """
        clone_dir = self._config.clone_dir

        if self._is_cloned(clone_dir):
            return self._fetch_and_checkout(clone_dir)
        else:
            return self._clone_and_checkout(clone_dir)

    def _is_cloned(self, clone_dir: Path) -> bool:
        """Check if the clone directory is an existing git repo."""
        return (clone_dir / ".git").is_dir()

    def _clone_and_checkout(self, clone_dir: Path) -> LogicRepoResult:
        """Clone the repo and checkout the target version."""
        self._log.info("cloning_repo", branch=self._config.branch)

        result = self._run_git(
            [
                "clone",
                "--branch",
                self._config.branch,
                "--single-branch",
                self._config.repo_url,
                str(clone_dir),
            ]
        )

        if not result.success:
            return result

        if self._config.target_version:
            return self._checkout_version(clone_dir, self._config.target_version)

        return self._resolve_head(clone_dir)

    def _fetch_and_checkout(self, clone_dir: Path) -> LogicRepoResult:
        """Fetch latest and checkout the target version or branch HEAD."""
        self._log.info("fetching_repo", branch=self._config.branch)

        result = self._run_git(
            ["fetch", "origin", self._config.branch],
            cwd=clone_dir,
        )

        if not result.success:
            return result

        if self._config.target_version:
            return self._checkout_version(clone_dir, self._config.target_version)

        checkout_result = self._run_git(
            ["checkout", f"origin/{self._config.branch}"],
            cwd=clone_dir,
        )

        if not checkout_result.success:
            return checkout_result

        return self._resolve_head(clone_dir)

    def _checkout_version(self, clone_dir: Path, version: str) -> LogicRepoResult:
        """Checkout a specific commit hash."""
        self._log.info("checkout_version", version=version)

        result = self._run_git(
            ["checkout", version],
            cwd=clone_dir,
        )

        if not result.success:
            return result

        return self._resolve_head(clone_dir)

    def _resolve_head(self, clone_dir: Path) -> LogicRepoResult:
        """Get the current HEAD commit hash."""
        try:
            proc = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                cwd=clone_dir,
            )

            if proc.returncode != 0:
                self._log.error("rev_parse_failed", stderr=proc.stderr)
                return LogicRepoResult(
                    success=False,
                    project_dir=clone_dir,
                    commit_hash=None,
                    branch=self._config.branch,
                    error=f"git rev-parse failed: {proc.stderr}",
                )

            commit_hash = proc.stdout.strip()
            self._log.info("repo_synced", commit_hash=commit_hash)

            return LogicRepoResult(
                success=True,
                project_dir=clone_dir,
                commit_hash=commit_hash,
                branch=self._config.branch,
            )

        except FileNotFoundError:
            self._log.error("git_not_found")
            return LogicRepoResult(
                success=False,
                project_dir=clone_dir,
                commit_hash=None,
                branch=self._config.branch,
                error="git command not found",
            )

    def _run_git(
        self,
        args: list[str],
        cwd: Path | None = None,
    ) -> LogicRepoResult:
        """Execute a git command. Returns LogicRepoResult on failure.

        On success, returns a result with success=True but no commit_hash;
        caller should follow up with _resolve_head().
        """
        cmd = ["git"] + args
        self._log.debug("executing_git", command=" ".join(cmd))

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=cwd,
            )

            if proc.returncode != 0:
                self._log.error(
                    "git_failed",
                    command=" ".join(cmd),
                    return_code=proc.returncode,
                    stderr=proc.stderr[:500] if proc.stderr else None,
                )
                return LogicRepoResult(
                    success=False,
                    project_dir=self._config.clone_dir,
                    commit_hash=None,
                    branch=self._config.branch,
                    error=f"git command failed (rc={proc.returncode}): {proc.stderr[:500]}",
                )

            return LogicRepoResult(
                success=True,
                project_dir=self._config.clone_dir,
                commit_hash=None,
                branch=self._config.branch,
            )

        except FileNotFoundError:
            self._log.error("git_not_found")
            return LogicRepoResult(
                success=False,
                project_dir=self._config.clone_dir,
                commit_hash=None,
                branch=self._config.branch,
                error="git command not found",
            )


def resolve_logic_dir(
    repo_url: str | None = None,
    clone_dir: Path | None = None,
    branch: str = "main",
    target_version: str | None = None,
    fallback_dir: Path | None = None,
) -> tuple[Path, str | None]:
    """Resolve the logic directory and version.

    Primary entry point for handler factories.

    Args:
        repo_url: Git repo URL. If None, uses fallback_dir.
        clone_dir: Where to clone. Defaults to /tmp/pudato-logic-repo.
        branch: Branch to track. Defaults to "main".
        target_version: Specific commit hash to checkout.
        fallback_dir: Local directory when no repo_url. Defaults to Path("dbt").

    Returns:
        Tuple of (project_dir, logic_version).
        If repo_url is set: (cloned_path, commit_hash).
        If repo_url is None: (fallback_dir, None).

    Raises:
        RuntimeError: If the repo sync fails.
    """
    if not repo_url:
        resolved_fallback = fallback_dir or Path("dbt")
        logger.info("using_local_logic_dir", path=str(resolved_fallback))
        return (resolved_fallback, None)

    resolved_clone_dir = clone_dir or Path("/tmp/pudato-logic-repo")

    config = LogicRepoConfig(
        repo_url=repo_url,
        clone_dir=resolved_clone_dir,
        branch=branch,
        target_version=target_version,
    )

    backend = LogicRepoBackend(config)
    result = backend.sync()

    if not result.success:
        raise RuntimeError(f"Failed to sync logic repo {repo_url}: {result.error}")

    return (result.project_dir, result.commit_hash)
