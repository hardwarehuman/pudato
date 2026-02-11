"""Integration tests for logic repo backend."""

import subprocess
from pathlib import Path

import pytest

from pudato.backends.logic_repo import (
    LogicRepoBackend,
    LogicRepoConfig,
    resolve_logic_dir,
)


@pytest.fixture
def temp_git_repo(tmp_path: Path) -> Path:
    """Create a temporary git repo simulating an external data logic repo."""
    repo_dir = tmp_path / "logic-repo"
    repo_dir.mkdir()

    # Initialize repo
    subprocess.run(["git", "init"], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@pudato.dev"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Test"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )

    # Create a dbt_project.yml to simulate a dbt project
    (repo_dir / "dbt_project.yml").write_text("name: test_logic\n")
    (repo_dir / "models").mkdir()
    (repo_dir / "models" / "example.sql").write_text("SELECT 1 as id\n")

    subprocess.run(["git", "add", "."], cwd=repo_dir, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "Initial commit"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )

    # Rename default branch to main
    subprocess.run(
        ["git", "branch", "-M", "main"],
        cwd=repo_dir,
        check=True,
        capture_output=True,
    )

    return repo_dir


@pytest.fixture
def clone_dir(tmp_path: Path) -> Path:
    """Temporary directory for cloning."""
    return tmp_path / "clone"


@pytest.mark.integration
class TestLogicRepoBackend:
    """Test LogicRepoBackend clone/fetch/checkout operations."""

    def test_clone_repo(self, temp_git_repo: Path, clone_dir: Path):
        """Test initial clone of a repo."""
        config = LogicRepoConfig(
            repo_url=str(temp_git_repo),
            clone_dir=clone_dir,
            branch="main",
        )
        backend = LogicRepoBackend(config)
        result = backend.sync()

        assert result.success is True
        assert result.commit_hash is not None
        assert len(result.commit_hash) == 40  # full SHA
        assert result.project_dir == clone_dir
        assert (clone_dir / "dbt_project.yml").exists()

    def test_fetch_after_clone(self, temp_git_repo: Path, clone_dir: Path):
        """Test that second sync does fetch instead of clone."""
        config = LogicRepoConfig(
            repo_url=str(temp_git_repo),
            clone_dir=clone_dir,
            branch="main",
        )
        backend = LogicRepoBackend(config)

        # First sync: clone
        result1 = backend.sync()
        assert result1.success is True

        # Second sync: fetch
        result2 = backend.sync()
        assert result2.success is True
        assert result2.commit_hash == result1.commit_hash

    def test_picks_up_new_commits(self, temp_git_repo: Path, clone_dir: Path):
        """Test that fetch picks up new commits on the branch."""
        config = LogicRepoConfig(
            repo_url=str(temp_git_repo),
            clone_dir=clone_dir,
            branch="main",
        )
        backend = LogicRepoBackend(config)

        # Initial clone
        result1 = backend.sync()
        assert result1.success is True

        # Add a new commit to the source repo
        (temp_git_repo / "models" / "new_model.sql").write_text("SELECT 2 as id\n")
        subprocess.run(["git", "add", "."], cwd=temp_git_repo, check=True, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "Add new model"],
            cwd=temp_git_repo,
            check=True,
            capture_output=True,
        )

        # Fetch should pick up new commit
        result2 = backend.sync()
        assert result2.success is True
        assert result2.commit_hash != result1.commit_hash
        assert (clone_dir / "models" / "new_model.sql").exists()

    def test_checkout_specific_version(self, temp_git_repo: Path, clone_dir: Path):
        """Test checking out a specific commit hash."""
        # Get initial commit hash
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=temp_git_repo,
            capture_output=True,
            text=True,
        )
        initial_hash = proc.stdout.strip()

        # Add a second commit
        (temp_git_repo / "models" / "v2.sql").write_text("SELECT 2\n")
        subprocess.run(["git", "add", "."], cwd=temp_git_repo, check=True, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "Second commit"],
            cwd=temp_git_repo,
            check=True,
            capture_output=True,
        )

        # Clone and sync to HEAD (second commit)
        config = LogicRepoConfig(
            repo_url=str(temp_git_repo),
            clone_dir=clone_dir,
            branch="main",
        )
        backend = LogicRepoBackend(config)
        result = backend.sync()
        assert result.success is True
        assert result.commit_hash != initial_hash

        # Now sync to the initial commit specifically
        config_pinned = LogicRepoConfig(
            repo_url=str(temp_git_repo),
            clone_dir=clone_dir,
            branch="main",
            target_version=initial_hash,
        )
        backend_pinned = LogicRepoBackend(config_pinned)
        result_pinned = backend_pinned.sync()

        assert result_pinned.success is True
        assert result_pinned.commit_hash == initial_hash
        # File from second commit should not exist
        assert not (clone_dir / "models" / "v2.sql").exists()


@pytest.mark.integration
class TestResolveLogicDir:
    """Test the resolve_logic_dir convenience function."""

    def test_no_repo_url_uses_fallback(self):
        """When no repo URL, returns fallback dir with None version."""
        project_dir, version = resolve_logic_dir(repo_url=None)
        assert project_dir == Path("dbt")
        assert version is None

    def test_custom_fallback_dir(self):
        """Custom fallback directory is respected."""
        project_dir, version = resolve_logic_dir(
            repo_url=None,
            fallback_dir=Path("/some/other/path"),
        )
        assert project_dir == Path("/some/other/path")
        assert version is None

    def test_with_repo_url(self, temp_git_repo: Path, clone_dir: Path):
        """When repo URL is set, clones and returns hash."""
        project_dir, version = resolve_logic_dir(
            repo_url=str(temp_git_repo),
            clone_dir=clone_dir,
            branch="main",
        )
        assert project_dir == clone_dir
        assert version is not None
        assert len(version) == 40

    def test_invalid_repo_raises(self, tmp_path: Path):
        """Invalid repo URL raises RuntimeError."""
        with pytest.raises(RuntimeError, match="Failed to sync"):
            resolve_logic_dir(
                repo_url="/nonexistent/path/that/does/not/exist.git",
                clone_dir=tmp_path / "clone",
            )
