import os
import subprocess
from pathlib import Path

import pytest

from hamilton_composer.utils import get_git_root


class TestGetGitRoot:
    def test_get_git_root_from_root(self, tmp_path: Path) -> None:
        root_path = tmp_path
        os.chdir(tmp_path)
        subprocess.run(["git", "init"])
        assert get_git_root() == root_path

    def test_get_git_root_from_subdirectory(self, tmp_path: Path) -> None:
        root_path = tmp_path
        os.chdir(tmp_path)
        subprocess.run(["git", "init"])
        sub = tmp_path / "subdir"
        sub.mkdir()
        os.chdir(sub)
        assert get_git_root() == root_path

    def test_get_git_root_can_ignore_errors(self, tmp_path: Path) -> None:
        os.chdir(tmp_path)
        assert get_git_root(raise_error=False) is None

    def test_get_git_root_raises_error(self, tmp_path: Path) -> None:
        os.chdir(tmp_path)
        with pytest.raises(RuntimeError):
            get_git_root(raise_error=True)
