import subprocess
from pathlib import Path
from typing import Literal, overload


@overload
def get_git_root(raise_error: Literal[False]) -> Path | None: ...


@overload
def get_git_root(raise_error: Literal[True]) -> Path: ...


@overload
def get_git_root() -> Path: ...


def get_git_root(raise_error: bool = True) -> Path | None:
    """
    Finds the root directory of a git repository.

    Args:
        raise_error: Whether to raise an error if not in a git repository.

    Returns:
        The root directory of the git repository, or None (only if raise_error is False).
    """
    try:
        response = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True)
        return Path(response.strip())
    except subprocess.CalledProcessError:
        if raise_error:
            raise RuntimeError("Not in a git repository") from None
    return None
