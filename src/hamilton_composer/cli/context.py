from logging import Logger
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from hamilton_composer.composer import HamiltonComposer
    from hamilton_composer.pipeline import Pipeline
else:
    HamiltonComposer = object
    Pipeline = object


class AppContext:
    """Context manager for the a Hamilton composer application."""

    def __init__(
        self,
        name: str,
        composer: HamiltonComposer,
        logger: Logger,
        *,
        config_directory: str | Path | None = None,
        config_name: str | None = None,
        search_recursive: bool = False,
        search_git_root: bool = False,
    ) -> None:
        self._project_name = name
        self._composer = composer
        self._logger = logger
        self._config_directory = config_directory
        self._config_name = config_name
        self._search_recursive = search_recursive
        self._search_git_root = search_git_root
        self._cached_pipelines: dict[str, Pipeline] | None = None
        self._cached_config: dict[str, Any] | None = None

    @property
    def name(self) -> str:
        """Returns the name of the application."""
        return self._project_name

    @property
    def logger(self) -> Logger:
        """Returns the Python standard library `Logger` pre-configured for the application."""
        return self._logger

    def load_config(self, params: Iterable[str] | None = None) -> dict[str, Any]:
        """Delegate config loading to the current Hamilton composer within the current context."""
        if self._cached_config is None:
            self._cached_config = self._composer.load_config(
                directory=self._config_directory,
                name=self._config_name,
                params=params,
                search_git_root=self._search_git_root,
                search_recursive=self._search_recursive,
            )
        return self._cached_config

    def find_pipelines(self, config: dict[str, Any] | None = None) -> dict[str, Pipeline]:
        """Delegate pipelines search to the current Hamilton composer within the current context."""
        return self._composer.find_pipelines(config)
