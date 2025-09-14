from dataclasses import fields
from dataclasses import is_dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, cast, overload

from hamilton_composer.utils import get_git_root

if TYPE_CHECKING:
    from hamilton_composer.pipeline import Pipeline

    PipelineFunction = Callable[[dict[str, Any] | None], dict[str, Pipeline]]
else:
    Pipeline = object
    PipelineFunction = object


class HamiltonComposer:
    """
    A composer that supports managing Hamilton pipelines with optional OmegaConf configurations.

    For more information on OmegaConf see https://omegaconf.readthedocs.io/en/latest/.

    Args:
        pipeline_function (Callable[[dict[str, Any] | Any | None], dict[str, Pipeline]] | str):
            Pipeline creation function. Can be either a callable or a string representing the fully
            qualified name of a module and function. In either case, the function can optionally
            accept a single dictionary argument for configuration parameters and return a dictionary
            mapping pipeline names to their respective `Pipeline` instances.
        config_file (str | pathlib.Path, optional):
            Path to configuration YAML file. If the path is specified as an absolute path, it will
            be used as is. If the path is specified as a relative path, it will be resolved from the
            current working directory subject to the search options in `load_config`. If None, the
            composer will create an empty configuration, note that overrides can still be used in
            this case.
        schema (type, optional):
            Structured config schema (dataclass or attrs class) to validate/complete config. If
            not provided, no validation is performed. Must be either a dataclass either from the
            standard library or a compatible library (i.e. pydantic). For more information on
            see https://omegaconf.readthedocs.io/en/latest/structured_config.html.
    """

    def __init__(
        self,
        pipeline_function: PipelineFunction | str,
        config_file: str | Path | None = None,
        schema: type | None = None,
    ) -> None:
        if not isinstance(pipeline_function, str) and not callable(pipeline_function):
            raise TypeError(
                "pipeline_function must be a callable or a string representing the fully "
                "qualified name of a module and function."
            )
        self._pipeline_function = pipeline_function
        self._config_file = config_file

        self._schema = None
        if schema:
            if not is_dataclass(schema):
                raise ValueError(
                    "Schema must be a dataclass or compatible substitute (i.e pydantic)."
                )
            if not isinstance(schema, type):
                raise ValueError("Schema must be a dataclass type vice a dataclass instance.")

            self._schema = schema

    @property
    def config_file(self) -> Path | None:
        """Returns the location of configuration file (relative or absolute)."""
        return Path(self._config_file) if self._config_file is not None else None

    def load_config(
        self,
        filepath: str | Path | None = None,
        params: Iterable[str] | None = None,
        search_git_root: bool = False,
        search_recursive: bool = False,
    ) -> dict[str, Any]:
        """
        Loads the configuration for the composer using OmegaConf.

        Args:
            filepath (str | Path, optional):
                Override the path to the configuration file specified in `config_file` during
                insanitation. If this is a relative path, it will be resolved against the current
                working directory. If it is an absolute path, it will be used as is. If the path
                does not exist, an error will be raised.
            params (Iterable[str], optional):
                Additional parameters to override the configuration. These should be provided as
                key-value pairs in the form of `key=value`. If not provided, no additional
                parameters will be used.
            search_git_root (bool, optional):
                If True attempts to find the config directory relative to the git root. Defaults to
                False. Ignored if `filepath` is an absolute path.
            search_recursive (bool, optional):
                If True, searches for the config directory recursively from the current working
                directory. Defaults to False. Ignored if `filepath` is an absolute path.

        Returns:
            The composed configuration as a dictionary or an instance of the current schema.

        """
        from omegaconf import OmegaConf

        config_file = self._resolve_config_file(
            filepath if filepath is not None else self._config_file,
            search_git_root=search_git_root,
            search_recursive=search_recursive,
        )

        params = OmegaConf.from_dotlist(list(params)) if params else OmegaConf.create()

        composed = OmegaConf.load(config_file) if config_file else OmegaConf.create()
        composed = OmegaConf.merge(composed, params)

        if self._schema:
            structured_default = OmegaConf.structured(self._schema)
            merged = OmegaConf.merge(structured_default, composed)
            instance = OmegaConf.to_object(merged)
            assert is_dataclass(instance), "Schema must be a dataclass type."
            config = {field.name: getattr(instance, field.name) for field in fields(instance)}
            return config

        container = cast(dict[str, Any], OmegaConf.to_container(composed, resolve=True))
        assert isinstance(container, dict)
        return container

    @overload
    def find_pipelines(self, config: dict[str, Any]) -> dict[str, Pipeline]: ...

    @overload
    def find_pipelines(self, config: Any) -> dict[str, Pipeline]: ...

    @overload
    def find_pipelines(self) -> dict[str, Pipeline]: ...

    def find_pipelines(self, config: dict[str, Any] | Any | None = None) -> dict[str, Pipeline]:
        """
        Finds and returns all pipelines defined in the specified module.

        Args:
            config (dict[str, Any], optional):
                Configuration parameters for pipeline creation. These will be passed to the pipeline
                creation function. If not provided, an empty dictionary will be used.

        Returns:
            A dictionary mapping pipeline names to their respective Pipeline instances.
        """
        from importlib import import_module

        if isinstance(self._pipeline_function, str):
            module_name, func_name = self._pipeline_function.rsplit(".", 1)
            module = import_module(module_name)
            create_pipelines_func = getattr(module, func_name, None)
            if create_pipelines_func is None:  # pragma: no cover
                raise ValueError(f"Function '{func_name}' not found in module '{module_name}'")
        else:
            create_pipelines_func = self._pipeline_function

        return create_pipelines_func(config)

    def _resolve_config_file(
        self,
        initial_path: str | Path | None,
        search_git_root: bool,
        search_recursive: bool,
    ) -> Path | None:
        """Resolves the location of the configuration file to an absolute path."""
        if initial_path is None:
            return None

        path = Path(initial_path)

        if path.is_absolute():
            if not path.exists():
                raise FileNotFoundError(f"Configuration directory '{path}' does not exist.")
            return path.resolve()

        local_path = Path.cwd().joinpath(path)
        if local_path.exists():
            return local_path.resolve()

        if search_git_root:
            git_root = get_git_root(raise_error=False)
            if git_root is not None:
                git_path = git_root.joinpath(path)
                if git_path.exists():
                    return git_path.resolve()

        if search_recursive:
            current_dir = Path.cwd()
            while current_dir != current_dir.parent:
                candidate_path = current_dir.joinpath(path)
                if candidate_path.exists():
                    return candidate_path.resolve()
                current_dir = current_dir.parent

        raise FileNotFoundError(
            f"Configuration directory '{path}' not found in the current working directory or "
            f"git root. Consider using an absolute path."
        )
