from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, cast

from hamilton_composer.utils import get_git_root

if TYPE_CHECKING:
    from hamilton_composer.pipeline import Pipeline

    PipelineFunction = Callable[[dict[str, Any]], dict[str, Pipeline]]
else:
    Pipeline = object
    PipelineFunction = object


class HamiltonComposer:
    """
    A composer that supports managing Hamilton pipelines with optional Hydra configurations.

    For more information on Hydra see the documentation https://hydra.cc/docs/intro/.

    Args:
        pipeline_function (Callable | str):
            Pipeline creation function. Can be either a callable or a string representing the fully
            qualified name of a module and function. In either case, the function should accept a
            single dictionary argument for configuration parameters and return a dictionary mapping
            pipeline names to their respective `Pipeline` instances.
        config_directory (str | Path | None):
            Path to the directory containing Hydra config files.
            - If a relative path, it will be resolved against the current working directory or
            along the specified search paths
            - If it is an absolute path, it will be used as is
            - If a str or path is specified but does not exist, an error will be raised
            - If None, the composer will bypass configuration file loading creating and empty
            configuration, note that Hydra overrides can still be used in this case.
        config_name (str, optional):
            Name of the root Hydra config file (without extension). Must be specified if
            `config_dir` is provided.
        schema (type, optional):
            Structured config schema (dataclass or attrs class) to validate/complete config. If
            not provided, no validation is performed. For more information on structured
            configuration, see https://hydra.cc/docs/tutorials/structured_config/intro.
        schema_name (str, optional):
            Name of the schema to be used when registering it with Hydra's ConfigStore. If not
            provided, it will default to `config_name + "_schema"`. Note that this should be
            referred to in the default list of the configuration file.
    """

    def __init__(
        self,
        pipeline_function: PipelineFunction | str,
        config_directory: str | Path | None = None,
        config_name: str | None = None,
        schema: type | None = None,
        schema_name: str | None = None,
    ) -> None:
        self._pipeline_function = pipeline_function
        self._config_directory, self._config_name = self._resolve_config_params(
            config_directory, config_name
        )
        self._schema = schema
        if self._schema:
            if schema_name:
                self._schema_name = schema_name
            elif self._config_name:
                self._schema_name = self._config_name + "_schema"
            else:
                raise ValueError(
                    "When a schema is provided, either `config_name` or `schema_name` "
                    "must also be specified."
                )

    @property
    def config_directory(self) -> Path | None:
        """Returns the directory path that will be used for loading configurations."""
        return Path(self._config_directory) if self._config_directory is not None else None

    @property
    def config_name(self) -> str | None:
        """Returns the name of the root configuration file."""
        return self._config_name

    def load_config(
        self,
        directory: str | Path | None = None,
        name: str | None = None,
        params: Iterable[str] | None = None,
        search_git_root: bool = False,
        search_recursive: bool = False,
    ) -> dict[str, Any]:
        """
        Loads the configuration for the composer using Hydra's initialize/compose API.

        Args:
            directory (str | Path, optional):
                Manually specify the path to the configuration directory. If not provided, the
                `config_dir` specified during initialization will be used. If this is a relative
                path, it will be resolved against the current working directory. If it is an
                absolute path, it will be used as is. If the path does not exist, an error will be
                raised.
            name (str, optional):
                Manually specify the name of the root configuration file (without extension). If not
                provided, the `config_name` specified during initialization will be used.
            params (list[str], optional):
                Iterable of parameters for Hydra. E.g., ["db.user=chuck", "debug=True"]. For more
                information see https://hydra.cc/docs/advanced/override_grammar/basic/.
            search_git_root (bool, optional):
                If True attempts to find the config directory relative to the git root. Defaults to
                False. Ignored if `config_dir` is an absolute path.
            search_recursive (bool, optional):
                If True, searches for the config directory recursively from the current working
                directory. Defaults to False. Ignored if `config_dir` is an absolute path.

        Returns:
            The composed configuration as a standard dictionary or structured config, depending on
            whether a schema was provided.
        """
        from hydra import compose
        from hydra import initialize
        from hydra import initialize_config_dir
        from hydra.core.config_store import ConfigStore
        from omegaconf import OmegaConf

        directory, name = self._resolve_config_params(directory, name)

        # Resolve the configuration parameters
        config_dir = self._resolve_config_dir(
            directory if directory is not None else self._config_directory,
            search_git_root,
            search_recursive,
        )
        config_name = name if name is not None else self._config_name
        params = list(params) if params is not None else []

        # We need to select the appropriate hydra initializer
        initializer_kwargs: dict[str, Any] = {}
        initializer: type
        if config_dir is None:
            initializer = initialize
            initializer_kwargs = {"config_path": None, "version_base": None}
            config_name = None
        else:
            initializer = initialize_config_dir
            initializer_kwargs = {"config_dir": config_dir, "version_base": None}

        # Schemas need to be registered in the ConfigStore
        if self._schema is not None:
            cs = ConfigStore.instance()
            cs.store(name=self._schema_name, node=self._schema)

        # Initialize Hydra and compose the configuration
        with initializer(**initializer_kwargs):
            cfg = compose(config_name=config_name, overrides=params)
            container = OmegaConf.to_container(cfg, resolve=True)
        return cast(dict[str, Any], container)

    def find_pipelines(self, config: dict[str, Any] | None = None) -> dict[str, Pipeline]:
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

        config = config if config is not None else {}
        return create_pipelines_func(config)

    def _resolve_config_params(
        self, directory: str | Path | None, name: str | None
    ) -> tuple[Path | str | None, str | None]:
        """Validates the configuration directory and name."""
        if len(set([directory is not None, name is not None])) != 1:
            raise ValueError(
                "Either both the configuration directory and configuration name must be provided, "
                "or neither."
            )
        if name:
            name = name.replace(".yaml", "").replace(".yml", "") if name else None
        return directory, name

    def _resolve_config_dir(
        self,
        search_dir: str | Path | None,
        search_git_root: bool,
        search_recursive: bool,
    ) -> str | None:
        """
        Resolves the configuration directory to an absolute path.

        Note that this method returns a string so that it can be used directly with Hydra's
        `initialize_config_dir` or `initialize` functions, which both expect a string path.
        """
        if search_dir is None:
            return None

        path = Path(search_dir)

        if path.is_absolute():
            if not path.exists():
                raise FileNotFoundError(f"Configuration directory '{path}' does not exist.")
            return str(path.resolve())

        local_path = Path.cwd().joinpath(path)
        if local_path.exists():
            return str(local_path.resolve())

        if search_git_root:
            git_root = get_git_root(raise_error=False)
            if git_root is not None:
                git_path = git_root.joinpath(path)
                if git_path.exists():
                    return str(git_path.resolve())

        if search_recursive:
            current_dir = Path.cwd()
            while current_dir != current_dir.parent:
                candidate_path = current_dir.joinpath(path)
                if candidate_path.exists():
                    return str(candidate_path.resolve())
                current_dir = current_dir.parent

        raise FileNotFoundError(
            f"Configuration directory '{path}' not found in the current working directory or "
            f"git root. Consider using an absolute path."
        )
