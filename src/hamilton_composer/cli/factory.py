import pathlib
from logging import Logger
from typing import TYPE_CHECKING, Iterable

import click

from hamilton_composer.cli.cmds.list import list_pipelines
from hamilton_composer.cli.cmds.run import run_pipelines
from hamilton_composer.cli.cmds.shell import launch_ipython_shell

if TYPE_CHECKING:
    from hamilton_composer.composer import HamiltonComposer
else:
    HamiltonComposer = object

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"], "max_content_width": 120}


def build_cli(
    project_name: str,
    composer: HamiltonComposer,
    *,
    logger: str | Logger | None = None,
    log_file: str | pathlib.Path | None = None,
    help: str = "",
    plugins: Iterable[click.Command] | None = None,
) -> click.Command:
    """
    Build the CLI application with all commands.

    Args:
        project_name (str):
            Name of the project within the CLI. Does not have to be a valid Python name.
        composer (HamiltonComposer):
            Composer instance associated with project pipelines.
        logger (str | logging.Logger, optional):
            Logger (or logger name) that will be configured for logging. If not provided logs will
            be sent to 'hamilton_composer', but no handlers will be configured.
        log_file (str | pathlib.Path, optional):
            Specifies the path or name of the log file where configured logs will be written. If a
            relative path is provided it will be written relative the execution. If not provided,
            logs will not be written to a file.
        help (str):
            Optional help text for the CLI application. If provided, it will be displayed in the
            help message of the CLI, otherwise a default message will be used.
        plugins (Iterable[click.Command], optional):
            Additional click commands that will be nested under the `plugins` subcommand.
    """
    from hamilton_composer.cli.context import AppContext
    from hamilton_composer.logging import configure_logging
    from hamilton_composer.logging import get_default_logger

    if logger is None:
        logger = get_default_logger()
    elif isinstance(logger, str):
        logger = Logger(logger)

    # Main group function
    def main(ctx: click.Context, debug: bool, **kwargs) -> None:
        """Hamilton composer application command line."""
        from colorama import init
        from rich.traceback import install

        init(autoreset=True)
        install(show_locals=False, suppress=[click])
        _patch_hamilton_message()

        if logger:
            configure_logging(logger, log_file=log_file, debug=debug)

        ctx.obj = AppContext(
            name=project_name,
            composer=composer,
            logger=logger,
            config_file=kwargs["config_file"],
            search_git_root=kwargs["search_git_root"],
            search_recursive=kwargs["search_recursive"],
        )

    # Add click decorators dynamically to the main group (needs to be in REVERSE order)
    app = click.pass_context(main)

    app = click.option(
        "--debug",
        "-d",
        is_flag=True,
        default=False,
        help="Enable debug mode for the pipeline execution.",
    )(app)

    app = click.option(
        "--search-recursive",
        "-r",
        is_flag=True,
        default=False,
        help=(
            "Search for the configuration file recursively in parent directories. Only used if "
            "`--config-file` is a relative path (either the specified or default value)."
        ),
    )(app)

    app = click.option(
        "--search-git-root",
        "-g",
        is_flag=True,
        default=False,
        help=(
            "Search for the configuration directory relative to the git root. Only used if "
            "`--config-file` is a relative path (either the specified or default value)."
        ),
    )(app)

    app = click.option(
        "--config-file",
        default=composer.config_file,
        show_default=False,  # The default is set manually in the help
        help=(
            f"Location of YAML configuration file for the project pipelines. "
            f"[default: {composer.config_file}]"
        ),
    )(app)

    # Create the main CLI application group
    app = click.group(
        name=project_name,
        help=help or getattr(main, "__doc__", "Hamilton Composer CLI Application"),
        context_settings=CONTEXT_SETTINGS,
    )(app)

    # Register the subcommands with the main CLI application group
    for cmd in [list_pipelines, run_pipelines, launch_ipython_shell]:
        cmd.context_settings = CONTEXT_SETTINGS
        app.add_command(cmd=cmd)

    # Register the plugins subcommand if plugins have been provided
    if plugins:

        @app.group(name="plugins", context_settings=CONTEXT_SETTINGS)
        def execute_plugin():
            """Registered Hamilton Composer plugin sub-commands."""
            pass

        for plugin in plugins:
            plugin.context_settings = CONTEXT_SETTINGS
            execute_plugin.add_command(cmd=plugin)

    return app


def _patch_hamilton_message() -> None:  # pragma no cover
    """Removes the hamilton slack error message."""
    import hamilton.driver

    hamilton.driver.SLACK_ERROR_MESSAGE = (
        "Hamilton encountered an error during execution (Check traceback)"
    )
