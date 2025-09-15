from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hamilton_composer.cli.context import AppContext
else:
    AppContext = object


import click


@click.command(name="shell")
@click.argument("params", nargs=-1, type=str, default=None)
@click.pass_obj
def launch_ipython_shell(context: AppContext, params: tuple[str, ...]) -> None:
    """
    Launch a custom Hamilton Composer IPython shell subjected to the provided OmegaConf PARAMS.

    The shell is intended for exploration or debug and will be pre-populated with the following:
    - config: A dictionary contating the complete OmegaConf-based configuration (subject to global
              config options and OmegaConf PARAMS).
    - pipeline
    """
    from hamilton_composer.exts.ipython import launch_shell

    config = context.load_config(params)
    pipelines = context.find_pipelines(config)
    launch_shell(config=config, pipelines=pipelines)
