from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    from hamilton_composer.cli.context import AppContext
else:
    AppContext = object


@click.command(
    name="run",
    short_help="Execute a specific PIPELINE from the project.",
)
@click.argument("pipeline_name", required=True)
@click.argument("params", nargs=-1, type=str, default=None)
@click.pass_obj
def run_pipelines(context: AppContext, pipeline_name: str, params: tuple[str, ...]) -> None:
    """
    Execute a specific PIPELINE from the project subjected to the provided PARAMS.

    At a basic level, PARAMS are key-value pairs that can be used to either configure the Hamilton
    Driver or to provide input values to the pipeline. PARAMS should be provided in the form
    of `key=value` pairs (also known as a dotlist in OmegaConf). Note that depending on the shell
    you are using, you may need to wrap the key-value pairs in quotes to prevent them from being
    interpreted by the shell.

    For more information on OmegaConf dotlists see:
    https://omegaconf.readthedocs.io/en/2.3_branch/usage.html#from-a-dot-list.

    Example usage:
        $ hamilton-composer run my_pipeline param1=value1 param2=value2
    """
    config = context.load_config(params)
    pipelines = context.find_pipelines(config)
    pipeline = pipelines.get(pipeline_name)
    if pipeline is None:
        raise click.BadParameter(f"Pipeline '{pipeline_name}' not found.")
    if not pipeline.public:
        raise click.BadParameter(
            f"Pipeline '{pipeline_name}' is private and cannot be executed from the command line."
        )
    _ = pipeline.execute(inputs=config)
