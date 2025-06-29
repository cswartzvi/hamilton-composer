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
    Execute a specific PIPELINE from the project subjected to the provided Hydra PARAMS.

    At a basic level Hydra PARAMS can be provided in the form of `key=value` pairs. More advanced
    patterns are also possible. For more information on see the documentation:
    https://hydra.cc/docs/advanced/override_grammar/basic/
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
