from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    from hamilton_composer.cli.context import AppContext
else:
    AppContext = object


@click.command(name="list")
@click.pass_obj
def list_pipelines(context: AppContext) -> None:
    """List available pipelines within the project."""
    from rich import get_console
    from rich.table import Table

    pipelines = context.find_pipelines()
    console = get_console()
    table = Table(title="", show_lines=False, box=None)
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Description", style=None, no_wrap=False)

    if not pipelines:
        console.print("[orange]No pipelines available.[/]")
        return

    for name, pipeline in sorted(pipelines.items()):
        if pipeline.public:
            table.add_row(name, pipeline.description or "No description provided")

    console.print()
    console.print(table)
