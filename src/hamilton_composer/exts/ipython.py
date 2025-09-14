from typing import TYPE_CHECKING, Any

from IPython.terminal.embed import InteractiveShellEmbed
from IPython.terminal.ipapp import load_default_config

if TYPE_CHECKING:
    from hamilton_composer.pipeline import Pipeline
else:
    Pipeline = object


def launch_shell(config: dict[str, Any], pipelines: dict[str, Pipeline]) -> None:
    """Launches an ipython shell pre-loaded with Hamilton Composer configuration and pipelines."""
    from rich import get_console

    ipython_config = load_default_config()
    shell = InteractiveShellEmbed(config=ipython_config)
    shell.push({"config": config, "pipelines": pipelines})
    shell.show_banner()
    console = get_console()
    console.print(
        "\n[cyan]Hamilton Composer IPython shell[/]\nPreloaded variables: 'config' and 'pipelines'"
    )
    shell.mainloop()
