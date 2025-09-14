# Hamilton Composer

Hamilton Composer simplifies the creation and management of [Hamilton](https://hamilton.dagworks.io/) data pipelines with built-in CLI support and optional OmegaConf configuration management. Transform your Hamilton DAGs into executable applications with minimal boilerplate.

## Features

- **Simple Pipeline Management**: Wrap Hamilton Builders in reusable Pipeline objects
- **Configuration Management**: Optional OmegaConf integration for flexible YAML-based configuration
- **CLI Generation**: Automatically generate command-line interfaces for your pipelines
- **Interactive Shell**: Built-in IPython shell with preloaded pipelines and config
- **Jupyter Integration**: Use your pipelines directly in Jupyter notebooks
- **Type Safety**: Full type hints throughout the codebase

## Installation

Install using [uv](https://docs.astral.sh/uv/) (recommended):

```bash
uv pip install hamilton-composer
```

Or with pip:

```bash
pip install hamilton-composer
```

### Optional Dependencies

For IPython/Jupyter support:
```bash
uv pip install "hamilton-composer[ipython]"
```

For visualization support:
```bash
uv pip install "hamilton-composer[graphviz]"
```

## Quick Start Examples

### 1. Simple Programmatic Usage

Create and execute pipelines directly in your Python code:

```python
from hamilton.driver import Builder
from hamilton_composer import Pipeline, HamiltonComposer

# Define your Hamilton functions
def double_value(input_value: int) -> int:
    return input_value * 2

def add_ten(double_value: int) -> int:
    return double_value + 10

# Create a pipeline
def create_pipelines(config=None):
    from hamilton.ad_hoc_utils import create_temporary_module

    module = create_temporary_module(double_value, add_ten)
    builder = Builder().with_modules(module)

    return {
        "math_pipeline": Pipeline(
            builder=builder,
            final_vars=["add_ten"],
            description="Doubles input and adds 10"
        )
    }

# Use the composer
composer = HamiltonComposer(create_pipelines)
pipelines = composer.find_pipelines()
result = pipelines["math_pipeline"].execute({"input_value": 5})
print(result)  # {"add_ten": 20}
```

### 2. CLI Application (No Configuration)

Create a command-line application from your pipelines:

```python
# my_app.py
from typing import Any
from hamilton.driver import Builder
from hamilton.ad_hoc_utils import create_temporary_module
from hamilton_composer import HamiltonComposer, Pipeline, build_cli

def process_text(raw_text: str) -> str:
    """Clean and process input text."""
    return raw_text.strip().upper()

def count_words(process_text: str) -> int:
    """Count words in processed text."""
    return len(process_text.split())

def count_chars(process_text: str) -> int:
    """Count characters in processed text."""
    return len(process_text)

def create_pipelines(config: dict[str, Any] | None = None) -> dict[str, Pipeline]:
    """Create pipelines for the text processing application."""
    module = create_temporary_module(process_text, count_words, count_chars)
    builder = Builder().with_modules(module)

    return {
        "word_counter": Pipeline(
            builder=builder,
            final_vars=["count_words"],
            description="Counts words in processed text"
        ),
        "char_counter": Pipeline(
            builder=builder,
            final_vars=["count_chars"],
            description="Counts characters in processed text"
        ),
        "full_analysis": Pipeline(
            builder=builder,
            final_vars=["count_words", "count_chars", "process_text"],
            description="Complete text analysis"
        )
    }

if __name__ == "__main__":
    composer = HamiltonComposer(create_pipelines)
    cli = build_cli("text-processor", composer)
    cli()
```

Run your CLI application:

```bash
python my_app.py list                                     # List available pipelines
python my_app.py run word_counter raw_text="Hello world"  # Run specific pipeline
python my_app.py shell                                    # Launch interactive shell
```

### 3. Advanced Application with Configuration

Use YAML configuration files for more complex applications:

**config.yaml:**
```yaml
# Configuration for text processing
preprocessing:
  uppercase: true
  strip_whitespace: true

analysis:
  min_word_length: 3
  include_punctuation: false
```

**advanced_app.py:**
```python
from dataclasses import dataclass
from typing import Any
from hamilton.driver import Builder
from hamilton.ad_hoc_utils import create_temporary_module
from hamilton_composer import HamiltonComposer, Pipeline, build_cli

@dataclass
class AppConfig:
    """Configuration schema for the application."""

    raw_text: str
    preprocessing: dict[str, Any]
    analysis: dict[str, Any]


def process_text(raw_text: str, uppercase: bool, strip_whitespace: bool) -> str:
    """Process text based on configuration."""
    result = raw_text
    if strip_whitespace:
        result = result.strip()
    if uppercase:
        result = result.upper()
    return result


def filter_words(process_text: str, min_word_length: int) -> list[str]:
    """Filter words by minimum length."""
    words = process_text.split()
    return [word for word in words if len(word) >= min_word_length]


def word_stats(filter_words: list[str]) -> dict[str, int]:
    """Generate word statistics."""
    return {
        "total_words": len(filter_words),
        "avg_word_length": sum(len(word) for word in filter_words) // len(filter_words)
        if filter_words
        else 0,
    }


def create_pipelines(config: dict[str, Any] | None = None) -> dict[str, Pipeline]:
    """Create pipelines with configuration support."""
    module = create_temporary_module(process_text, filter_words, word_stats)

    # Use config values as builder config
    builder_config = {}
    if config:
        builder_config.update(config.get("preprocessing", {}))
        builder_config.update(config.get("analysis", {}))

    builder = Builder().with_modules(module).with_config(builder_config)

    return {
        "analyze": Pipeline(
            builder=builder,
            final_vars=["word_stats"],
            description="Analyze text with configurable preprocessing",
        )
    }


if __name__ == "__main__":
    composer = HamiltonComposer(create_pipelines, config_file="config.yaml", schema=AppConfig)
    cli = build_cli("advanced-processor", composer)
    cli()

```

Run with configuration:

```bash
python advanced_app.py run analyze raw_text="Hello beautiful world"
python advanced_app.py run analyze raw_text="Hello world" preprocessing.uppercase=false
python advanced_app.py shell --config-file custom_config.yaml
```

## Using the CLI

Once you have a CLI-enabled application, you can use these commands:

### List Available Pipelines

```bash
python my_app.py list
```

This shows all public pipelines with their descriptions and final variables.

### Run a Pipeline

```bash
# Basic execution
python my_app.py run <pipeline_name>

# With configuration overrides using a OmegaConf dotlist
python my_app.py run <pipeline_name> <input_param1>=<value1> <input_param2>=<value2>

# With custom config file
python my_app.py --config-file run <pipeline_name> custom.yaml <input_param>=<value>
```

Examples:
```bash
python my_app.py run word_counter raw_text="Count these words"
python my_app.py run analyze raw_text="Hello world" analysis.min_word_length=2
```

### Launch Interactive Shell

```bash
python my_app.py shell
```

This launches an IPython shell with:
- All pipelines preloaded as variables
- Configuration available as `config`
- Hamilton composer available as `composer`

### CLI Options

Global options available for all commands:

- `--config-file, -c`: Specify a custom configuration file
- `--search-git-root, -g`: Search for config files from git root
- `--search-recursive, -r`: Search for config files recursively in parent directories
- `--debug, -d`: Enable debug mode with detailed logging
- `--help, -h`: Show help information

## Advanced Features

### Pipeline Configuration

Pipelines support several configuration options:

```python
Pipeline(
    builder=builder,
    final_vars=["output1", "output2"],
    description="Pipeline description shown in CLI",
    tags=["processing", "analysis"],  # Tags for organization
    public=True  # Whether pipeline appears in CLI (default: True)
)
```

### Lifecycle Adapters

Add Hamilton lifecycle adapters to your pipelines:

```python
from hamilton.plugins.h_logging import LoggingAdapter

def create_pipelines(config=None):
    builder = (Builder()
        .with_modules(my_module)
        .with_adapters(LoggingAdapter("my_logger"))
    )

    return {
        "logged_pipeline": Pipeline(builder, ["output"])
    }
```

### Custom CLI Plugins

Extend the CLI with custom commands:

```python
import click

@click.command()
@click.pass_context
def custom_command(ctx):
    """My custom command."""
    composer = ctx.obj.composer
    # Your custom logic here
    click.echo("Custom command executed!")

# Add to CLI
cli = build_cli("my-app", composer, plugins=[custom_command])
```

### Error Handling

Hamilton Composer provides rich error reporting and debugging:

```bash
# Run with debug mode for detailed error information
python my_app.py --debug run my_pipeline --input_value 42
```

### Jupyter Notebook Integration

Use your pipelines directly in Jupyter notebooks:

```python
# In a Jupyter cell
from my_app import create_pipelines
from hamilton_composer import HamiltonComposer

composer = HamiltonComposer(create_pipelines, config_file="config.yaml")
pipelines = composer.get_pipelines()

# Execute pipeline
result = pipelines["analyze"].execute({"raw_text": "Jupyter integration test"})
display(result)

# Visualize pipeline (requires graphviz)
pipelines["analyze"].visualize()
```

## Configuration Management

Hamilton Composer uses [OmegaConf](https://omegaconf.readthedocs.io/) for configuration management:

### Configuration Loading

```python
composer = HamiltonComposer(
    create_pipelines,
    config_file="config.yaml",  # Path to config file
    schema=MyConfigSchema       # Optional validation schema
)

# Load with overrides
config = composer.load_config(
    params=["key=value", "nested.key=new_value"],
    search_git_root=True,
    search_recursive=True
)
```

### Configuration Schema

Use dataclasses to validate and structure your configuration:

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class DatabaseConfig:
    host: str
    port: int = 5432
    username: Optional[str] = None

@dataclass
class AppConfig:
    database: DatabaseConfig
    debug: bool = False

composer = HamiltonComposer(
    create_pipelines,
    config_file="config.yaml",
    schema=AppConfig
)
```

## Best Practices

1. **Pipeline Organization**: Group related functionality into separate pipelines
2. **Configuration**: Use structured config schemas for complex applications
3. **Documentation**: Provide clear descriptions and docstrings for your functions
4. **Error Handling**: Use Hamilton's built-in error handling and logging
5. **Testing**: Test your pipeline functions independently before composing them
