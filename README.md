# Hamilton Composer

Hamilton Composer simplifies the creation and management of [Hamilton](https://hamilton.dagworks.io/) data pipelines with built-in CLI support and optional Hydra configuration management. Transform your Hamilton DAGs into executable applications with minimal boilerplate.

## Features

- **Simple Pipeline Management**: Wrap Hamilton Builders in reusable Pipeline objects
- **CLI Generation**: Automatically generate command-line interfaces for your pipelines
- **Configuration Management**: Optional Hydra integration for flexible configuration
- **Interactive Shell**: Built-in IPython shell with preloaded pipelines and config
- **Jupyter Integration**: Use your pipelines directly in Jupyter notebooks
- **Type Safety**: Full type hints throughout the codebase

## Installation

Install using [uv](https://docs.astral.sh/uv/) (recommended):

```bash
uv add hamilton-composer
```

For IPython/Jupyter support:
```bash
uv add "hamilton-composer[ipython]"
```

For visualization support:
```bash
uv add "hamilton-composer[graphviz]"
```

## Quick Start Examples

### 1. Simple Application (Composer Only)

Create a basic Hamilton Composer application without CLI or configuration:

```python
# my_functions.py
def process_data(raw_data: str) -> str:
    return raw_data.strip().upper()

def count_words(process_data: str) -> int:
    return len(process_data.split())

# app.py
from typing import Any, Dict
from hamilton.driver import Builder
from hamilton_composer import HamiltonComposer, Pipeline
import my_functions

def create_pipelines(config: Dict[str, Any] | None = None) -> Dict[str, Pipeline]:
    """Create pipelines for the application."""
    builder = Builder().with_modules(my_functions)

    return {
        "word_counter": Pipeline(
            builder=builder,
            final_vars=["count_words"],
            description="Counts words in processed text"
        )
    }

# Create and use the composer
if __name__ == "__main__":
    composer = HamiltonComposer(create_pipelines)
    pipelines = composer.find_pipelines()

    # Execute the pipeline
    result = pipelines["word_counter"].execute(
        inputs={"raw_data": "  hello world  "}
    )
    print(f"Word count: {result['count_words']}")  # Output: Word count: 2
```

### 2. CLI Application (No Configuration)

Add a command-line interface to your application:

```python
# pipeline_functions.py
def load_data(file_path: str) -> str:
    with open(file_path, 'r') as f:
        return f.read()

def clean_data(load_data: str) -> str:
    return load_data.strip().lower()

def analyze_data(clean_data: str) -> dict:
    words = clean_data.split()
    return {
        "word_count": len(words),
        "unique_words": len(set(words)),
        "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0
    }

# cli_app.py
from typing import Any, Dict
from hamilton.driver import Builder
from hamilton_composer import HamiltonComposer, Pipeline, build_cli
import pipeline_functions

def create_pipelines(config: Dict[str, Any] | None = None) -> Dict[str, Pipeline]:
    """Create analysis pipelines."""
    builder = Builder().with_modules(pipeline_functions)

    return {
        "text_analyzer": Pipeline(
            builder=builder,
            final_vars=["analyze_data"],
            description="Analyzes text files for basic statistics"
        ),
        "data_cleaner": Pipeline(
            builder=builder,
            final_vars=["clean_data"],
            description="Cleans and normalizes text data",
            public=True  # Accessible via CLI
        )
    }

def main():
    composer = HamiltonComposer(create_pipelines)
    cli = build_cli("text-processor", composer)
    cli()

if __name__ == "__main__":
    main()
```

### 3. Advanced Application (CLI + Hydra Configuration)

Create a fully-featured application with configuration management:

**Project Structure:**
```
my_project/
├── config/
│   ├── config.yaml
│   ├── database/
│   │   ├── sqlite.yaml
│   │   └── postgresql.yaml
│   └── processing/
│       ├── fast.yaml
│       └── thorough.yaml
├── src/
│   ├── data_functions.py
│   └── app.py
└── run.py
```

**Configuration Files:**
```yaml
# config/config.yaml
defaults:
  - database: sqlite
  - processing: fast

output_path: "results.json"
```

```yaml
# config/database/sqlite.yaml
driver: "sqlite"
path: "data.db"
```

```yaml
# config/processing/fast.yaml
max_iterations: 10
use_cache: true
```

**Application Code:**
```python
# src/data_functions.py
from hamilton.function_modifiers import config

def load_from_db(database: dict) -> dict:
    """Load data from configured database."""
    if database["driver"] == "sqlite":
        # SQLite loading logic
        return {"data": f"SQLite data from {database['path']}"}
    else:
        # PostgreSQL loading logic
        return {"data": f"PostgreSQL data from {database['host']}"}

@config.when(processing__use_cache=True)
def process_with_cache(load_from_db: dict, processing: dict) -> dict:
    """Process data with caching enabled."""
    return {
        "processed": f"Cached processing: {load_from_db['data']} "
                    f"(max_iter: {processing['max_iterations']})"
    }

@config.when(processing__use_cache=False)
def process_without_cache(load_from_db: dict, processing: dict) -> dict:
    """Process data without caching."""
    return {
        "processed": f"No-cache processing: {load_from_db['data']} "
                    f"(max_iter: {processing['max_iterations']})"
    }

def save_results(process_with_cache: dict, process_without_cache: dict,
                output_path: str) -> str:
    """Save processing results."""
    # This function will receive the output from whichever process_* function ran
    data = process_with_cache or process_without_cache
    return f"Saved to {output_path}: {data['processed']}"

# src/app.py
from typing import Any, Dict
from hamilton.driver import Builder
from hamilton_composer import HamiltonComposer, Pipeline
from . import data_functions

def create_pipelines(config: Dict[str, Any] | None = None) -> Dict[str, Pipeline]:
    """Create data processing pipelines."""
    builder = Builder().with_modules(data_functions).with_config(config or {})

    return {
        "data_pipeline": Pipeline(
            builder=builder,
            final_vars=["save_results"],
            description="Complete data processing pipeline with configurable components"
        )
    }

# run.py
from hamilton_composer import build_cli
from src.app import create_pipelines, HamiltonComposer

def main():
    composer = HamiltonComposer(
        create_pipelines,
        config_directory="config",
        config_name="config"
    )

    cli = build_cli(
        "data-processor",
        composer,
        help="Advanced data processing application with Hydra configuration"
    )
    cli()

if __name__ == "__main__":
    main()
```

## Using the CLI

Once you have a CLI-enabled application, you can use these commands:

### List Available Pipelines
```bash
python run.py list
```

### Run a Pipeline
```bash
# Basic execution
python run.py run data_pipeline

# With Hydra configuration overrides
python run.py run data_pipeline database=postgresql processing.max_iterations=50

# With multiple overrides
python run.py run data_pipeline database=postgresql processing=thorough output_path=custom_results.json
```

### Launch Interactive Shell
```bash
# Basic shell
python run.py shell

# Shell with configuration overrides
python run.py shell database=postgresql processing=thorough
```

The shell provides access to:
- `config`: Loaded configuration dictionary
- `pipelines`: Dictionary of available Pipeline objects

### CLI Options
```bash
# Enable debug logging
python run.py --debug run data_pipeline

# Use different config directory
python run.py --config-dir ./custom_config run data_pipeline

# Search for config in git root
python run.py --search-git-root run data_pipeline
```

## Jupyter Notebook Integration

Use Hamilton Composer pipelines directly in Jupyter notebooks:

```python
# In a Jupyter cell
from src.app import create_pipelines
from hamilton_composer import HamiltonComposer

# Create composer
composer = HamiltonComposer(
    create_pipelines,
    config_directory="config",
    config_name="config"
)

# Load configuration with overrides
config = composer.load_config(params=["database=postgresql", "processing.use_cache=false"])

# Get pipelines
pipelines = composer.find_pipelines(config)

# Execute pipeline
result = pipelines["data_pipeline"].execute()
print(result)

# Visualize pipeline (requires graphviz)
pipelines["data_pipeline"].visualize_execution()
```

You can also launch the interactive shell from within a notebook:

```python
# This will open an IPython shell in your terminal
from hamilton_composer.exts.ipython import launch_shell

config = composer.load_config()
pipelines = composer.find_pipelines(config)
launch_shell(config=config, pipelines=pipelines)
```

## Contributing

We use modern Python tooling for development. Here's how to get started:

### Development Setup

1. **Install uv** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone and setup the project**:
   ```bash
   git clone <repository-url>
   cd hamilton_composer
   uv sync
   ```

3. **Install pre-commit hooks**:
   ```bash
   uv run pre-commit install
   ```

### Development Commands

```bash
# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=hamilton_composer

# Format code
uv run ruff format

# Lint code
uv run ruff check

# Type checking
uv run mypy

# Run all checks (what CI runs)
uv run nox
```

### Recommended VS Code Settings

Create `.vscode/settings.json` in your project:

```json
{
    "python.defaultInterpreterPath": "./.venv/bin/python",
    "python.terminal.activateEnvironment": true,
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": ["tests"],
    "python.linting.enabled": true,
    "python.linting.ruffEnabled": true,
    "python.formatting.provider": "black",
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.organizeImports": true
    },
    "files.exclude": {
        "**/__pycache__": true,
        "**/.pytest_cache": true,
        "**/.mypy_cache": true,
        "**/.coverage": true,
        "**/htmlcov": true
    }
}
```

### Project Structure

- `src/hamilton_composer/` - Main package code
- `tests/` - Unit tests
- `tests/cases/` - Integration test scenarios
- `noxfile.py` - Automated testing and linting
- `pyproject.toml` - Project configuration and dependencies

### Pull Request Guidelines

1. **Create feature branch**: `git checkout -b feature/your-feature-name`
2. **Make changes**: Follow existing code patterns and style
3. **Add tests**: Ensure your changes are covered by tests
4. **Run checks**: `uv run nox` should pass
5. **Commit**: Use clear, descriptive commit messages
6. **Push and create PR**: Include description of changes and testing done

Pre-commit hooks will automatically:
- Format code with Ruff
- Lint code with Ruff
- Type check with MyPy
- Run basic tests

For questions or suggestions, please open an issue or start a discussion.