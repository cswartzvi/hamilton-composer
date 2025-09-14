import os
from dataclasses import dataclass

import pytest

from hamilton_composer.composer import HamiltonComposer


@dataclass
class SimpleSchema:
    key: str


@dataclass
class Name:
    first: str
    last: str


@dataclass
class NestedSchema:
    name: Name
    age: int


class TestComposerInitialization:
    """Test basic initialization of HamiltonComposer."""

    def test_initialization_without_config(self):
        """Test composer initialization without configuration."""
        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        assert composer.config_file is None

        config = composer.load_config()
        assert config == {}

        pipelines = composer.find_pipelines(config)
        assert set(pipelines) == {"simple_pipeline", "branched_pipeline"}

    def test_initialization_from_module_function(self):
        """Test composer initialization with module function."""
        from .. import defs

        composer = HamiltonComposer(defs.pipelines.create_pipelines)
        assert composer.config_file is None

        config = composer.load_config()
        assert config == {}

        pipelines = composer.find_pipelines(config)
        assert set(pipelines) == {"simple_pipeline", "branched_pipeline"}


class TestComposerConfigLoading:
    """Test configuration loading functionality."""

    def test_load_config_from_file(self, tmp_path):
        """Test loading configuration from YAML file."""
        os.chdir(tmp_path)
        config_file = tmp_path / "config.yaml"
        config_file.write_text("numbers: [1, 2, 3]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=config_file
        )

        config = composer.load_config()
        assert config == {"numbers": [1, 2, 3]}

        pipelines = composer.find_pipelines(config)
        assert set(pipelines) == {"simple_pipeline", "branched_pipeline"}

    def test_load_config_file_not_found(self, tmp_path):
        """Test error when config file doesn't exist."""
        os.chdir(tmp_path)
        config_file = tmp_path / "nonexistent.yaml"

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=config_file
        )

        with pytest.raises(FileNotFoundError):
            composer.load_config()

    def test_load_config_with_parameters_override(self, tmp_path):
        """Test loading config with parameter overrides."""
        os.chdir(tmp_path)
        config_file = tmp_path / "config.yaml"
        config_file.write_text("numbers: [1, 2, 3]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=config_file
        )

        config = composer.load_config(params=["numbers=[4,5,6]"])
        assert config == {"numbers": [4, 5, 6]}


class TestComposerWithSchema:
    """Test schema validation functionality."""

    def test_schema_validation_success(self, tmp_path):
        """Test successful schema validation."""
        os.chdir(tmp_path)
        config_file = tmp_path / "config.yaml"
        config_file.write_text("key: test_value")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=config_file, schema=SimpleSchema
        )

        config = composer.load_config()
        assert config == {"key": "test_value"}

    def test_schema_with_defaults(self, tmp_path):
        """Test schema providing default values."""
        os.chdir(tmp_path)
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("# empty config")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=config_file, schema=SimpleSchema
        )

        # This should work with missing fields by providing them via params
        config = composer.load_config(params=["key=from_params"])
        assert config == {"key": "from_params"}


class TestComposerValidation:
    """Test validation and error handling in HamiltonComposer."""

    def test_invalid_pipeline_function_type(self):
        """Test error when pipeline_function is not callable or string."""
        with pytest.raises(TypeError, match="pipeline_function must be a callable or a string"):
            HamiltonComposer(123)  # pyright: ignore

    def test_non_dataclass_schema(self):
        """Test error when schema is not a dataclass."""

        class NotADataclass:
            pass

        with pytest.raises(ValueError, match="Schema must be a dataclass"):
            HamiltonComposer("tests.defs.pipelines.create_pipelines", schema=NotADataclass)

    def test_schema_instance_instead_of_type(self):
        """Test error when schema is a dataclass instance instead of type."""
        schema_instance = SimpleSchema(key="test")

        with pytest.raises(ValueError, match="Schema must be a dataclass type"):
            HamiltonComposer("tests.defs.pipelines.create_pipelines", schema=schema_instance)  # pyright: ignore


class TestComposerConfigResolution:
    """Test configuration file resolution with different search strategies."""

    def test_config_resolution_with_git_search(self, tmp_path):
        """Test config file resolution searching in git root."""
        import subprocess

        os.chdir(tmp_path)
        # Create git repo structure
        subprocess.run(["git", "init"], check=True, capture_output=True)

        # Create config in git root
        config_file = tmp_path / "project_config.yaml"
        config_file.write_text("numbers: [1, 2, 3]")

        # Create and move to subdirectory
        subdir = tmp_path / "sub" / "deep"
        subdir.mkdir(parents=True)
        os.chdir(subdir)

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file="project_config.yaml"
        )

        config = composer.load_config(search_git_root=True)
        assert config == {"numbers": [1, 2, 3]}

    def test_config_resolution_with_recursive_search(self, tmp_path):
        """Test config file resolution with recursive directory search."""
        os.chdir(tmp_path)

        # Create config in parent directory
        config_file = tmp_path / "project_config.yaml"
        config_file.write_text("numbers: [4, 5, 6]")

        # Create and move to subdirectory
        subdir = tmp_path / "sub" / "deep"
        subdir.mkdir(parents=True)
        os.chdir(subdir)

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file="project_config.yaml"
        )

        config = composer.load_config(search_recursive=True)
        assert config == {"numbers": [4, 5, 6]}

    def test_config_resolution_fallback_error(self, tmp_path):
        """Test error when config file not found with all search options."""
        os.chdir(tmp_path)

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file="nonexistent.yaml"
        )

        with pytest.raises(FileNotFoundError, match="not found in the current working directory"):
            composer.load_config(search_git_root=True, search_recursive=True)
