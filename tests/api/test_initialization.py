import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import hydra.errors
import pytest

from hamilton_composer.composer import HamiltonComposer


class TestComposerInit:
    def test_valid_initialization_from_string(self):
        """Test that the HamiltonComposer module can be initialized."""

        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        assert composer.config_directory is None
        assert composer.config_name is None

        config = composer.load_config()
        assert config == {}

        pipelines = composer.find_pipelines(config)
        assert set(pipelines) == {"simple_pipeline", "branched_pipeline"}

    def test_valid_initialization_from_module(self):
        """Test that the HamiltonComposer module can be initialized."""
        from .. import defs

        composer = HamiltonComposer(defs.pipelines.create_pipelines)
        assert composer.config_directory is None
        assert composer.config_name is None

        config = composer.load_config()
        assert config == {}

        pipelines = composer.find_pipelines(config)
        assert set(pipelines) == {"simple_pipeline", "branched_pipeline"}

    def test_invalid_initialization_from_string(self):
        """Test that an error is raised when an invalid string is passed."""
        composer = HamiltonComposer("invalid.module.path")
        with pytest.raises(ModuleNotFoundError):
            composer.find_pipelines()

    def test_invalid_initialization_config_arguments(self):
        """Test that an error is raised when an invalid config directory is passed."""
        with pytest.raises(ValueError, match="Either both"):
            HamiltonComposer(
                "tests.defs.pipelines.create_pipelines",
                config_directory="invalid/directory/path",
            )


class TestComposerLoadDictConfig:
    def test_valid_load_config_manual_config(self):
        """Test that HamiltonComposer can load a config from a manual string."""
        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        config = composer.load_config(params=["+key=value"])
        assert config == {"key": "value"}

    def test_valid_load_config_absolute_path(self, tmp_path):
        """Test that HamiltonComposer can load a config from an absolute file path."""
        from hamilton_composer.composer import HamiltonComposer

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        config_file = config_dir / "config.yaml"
        config_file.write_text("key: value")

        composer = HamiltonComposer(
            lambda cfg: {},
            config_directory=config_dir.resolve(),
            config_name=config_file.name,
        )

        config = composer.load_config()
        assert config == {"key": "value"}

    def test_valid_load_config_local_path(self, tmp_path):
        """Test that HamiltonComposer can load a config from a local file path."""
        from hamilton_composer.composer import HamiltonComposer

        os.chdir(tmp_path)
        config_dir = Path("config")
        config_dir.mkdir()

        config_file = config_dir / "config.yaml"
        config_file.write_text("key: value")

        composer = HamiltonComposer(
            lambda cfg: {},
            config_directory=config_dir,
            config_name=config_file.name,
        )

        config = composer.load_config()
        assert config == {"key": "value"}

    def test_valid_load_config_git_root_search(self, tmp_path):
        """Test that HamiltonComposer can load a config from a file in the git root."""
        from hamilton_composer.composer import HamiltonComposer

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        config_file = config_dir / "config.yaml"
        config_file.write_text("key: value")

        subdir = tmp_path / "subdir"
        subdir.mkdir()

        os.chdir(tmp_path)
        subprocess.run(["git", "init"], check=True)
        os.chdir(subdir)

        composer = HamiltonComposer(
            lambda cfg: {},
            config_directory="config",
            config_name="config.yaml",
        )

        config = composer.load_config(search_git_root=True)
        assert config == {"key": "value"}

    def test_valid_load_config_recursive_search(self, tmp_path):
        """Test that HamiltonComposer can load a config from a file in a parent directory."""
        from hamilton_composer.composer import HamiltonComposer

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        config_file = config_dir / "config.yaml"
        config_file.write_text("key: value")

        subdir = tmp_path / "subdir"
        subdir.mkdir()

        os.chdir(subdir)

        composer = HamiltonComposer(
            lambda cfg: {},
            config_directory="config",
            config_name="config.yaml",
        )

        config = composer.load_config(search_recursive=True)
        assert config == {"key": "value"}

    @pytest.mark.parametrize(
        "search_git_root, search_recursive",
        [(True, False), (False, True), (True, True), (False, False)],
    )
    def test_invalid_load_config_nonexistent_local_file(self, search_git_root, search_recursive):
        """Test that an error is raised when trying to load a config from a non-existent file."""
        composer = HamiltonComposer(
            lambda cfg: {},
            config_directory="nonexistent/directory",
            config_name="config.yaml",
        )
        with pytest.raises(FileNotFoundError):
            composer.load_config(search_git_root=search_git_root, search_recursive=search_recursive)

    @pytest.mark.parametrize(
        "search_git_root, search_recursive",
        [(True, False), (False, True), (True, True), (False, False)],
    )
    def test_invalid_load_config_nonexistent_absolute_file(
        self, tmp_path, search_git_root, search_recursive
    ):
        """Test that an error is raised when trying to load a config from a non-existent file."""
        from hamilton_composer.composer import HamiltonComposer

        composer = HamiltonComposer(
            lambda cfg: {},
            config_directory=tmp_path.joinpath("nonexistent", "directory").resolve(),
            config_name="config.yaml",
        )
        with pytest.raises(FileNotFoundError):
            composer.load_config(search_git_root=search_git_root, search_recursive=search_recursive)

    def test_invalid_load_config_in_git_repo(self, tmp_path):
        """Test that an error is raised when trying to load a config from a file in a git repo."""
        from hamilton_composer.composer import HamiltonComposer

        os.chdir(tmp_path)
        subprocess.run(["git", "init"], check=True)

        composer = HamiltonComposer(
            lambda cfg: {},
            config_directory="config",
            config_name="config.yaml",
        )

        with pytest.raises(FileNotFoundError):
            composer.load_config(search_git_root=True)


class TestComposerLoadStructuredConfig:
    @pytest.fixture(autouse=True)
    def _setup(self, hydra_restore_singletons):
        """Fixture to restore Hydra singletons before each test."""
        # This fixture ensures that the Hydra singletons are restored
        # to their initial state before each test runs.
        pass

    @pytest.fixture
    def config_dir(self, tmp_path):
        """Fixture to create a temporary config directory."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        return config_dir

    @pytest.fixture
    def config_file(self, config_dir):
        """Fixture to create a temporary config file."""
        config_file = config_dir / "config.yaml"
        return config_file

    def test_invalid_schema_name(self):
        """Test that an error is raised when schema_name is provided without config_name."""

        @dataclass
        class ConfigSchema:
            key: str

        with pytest.raises(ValueError):
            HamiltonComposer(lambda cfg: {}, schema=ConfigSchema)

    def test_valid_load_schema_with_inferred_name(self, config_dir, config_file):
        """Test that HamiltonComposer can load a config from an absolute file path with schema."""

        @dataclass
        class ConfigSchema:
            key: str

        config_file.write_text("defaults:\n  - config_schema\n  - _self_\nkey: value\n")

        composer = HamiltonComposer(
            lambda cfg: {},
            config_directory=config_dir.resolve(),
            config_name=config_file.name,
            schema=ConfigSchema,
        )

        config = composer.load_config()
        assert config == {"key": "value"}

    def test_valid_load_schema_with_specified_name(self, config_dir, config_file):
        """Test that HamiltonComposer can load a config from an absolute file path with schema."""

        @dataclass
        class ConfigSchema:
            key: str

        config_file.write_text("defaults:\n  - custom_schema\n  - _self_\nkey: value\n")

        composer = HamiltonComposer(
            lambda cfg: {},
            config_directory=config_dir.resolve(),
            config_name=config_file.name,
            schema=ConfigSchema,
            schema_name="custom_schema",
        )

        config = composer.load_config()
        assert config == {"key": "value"}

    def test_invalid_load_schema_config_from_disk(self, config_dir, config_file):
        """Test that HamiltonComposer can load a config from an absolute file path with schema."""

        @dataclass
        class ConfigSchema:
            foo: str

        config_file.write_text("defaults:\n  - config_schema\n  - _self_\nkey: value\n")

        composer = HamiltonComposer(
            lambda cfg: {},
            config_directory=config_dir.resolve(),
            config_name=config_file.name,
            schema=ConfigSchema,
        )

        with pytest.raises(hydra.errors.ConfigCompositionException):
            composer.load_config()

    def test_unused_schema_config_from_disk(self, config_dir, config_file):
        """Test that HamiltonComposer can load a config from an absolute file path with schema."""

        @dataclass
        class ConfigSchema:
            foo: str

        config_file.write_text("key: value")  # No reference to the schema in the config file

        composer = HamiltonComposer(
            lambda cfg: {},
            config_directory=config_dir.resolve(),
            config_name=config_file.name,
            schema=ConfigSchema,
        )

        composer.load_config()  # Does not raise an error, schema was referenced in config file
