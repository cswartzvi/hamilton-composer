import os
from dataclasses import dataclass

import hydra.errors
import pytest

from hamilton_composer import HamiltonComposer


class TestPipelineExecutionWithoutConfig:
    def test_execution_with_actual_pipeline_function(self):
        """Test that a pipeline can be executed without any configuration."""
        from ..defs.pipelines import create_pipelines

        composer = HamiltonComposer(create_pipelines)
        pipelines = composer.find_pipelines()
        result = pipelines["simple_pipeline"].execute(inputs={"numbers": [1, 2, 3]})
        assert result == {"sum_doubled": 12}

    def tests_execution_with_referential_pipeline_function(self):
        """Test that a pipeline can be executed with a configuration."""
        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        pipelines = composer.find_pipelines()
        result = pipelines["simple_pipeline"].execute(inputs={"numbers": [2, 4, 6]})
        assert result == {"sum_doubled": 24}

    def tests_execution_with_node_branching(self):
        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        pipelines = composer.find_pipelines({"method": "multiply"})
        result = pipelines["branched_pipeline"].execute(inputs={"numbers": [2, 4, 6], "factor": 2})
        assert result["transformed_sum"] == 48


class TestPipelineExecutionWithConfig:
    def test_execution_with_dict_config(self, tmp_path):
        """Test that a pipeline can be executed with a configuration."""

        os.chdir(tmp_path)
        config_directory = tmp_path / "config"
        config_directory.mkdir()

        config_file = config_directory / "config.yaml"
        config_file.write_text("numbers: [1, 2, 3]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines",
            config_directory=config_directory,
            config_name=config_file.with_suffix("").name,
        )
        config = composer.load_config()
        pipelines = composer.find_pipelines(config)
        result = pipelines["simple_pipeline"].execute(inputs=config)
        assert result == {"sum_doubled": 12}

    def test_execution_with_dict_config_specified_at_runtime(self, tmp_path):
        os.chdir(tmp_path)

        config_directory1 = tmp_path / "config"
        config_directory1.mkdir()

        config_file1 = config_directory1 / "config.yaml"
        config_file1.write_text("numbers: [1, 2, 3]")

        config_directory2 = tmp_path / "config2"
        config_directory2.mkdir()

        config_file2 = config_directory2 / "config2.yaml"
        config_file2.write_text("numbers: [2, 3, 4]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines",
            config_directory=config_directory1,
            config_name=config_file1.with_suffix("").name,
        )

        config = composer.load_config(
            directory=config_directory2, name=config_file2.with_suffix("").name
        )
        pipelines = composer.find_pipelines(config)
        result = pipelines["simple_pipeline"].execute(inputs=config)
        assert result == {"sum_doubled": 18}  # 2*2 + 2*3 + 2*4 = 18

    def test_execution_with_valid_structured_config(self, tmp_path):
        """Test that a pipeline can be executed with a structured configuration."""

        @dataclass
        class Config:
            numbers: list[int]

        os.chdir(tmp_path)
        config_directory = tmp_path / "config"
        config_directory.mkdir()

        config_file = config_directory / "config.yaml"
        config_file.write_text("defaults:\n  - my_schema\n  - _self_\nnumbers: [2, 4, 6]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines",
            config_directory=config_directory,
            config_name=config_file.with_suffix("").name,
            schema=Config,
            schema_name="my_schema",
        )
        config = composer.load_config()
        pipelines = composer.find_pipelines(config)
        result = pipelines["simple_pipeline"].execute(inputs=config)
        assert result == {"sum_doubled": 24}

    def test_execution_with_invalid_structured_config(self, tmp_path):
        """Test that a pipeline can be executed with a structured configuration."""

        @dataclass
        class Config:
            not_numbers: list[int]

        os.chdir(tmp_path)
        config_directory = tmp_path / "config"
        config_directory.mkdir()

        config_file = config_directory / "config.yaml"
        config_file.write_text("defaults:\n  - my_schema\n  - _self_\nnumbers: [2, 4, 6]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines",
            config_directory=config_directory,
            config_name=config_file.with_suffix("").name,
            schema=Config,
            schema_name="my_schema",
        )
        with pytest.raises(hydra.errors.ConfigCompositionException):
            composer.load_config()

    def test_execution_with_node_branching(self, tmp_path):
        """Test that a pipeline can be executed with node-specific branching (via @config)."""

        os.chdir(tmp_path)
        config_directory = tmp_path / "config"
        config_directory.mkdir()

        config_file = config_directory / "config.yaml"
        config_file.write_text("numbers: [1, 2, 3]\nmethod: add\nfactor: 2")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines",
            config_directory=config_directory,
            config_name=config_file.with_suffix("").name,
        )
        config = composer.load_config()
        pipelines = composer.find_pipelines(config)
        result = pipelines["branched_pipeline"].execute(inputs=config)
        assert result == {"transformed_sum": 14}  # (1*2 + 2*2 + 3*2) + 2 = 14
