import os
from dataclasses import dataclass

from hamilton_composer import HamiltonComposer


@dataclass
class SimpleSchema:
    numbers: list[int]


class TestPipelineExecutionWithoutConfig:
    """Test pipeline execution without configuration files."""

    def test_execution_with_actual_pipeline_function(self):
        """Test pipeline execution without any configuration."""
        from ..defs.pipelines import create_pipelines

        composer = HamiltonComposer(create_pipelines)
        pipelines = composer.find_pipelines()
        result = pipelines["simple_pipeline"].execute(inputs={"numbers": [1, 2, 3]})
        assert result == {"sum_doubled": 12}

    def test_execution_with_referential_pipeline_function(self):
        """Test pipeline execution with string reference."""
        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        pipelines = composer.find_pipelines()
        result = pipelines["simple_pipeline"].execute(inputs={"numbers": [2, 4, 6]})
        assert result == {"sum_doubled": 24}

    def test_execution_with_node_branching(self):
        """Test pipeline execution with node branching."""
        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        pipelines = composer.find_pipelines({"method": "multiply"})
        result = pipelines["branched_pipeline"].execute(inputs={"numbers": [2, 4, 6], "factor": 2})
        assert result["transformed_sum"] == 48


class TestPipelineExecutionWithConfig:
    """Test pipeline execution with configuration files."""

    def test_execution_with_config_file(self, tmp_path):
        """Test pipeline execution with YAML config file."""
        os.chdir(tmp_path)
        config_file = tmp_path / "config.yaml"
        config_file.write_text("numbers: [1, 2, 3]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=config_file
        )
        config = composer.load_config()
        pipelines = composer.find_pipelines(config)
        result = pipelines["simple_pipeline"].execute(inputs=config)
        assert result == {"sum_doubled": 12}

    def test_execution_with_runtime_config_override(self, tmp_path):
        """Test execution with runtime config file override."""
        os.chdir(tmp_path)

        config_file1 = tmp_path / "config1.yaml"
        config_file1.write_text("numbers: [1, 2, 3]")

        config_file2 = tmp_path / "config2.yaml"
        config_file2.write_text("numbers: [2, 3, 4]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=config_file1
        )

        # Override config at runtime
        config = composer.load_config(filepath=config_file2)
        pipelines = composer.find_pipelines(config)
        result = pipelines["simple_pipeline"].execute(inputs=config)
        assert result == {"sum_doubled": 18}  # 2*2 + 2*3 + 2*4 = 18

    def test_execution_with_schema_validation(self, tmp_path):
        """Test pipeline execution with schema validation."""
        os.chdir(tmp_path)
        config_file = tmp_path / "config.yaml"
        config_file.write_text("numbers: [2, 4, 6]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=config_file, schema=SimpleSchema
        )
        config = composer.load_config()
        pipelines = composer.find_pipelines(config)
        result = pipelines["simple_pipeline"].execute(inputs=config)
        assert result == {"sum_doubled": 24}

    def test_execution_with_node_branching_config(self, tmp_path):
        """Test pipeline execution with node-specific branching."""
        os.chdir(tmp_path)
        config_file = tmp_path / "config.yaml"
        config_file.write_text("numbers: [1, 2, 3]\nmethod: add\nfactor: 2")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=config_file
        )
        config = composer.load_config()
        pipelines = composer.find_pipelines(config)
        result = pipelines["branched_pipeline"].execute(inputs=config)
        assert result == {"transformed_sum": 14}  # (1*2 + 2*2 + 3*2) + 2 = 14
