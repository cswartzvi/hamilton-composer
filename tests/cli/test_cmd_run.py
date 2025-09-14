from dataclasses import dataclass
from pathlib import Path

from hamilton_composer.cli.factory import build_cli
from hamilton_composer.composer import HamiltonComposer


class TestRunPipelineWithNoConfig:
    """Test running pipelines without configuration files."""

    def test_run_with_no_config(self, runner):
        """Test pipeline execution without any configuration."""
        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        cli = build_cli("test-project", composer)

        result = runner.invoke(cli, ["run", "simple_pipeline", "numbers=[2,3,4]"])
        assert result.exit_code == 0
        assert "sum_doubled = 18" in result.output


class TestRunPipelineWithConfig:
    """Test running pipelines with configuration files."""

    def test_run_with_config_file(self, runner):
        """Test pipeline execution with YAML configuration file."""
        parameters_file = Path("parameters.yaml")
        parameters_file.write_text("numbers: [1, 2, 3]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=parameters_file
        )
        cli = build_cli("test-project", composer)

        result = runner.invoke(cli, ["run", "simple_pipeline"])
        assert result.exit_code == 0
        assert "sum_doubled = 12" in result.output

    def test_run_with_config_and_override(self, runner):
        """Test pipeline execution with config file and parameter override."""
        parameters_file = Path("parameters.yaml")
        parameters_file.write_text("numbers: [1, 2, 3]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=parameters_file
        )
        cli = build_cli("test-project", composer)

        result = runner.invoke(cli, ["run", "simple_pipeline", "numbers=[2,3,4]"])
        assert result.exit_code == 0
        assert "sum_doubled = 18" in result.output

    def test_run_with_schema_validation(self, runner):
        """Test pipeline execution with schema validation."""

        @dataclass
        class Config:
            numbers: list[int]

        parameters_file = Path("parameters.yaml")
        parameters_file.write_text("numbers: [1, 2, 3]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines", config_file=parameters_file, schema=Config
        )
        cli = build_cli("test-project", composer)

        result = runner.invoke(cli, ["run", "simple_pipeline"])
        assert result.exit_code == 0
        assert "sum_doubled = 12" in result.output


class TestRunPipelineErrors:
    """Test error handling in the run command."""

    def test_run_nonexistent_pipeline(self, runner):
        """Test error when trying to run a pipeline that doesn't exist."""
        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        cli = build_cli("test-project", composer)

        result = runner.invoke(cli, ["run", "nonexistent_pipeline"])
        assert result.exit_code != 0
        assert "Pipeline 'nonexistent_pipeline' not found" in result.output

    def test_run_private_pipeline(self, runner):
        """Test error when trying to run a private pipeline."""
        from hamilton.driver import Builder

        from hamilton_composer.pipeline import Pipeline

        def create_private_pipeline(config):
            import sys

            builder = Builder().with_modules(sys.modules[__name__])
            private_pipeline = Pipeline(builder, ["dummy"], public=False)
            return {"private_pipeline": private_pipeline}

        composer = HamiltonComposer(create_private_pipeline)
        cli = build_cli("test-project", composer)

        result = runner.invoke(cli, ["run", "private_pipeline"])
        assert result.exit_code != 0
        assert "private and cannot be executed" in result.output


# Dummy function needed for the Hamilton pipeline
def dummy():
    """Dummy function for testing private pipeline."""
    return 42
