from dataclasses import dataclass
from pathlib import Path

from hamilton_composer.cli.factory import build_cli
from hamilton_composer.composer import HamiltonComposer


class TestRunPipelineWithNoConfig:
    def test_run_with_no_config(self, runner):
        """Test that a pipeline can be executed without any configuration."""

        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        cli = build_cli("test-project", composer)

        # NOTE: Hydra require the '+' in the parameter list because no config was specified
        result = runner.invoke(cli, ["run", "simple_pipeline", "+numbers=[2,3,4]"])
        assert result.exit_code == 0
        assert "sum_doubled = 18" in result.output


class TestRunPipelineWithConfig:
    def test_run_with_dict_config(self, runner):
        """Test that a pipeline can be executed with dict configuration."""
        directory = Path("config")
        parameters = directory / "parameters.yaml"
        parameters.parent.mkdir(parents=True)
        parameters.write_text("numbers: [1, 2, 3]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines",
            config_directory="config",
            config_name="parameters",
        )
        cli = build_cli("test-project", composer)

        result = runner.invoke(cli, ["run", "simple_pipeline"])
        assert result.exit_code == 0
        assert "sum_doubled = 12" in result.output

    def test_run_with_dict_config_and_override(self, runner):
        """Test that a pipeline can be executed with dict configuration and an override."""
        directory = Path("config")
        parameters = directory / "parameters.yaml"
        parameters.parent.mkdir(parents=True)
        parameters.write_text("numbers: [1, 2, 3]")

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines",
            config_directory="config",
            config_name="parameters",
        )
        cli = build_cli("test-project", composer)

        result = runner.invoke(cli, ["run", "simple_pipeline", "numbers=[2,3,4]"])
        assert result.exit_code == 0
        assert "sum_doubled = 18" in result.output

    def test_run_with_valid_structured_config(self, runner):
        @dataclass
        class Config:
            numbers: list[int]

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines",
            config_directory="config",
            config_name="parameters",
            schema=Config,
            schema_name="test_schema",
        )
        cli = build_cli("test-project", composer)

        directory = Path("config")
        parameters = directory / "parameters.yaml"
        parameters.parent.mkdir(parents=True)
        parameters.write_text("defaults:\n  - test_schema\n  - _self_\nnumbers: [1, 2, 3]")

        result = runner.invoke(cli, ["run", "simple_pipeline"])
        assert result.exit_code == 0
        assert "sum_doubled = 12" in result.output

    def test_run_with_invalid_structured_config(self, runner):
        @dataclass
        class Config:
            not_numbers: list[int]

        composer = HamiltonComposer(
            "tests.defs.pipelines.create_pipelines",
            config_directory="config",
            config_name="parameters",
            schema=Config,
            schema_name="test_schema",
        )
        cli = build_cli("test-project", composer)

        directory = Path("config")
        parameters = directory / "parameters.yaml"
        parameters.parent.mkdir(parents=True)
        parameters.write_text("defaults:\n  - test_schema\n  - _self_\nnumbers: [1, 2, 3]")

        result = runner.invoke(cli, ["run", "simple_pipeline"])
        assert result.exit_code != 0
