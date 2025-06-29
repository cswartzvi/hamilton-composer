import pytest

from hamilton_composer.cli.factory import build_cli
from hamilton_composer.composer import HamiltonComposer


@pytest.fixture(scope="module")
def composer():
    """Fixture to provide a HamiltonComposer instance."""
    return HamiltonComposer("tests.defs.pipelines.create_pipelines")


@pytest.fixture(scope="module")
def cli(composer):
    """Fixture to provide a HamiltonComposer CLI instance."""
    return build_cli("testing-project", composer)


class TestListCommand:
    """Test the 'list' command of the CLI."""

    def test_list_command_help(self, runner, cli):
        """Test the help message for the 'list' command."""
        result = runner.invoke(cli, ["list", "--help"])
        assert result.exit_code == 0
        assert "List available pipelines" in result.output

    def test_list_command_no_pipelines(self, runner):
        """Test the 'list' command when no pipelines are available."""
        from hamilton_composer import HamiltonComposer
        from hamilton_composer import build_cli

        composer = HamiltonComposer(lambda cfg: {})
        cli = build_cli("empty-composer", composer)
        result = runner.invoke(cli, ["list"])
        assert result.exit_code == 0
        assert "No pipelines available." in result.output

    def test_list_command_with_pipelines(self, runner, cli):
        """Test the 'list' command when pipelines are available."""
        result = runner.invoke(cli, ["list"])
        assert result.exit_code == 0
        assert "simple_pipeline" in result.output
        assert "branched_pipeline" in result.output
