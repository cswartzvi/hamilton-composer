from pathlib import Path

from click.testing import CliRunner

from hamilton_composer.cli.factory import build_cli
from hamilton_composer.composer import HamiltonComposer


class TestCLIFactory:
    """Test CLI factory functionality and edge cases."""

    def test_cli_with_plugins(self):
        """Test CLI factory with plugins."""
        import click

        @click.command()
        def test_plugin():
            """Test plugin command."""
            pass

        @click.command()
        def another_plugin():
            """Another test plugin command."""
            pass

        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        # Test with multiple plugins to ensure the plugin iteration works
        cli = build_cli("test-project", composer, plugins=[test_plugin, another_plugin])

        runner = CliRunner()
        # Test plugin group help
        result = runner.invoke(cli, ["plugins", "--help"])
        assert result.exit_code == 0
        assert "Registered Hamilton Composer plugin sub-commands" in result.output

        # Test executing a specific plugin to cover the plugin group function
        result = runner.invoke(cli, ["plugins", "test-plugin", "--help"])
        assert result.exit_code == 0

    def test_cli_with_string_logger(self):
        """Test CLI factory with string logger name."""
        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        cli = build_cli("test-project", composer, logger="test.logger")

        # Should build successfully without errors
        assert cli is not None

    def test_cli_with_log_file(self):
        """Test CLI factory with log file configuration."""
        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        cli = build_cli("test-project", composer, log_file=Path("test.log"))

        # Should build successfully without errors
        assert cli is not None

    def test_cli_context_creation(self):
        """Test CLI context object creation."""
        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        cli = build_cli("test-project", composer)

        runner = CliRunner()
        with runner.isolated_filesystem():
            config_path = Path("config.yaml")
            config_path.write_text("test: value")

            result = runner.invoke(cli, ["--config-path", str(config_path), "list"])
            assert result.exit_code == 0
