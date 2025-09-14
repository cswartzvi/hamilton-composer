from unittest.mock import Mock
from unittest.mock import patch

from click.testing import CliRunner

from hamilton_composer.cli.cmds.shell import launch_ipython_shell


class TestShellCommand:
    """Test suite for the shell CLI command."""

    @patch("hamilton_composer.exts.ipython.launch_shell")
    def test_shell_command_launches_successfully(self, mock_launch_shell):
        """Test that the shell command launches the IPython shell successfully."""
        # Arrange
        mock_context = Mock()
        mock_config = {"test": "config"}
        mock_pipelines = {"pipeline1": Mock(), "pipeline2": Mock()}
        mock_context.load_config.return_value = mock_config
        mock_context.find_pipelines.return_value = mock_pipelines

        runner = CliRunner()

        # Act
        result = runner.invoke(launch_ipython_shell, [], obj=mock_context)

        # Assert
        assert result.exit_code == 0
        mock_context.load_config.assert_called_once_with(())
        mock_context.find_pipelines.assert_called_once_with(mock_config)
        mock_launch_shell.assert_called_once_with(config=mock_config, pipelines=mock_pipelines)

    @patch("hamilton_composer.exts.ipython.launch_shell")
    def test_shell_command_with_params(self, mock_launch_shell):
        """Test that the shell command correctly passes params to config loading."""
        # Arrange
        mock_context = Mock()
        mock_config = {"env": "test", "debug": True}
        mock_pipelines = {"pipeline1": Mock()}
        mock_context.load_config.return_value = mock_config
        mock_context.find_pipelines.return_value = mock_pipelines

        runner = CliRunner()
        params = ["env=production", "debug=false"]

        # Act
        result = runner.invoke(launch_ipython_shell, params, obj=mock_context)

        # Assert
        assert result.exit_code == 0
        mock_context.load_config.assert_called_once_with(("env=production", "debug=false"))
        mock_context.find_pipelines.assert_called_once_with(mock_config)
        mock_launch_shell.assert_called_once_with(config=mock_config, pipelines=mock_pipelines)

    @patch("hamilton_composer.exts.ipython.launch_shell")
    def test_shell_command_with_empty_config_and_pipelines(self, mock_launch_shell):
        """Test that the shell command works with empty config and pipelines."""
        # Arrange
        mock_context = Mock()
        empty_config = {}
        empty_pipelines = {}
        mock_context.load_config.return_value = empty_config
        mock_context.find_pipelines.return_value = empty_pipelines

        runner = CliRunner()

        # Act
        result = runner.invoke(launch_ipython_shell, [], obj=mock_context)

        # Assert
        assert result.exit_code == 0
        mock_context.load_config.assert_called_once_with(())
        mock_context.find_pipelines.assert_called_once_with(empty_config)
        mock_launch_shell.assert_called_once_with(config=empty_config, pipelines=empty_pipelines)

    @patch("hamilton_composer.exts.ipython.launch_shell")
    def test_shell_command_context_flow(self, mock_launch_shell):
        """Test that the shell command correctly flows config through the context."""
        # Arrange
        mock_context = Mock()
        test_config = {
            "database": {"host": "localhost", "port": 5432},
            "pipeline_settings": {"batch_size": 100},
        }
        test_pipelines = {
            "etl_pipeline": Mock(name="etl_pipeline"),
            "ml_pipeline": Mock(name="ml_pipeline"),
        }
        mock_context.load_config.return_value = test_config
        mock_context.find_pipelines.return_value = test_pipelines

        runner = CliRunner()

        # Act
        result = runner.invoke(launch_ipython_shell, [], obj=mock_context)

        # Assert
        assert result.exit_code == 0

        # Verify that config is loaded first
        mock_context.load_config.assert_called_once_with(())

        # Verify that pipelines are found using the loaded config
        mock_context.find_pipelines.assert_called_once_with(test_config)

        # Verify that launch_shell is called with both config and pipelines
        mock_launch_shell.assert_called_once_with(config=test_config, pipelines=test_pipelines)

    @patch("hamilton_composer.exts.ipython.launch_shell")
    def test_shell_command_handles_config_loading_error(self, mock_launch_shell):
        """Test that the shell command properly handles config loading errors."""
        # Arrange
        mock_context = Mock()
        mock_context.load_config.side_effect = Exception("Config loading failed")

        runner = CliRunner()

        # Act
        result = runner.invoke(launch_ipython_shell, [], obj=mock_context)

        # Assert
        assert result.exit_code != 0
        mock_context.load_config.assert_called_once_with(())
        mock_context.find_pipelines.assert_not_called()
        mock_launch_shell.assert_not_called()

    @patch("hamilton_composer.exts.ipython.launch_shell")
    def test_shell_command_handles_pipeline_finding_error(self, mock_launch_shell):
        """Test that the shell command properly handles pipeline finding errors."""
        # Arrange
        mock_context = Mock()
        mock_config = {"test": "config"}
        mock_context.load_config.return_value = mock_config
        mock_context.find_pipelines.side_effect = Exception("Pipeline finding failed")

        runner = CliRunner()

        # Act
        result = runner.invoke(launch_ipython_shell, [], obj=mock_context)

        # Assert
        assert result.exit_code != 0
        mock_context.load_config.assert_called_once_with(())
        mock_context.find_pipelines.assert_called_once_with(mock_config)
        mock_launch_shell.assert_not_called()

    @patch("hamilton_composer.exts.ipython.launch_shell")
    def test_shell_command_handles_shell_launch_error(self, mock_launch_shell):
        """Test that the shell command properly handles shell launch errors."""
        # Arrange
        mock_context = Mock()
        mock_config = {"test": "config"}
        mock_pipelines = {"test": Mock()}
        mock_context.load_config.return_value = mock_config
        mock_context.find_pipelines.return_value = mock_pipelines
        mock_launch_shell.side_effect = Exception("Shell launch failed")

        runner = CliRunner()

        # Act
        result = runner.invoke(launch_ipython_shell, [], obj=mock_context)

        # Assert
        assert result.exit_code != 0
        mock_context.load_config.assert_called_once_with(())
        mock_context.find_pipelines.assert_called_once_with(mock_config)
        mock_launch_shell.assert_called_once_with(config=mock_config, pipelines=mock_pipelines)
