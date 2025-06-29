from typing import TYPE_CHECKING
from unittest.mock import Mock
from unittest.mock import patch

from hamilton_composer.exts.ipython import launch_shell

if TYPE_CHECKING:
    from hamilton_composer.pipeline import Pipeline


class TestIPythonIntegration:
    """Test suite for IPython integration functionality."""

    @patch("hamilton_composer.exts.ipython.InteractiveShellEmbed")
    @patch("hamilton_composer.exts.ipython.load_default_config")
    @patch("rich.get_console")
    def test_launch_shell_creates_shell_with_correct_config(
        self, mock_get_console, mock_load_config, mock_shell_class
    ):
        """Test that launch_shell creates an IPython shell with the correct configuration."""
        # Arrange
        mock_config = {"test": "config"}
        mock_pipelines: dict[str, "Pipeline"] = {"pipeline1": Mock(), "pipeline2": Mock()}
        mock_ipython_config = {"ipython": "config"}
        mock_shell_instance = Mock()
        mock_console = Mock()

        mock_load_config.return_value = mock_ipython_config
        mock_shell_class.return_value = mock_shell_instance
        mock_get_console.return_value = mock_console

        # Act
        launch_shell(config=mock_config, pipelines=mock_pipelines)

        # Assert
        mock_load_config.assert_called_once()
        mock_shell_class.assert_called_once_with(config=mock_ipython_config)
        mock_shell_instance.push.assert_called_once_with(
            {"config": mock_config, "pipelines": mock_pipelines}
        )
        mock_shell_instance.show_banner.assert_called_once()
        mock_shell_instance.mainloop.assert_called_once()

    @patch("hamilton_composer.exts.ipython.InteractiveShellEmbed")
    @patch("hamilton_composer.exts.ipython.load_default_config")
    @patch("rich.get_console")
    def test_launch_shell_displays_welcome_message(
        self, mock_get_console, mock_load_config, mock_shell_class
    ):
        """Test that launch_shell displays the correct welcome message."""
        # Arrange
        mock_config = {"test": "config"}
        mock_pipelines: dict[str, "Pipeline"] = {"pipeline1": Mock()}
        mock_console = Mock()
        mock_get_console.return_value = mock_console

        # Act
        launch_shell(config=mock_config, pipelines=mock_pipelines)

        # Assert
        mock_console.print.assert_called_once()
        call_args = mock_console.print.call_args[0][0]
        assert "Hamilton Composer IPython shell" in call_args
        assert "Preloaded variables: 'config' and 'pipelines'" in call_args

    @patch("hamilton_composer.exts.ipython.InteractiveShellEmbed")
    @patch("hamilton_composer.exts.ipython.load_default_config")
    @patch("rich.get_console")
    def test_launch_shell_pushes_correct_namespace(
        self, mock_get_console, mock_load_config, mock_shell_class
    ):
        """Test that the correct variables are pushed to the IPython namespace."""
        # Arrange
        test_config = {
            "database": {"host": "localhost", "port": 5432},
            "features": ["feature1", "feature2"],
        }
        test_pipelines: dict[str, "Pipeline"] = {
            "data_pipeline": Mock(name="data_pipeline"),
            "ml_pipeline": Mock(name="ml_pipeline"),
        }
        mock_shell_instance = Mock()
        mock_shell_class.return_value = mock_shell_instance

        # Act
        launch_shell(config=test_config, pipelines=test_pipelines)

        # Assert
        mock_shell_instance.push.assert_called_once()
        pushed_namespace = mock_shell_instance.push.call_args[0][0]

        # Verify the namespace contains the expected variables
        assert "config" in pushed_namespace
        assert "pipelines" in pushed_namespace
        assert pushed_namespace["config"] is test_config
        assert pushed_namespace["pipelines"] is test_pipelines
