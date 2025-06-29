import logging
import logging.config
import time
from pathlib import Path

import pytest

from hamilton_composer.logging import DEFAULT_LOGGERS
from hamilton_composer.logging import PACKAGE_LOGGER_NAME
from hamilton_composer.logging import MillisecondFormatter
from hamilton_composer.logging import configure_logging
from hamilton_composer.logging import get_default_logger


@pytest.fixture
def mock_logging_setup(mocker):
    """Set up common mocks for logging tests."""
    mock_dict_config = mocker.patch("logging.config.dictConfig")
    return mock_dict_config


class TestConfigureLogging:
    """Test suite for the configure_logging function."""

    def test_configure_logging_with_custom_config(self, mocker):
        """Test that custom config is passed directly to dictConfig."""
        mock_dict_config = mocker.patch("logging.config.dictConfig")
        custom_config = {"version": 1, "handlers": {}, "loggers": {}}

        configure_logging(config=custom_config)

        mock_dict_config.assert_called_once_with(custom_config)

    def test_configure_logging_default_behavior(self, mock_logging_setup):
        """Test default logging configuration behavior."""
        mock_dict_config = mock_logging_setup

        configure_logging()

        # Verify dictConfig was called and default loggers are configured
        assert mock_dict_config.called
        config_arg = mock_dict_config.call_args[0][0]

        assert "loggers" in config_arg
        for logger_name in DEFAULT_LOGGERS:
            assert logger_name in config_arg["loggers"]
            assert config_arg["loggers"][logger_name]["level"] == "INFO"
            assert config_arg["loggers"][logger_name]["handlers"] == ["console"]
            assert config_arg["loggers"][logger_name]["propagate"] is False

    def test_configure_logging_with_disabled_default_loggers(self, mock_logging_setup):
        """Test that default loggers are not configured when disabled."""
        mock_dict_config = mock_logging_setup

        configure_logging(include_defaults=False)

        # Verify dictConfig was called and no default loggers are configured
        assert mock_dict_config.called
        config_arg = mock_dict_config.call_args[0][0]

        assert "loggers" in config_arg
        for logger_name in DEFAULT_LOGGERS:
            assert logger_name not in config_arg["loggers"]

    def test_configure_logging_with_debug_mode(self, mock_logging_setup):
        """Test logging configuration with debug mode enabled."""
        mock_dict_config = mock_logging_setup

        configure_logging(debug=True)

        config_arg = mock_dict_config.call_args[0][0]

        # Verify debug level is set
        for logger_name in DEFAULT_LOGGERS:
            assert config_arg["loggers"][logger_name]["level"] == "DEBUG"

    def test_configure_logging_with_log_file(self, mock_logging_setup):
        """Test logging configuration with log file."""
        mock_dict_config = mock_logging_setup

        log_file = Path("/tmp/test.log")
        configure_logging(log_file=log_file)

        config_arg = mock_dict_config.call_args[0][0]

        # Verify log file is configured
        assert "rotating_file" in config_arg["handlers"]
        assert config_arg["handlers"]["rotating_file"]["filename"] == str(log_file.resolve())

        # Verify rotating_file handler is added to loggers
        for logger_name in DEFAULT_LOGGERS:
            assert "rotating_file" in config_arg["loggers"][logger_name]["handlers"]

    def test_configure_logging_without_log_file(self, mock_logging_setup):
        """Test that rotating_file handler is removed when no log file specified."""
        mock_dict_config = mock_logging_setup

        configure_logging()

        config_arg = mock_dict_config.call_args[0][0]

        # Verify rotating_file handler is removed
        assert "rotating_file" not in config_arg["handlers"]

        # Verify only console handler is in loggers
        for logger_name in DEFAULT_LOGGERS:
            assert config_arg["loggers"][logger_name]["handlers"] == ["console"]

    def test_configure_logging_with_custom_loggers(self, mock_logging_setup):
        """Test logging configuration with custom loggers."""
        mock_dict_config = mock_logging_setup

        custom_loggers = ["my.custom.logger", logging.getLogger("another.logger")]
        configure_logging(*custom_loggers)

        config_arg = mock_dict_config.call_args[0][0]

        # Verify custom logger is configured
        for custom_logger in custom_loggers:
            name = custom_logger if isinstance(custom_logger, str) else custom_logger.name
            assert name in config_arg["loggers"]
            assert config_arg["loggers"][name]["level"] == "INFO"
            assert config_arg["loggers"][name]["handlers"] == ["console"]


class TestGetDefaultLogger:
    """Test suite for the get_default_logger function."""

    def test_get_default_logger_returns_logger(self):
        """Test that get_default_logger returns a Logger instance."""
        logger = get_default_logger()

        assert isinstance(logger, logging.Logger)
        assert logger.name == PACKAGE_LOGGER_NAME

    def test_get_default_logger_name_consistency(self):
        """Test that the returned logger has the correct name."""
        logger = get_default_logger()

        assert logger.name == "hamilton_composer"
        assert logger.name == PACKAGE_LOGGER_NAME

    def test_get_default_logger_multiple_calls(self):
        """Test that multiple calls return loggers with the same name."""
        logger1 = get_default_logger()
        logger2 = get_default_logger()

        # Should have same name (though may be different instances)
        assert logger1.name == logger2.name == PACKAGE_LOGGER_NAME


class TestMillisecondFormatter:
    """Test suite for the MillisecondFormatter class."""

    def test_millisecond_formatter_init(self):
        """Test that MillisecondFormatter can be instantiated."""
        formatter = MillisecondFormatter()

        assert isinstance(formatter, logging.Formatter)
        assert formatter.converter == time.localtime

    def test_format_time_with_milliseconds(self):
        """Test that formatTime includes milliseconds."""
        formatter = MillisecondFormatter()

        # Create a mock LogRecord with known timestamp
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        # Set a known created time and msecs
        record.created = 1640995200.123  # 2022-01-01 00:00:00.123 UTC
        record.msecs = 123.456

        formatted_time = formatter.formatTime(record)

        # Should include milliseconds (the key functionality we're testing)
        assert ".123" in formatted_time
        # Should be a valid timestamp format
        assert len(formatted_time) == 23  # YYYY-MM-DD HH:MM:SS.mmm
        assert ":" in formatted_time
        assert "-" in formatted_time

    def test_format_time_millisecond_precision(self):
        """Test that milliseconds are formatted to 3 decimal places."""
        formatter = MillisecondFormatter()

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        test_cases = [
            (1.0, "001"),
            (123.456, "123"),
            (999.999, "999"),
        ]

        for msecs, expected_str in test_cases:
            record.created = 1640995200.0
            record.msecs = msecs

            formatted_time = formatter.formatTime(record)
            assert formatted_time.endswith(expected_str)

    def test_format_complete_log_message(self):
        """Test that the formatter works with complete log formatting."""
        formatter = MillisecondFormatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        record.created = 1640995200.555
        record.msecs = 555.0

        formatted_message = formatter.format(record)

        # Should contain all expected elements
        assert ".555" in formatted_message
        assert "test.logger" in formatted_message
        assert "INFO" in formatted_message
        assert "Test message" in formatted_message
