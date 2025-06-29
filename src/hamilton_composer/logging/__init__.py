import logging
import logging.config
import time
from importlib.resources import files
from itertools import chain
from pathlib import Path
from typing import Any, Final, override

import yaml

PACKAGE_LOGGER_NAME: Final[str] = "hamilton_composer"
DEFAULT_LOGGERS: Final[tuple[str, ...]] = (PACKAGE_LOGGER_NAME, "hamilton.plugins.h_logging")


class MillisecondFormatter(logging.Formatter):
    """Formatter that adds milliseconds to the log timestamp."""

    @override
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        t = time.strftime("%Y-%m-%d %H:%M:%S", ct)
        s = "%s.%03d" % (t, record.msecs)
        return s


def get_default_logger() -> logging.Logger:
    """Returns the default Hamilton Composer logger."""
    return logging.Logger(PACKAGE_LOGGER_NAME)


def configure_logging(
    *loggers: str | logging.Logger,
    config: dict[str, Any] | None = None,
    log_file: str | Path | None = None,
    include_defaults: bool = True,
    debug: bool = False,
) -> None:
    """
    Configures logging for a Hamilton Composer application.

    Args:
        *loggers (str | logging.Logger):
            The loggers (or logger names) that should be included in both the console and log file.
        config (dict[str, Any] | None):
            Optional configuration dictionary for logging. If provided, it will override the base
            logging settings for Hamilton Composer.
        log_file (str | pathlib.Path, optional):
            Specifies the path or name of the log file where configured logs will be written. If a
            relative path is provided it will be written relative the execution. If not provided,
            logs will not be written to a file.
        include_defaults (bool, optional):
            If True, the default logger in `hamilton_composer.logging.DEFAULT_LOGGERS` will be
            automatically configured before any manually specified loggers via the `loggers`
            parameter. If False, no default logger will be configured - all loggers must be manually
            specified.
            argument.
        debug (bool, optional):
            If True, sets the logger level for all default and manually specified loggers.
    """
    # Manual configuration should be returned as-is
    if config:
        logging.config.dictConfig(config)
        return

    level = "DEBUG" if debug else "INFO"
    default_loggers = DEFAULT_LOGGERS if include_defaults else []
    handlers = ["console"]

    path = files("hamilton_composer.logging").joinpath("default.yaml")
    with path.open("r", encoding="utf-8") as f:
        content = f.read()
    config = yaml.safe_load(content)
    assert isinstance(config, dict), "Logging configuration must be a dictionary"
    assert "handlers" in config, "Internal logging configuration must include 'handlers'"
    assert "loggers" in config, "Internal logging configuration must include 'loggers'"

    # NOTE: We need to determine if the rotating file handler should be included, when not
    # specified by the user it should be removed from the logger configuration.
    log_file = str(log_file.resolve()) if isinstance(log_file, Path) else log_file
    if log_file:
        config["handlers"]["rotating_file"]["filename"] = log_file
        handlers.append("rotating_file")
    else:
        config["handlers"].pop("rotating_file")

    for logger in chain(default_loggers, loggers):
        name = logger.name if isinstance(logger, logging.Logger) else logger
        config["loggers"][name] = {"level": level, "handlers": handlers, "propagate": False}

    logging.config.dictConfig(config)
