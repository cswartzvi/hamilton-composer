# NOTE: Top-level imports for files in this sub-package should be kept to a minimum (limited to the
#       standard library or the click package). This is done to reduce the initial startup time of
#       the CLI application. Use function-level imports for any additional modules that are
#       required at startup.

from hamilton_composer.cli.factory import build_cli

__all__ = ["build_cli"]
