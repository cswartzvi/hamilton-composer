from hamilton_composer.cli.context import AppContext
from hamilton_composer.composer import HamiltonComposer


class TestAppContext:
    """Test AppContext properties intended for plugins."""

    def test_context_name_property(self):
        """Test accessing the context name property."""
        from hamilton_composer.logging import get_default_logger

        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        logger = get_default_logger()

        context = AppContext(name="test-project", composer=composer, logger=logger)

        assert context.name == "test-project"

    def test_context_logger_property(self):
        """Test accessing the context logger property."""
        from hamilton_composer.logging import get_default_logger

        composer = HamiltonComposer("tests.defs.pipelines.create_pipelines")
        logger = get_default_logger()

        context = AppContext(name="test-project", composer=composer, logger=logger)

        assert context.logger is logger
