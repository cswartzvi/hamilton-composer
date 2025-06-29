import pytest
from click.testing import CliRunner


@pytest.fixture(scope="function")
def runner():
    """Fixture to provide a CLI runner."""
    _runner = CliRunner()
    with _runner.isolated_filesystem():
        yield _runner
