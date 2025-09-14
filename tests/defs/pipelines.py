from dataclasses import is_dataclass
from typing import TYPE_CHECKING, Any

from hamilton.driver import Builder

from hamilton_composer import Pipeline

if TYPE_CHECKING:
    from _typeshed import DataclassInstance
else:
    DataclassInstance = object


def create_pipelines(
    config: dict[str, Any] | DataclassInstance | None = None,
) -> dict[str, Pipeline]:
    """Create and return the pipelines."""
    from . import functions

    pipelines = {}
    builder = Builder().with_modules(functions)

    # NOTE: This is NOT the recommended way to do this in user code, you would normally just
    # use either the dictionary of the dataclass. However, we are trying to test both in this suite.
    method = None
    if is_dataclass(config):
        method = getattr(config, "method", None)
    elif config:
        method = config.get("method", None)
    if method:
        config = {"method": method}
        builder = builder.with_config(config)

    pipelines["simple_pipeline"] = Pipeline(builder, final_vars=["sum_doubled"])
    pipelines["branched_pipeline"] = Pipeline(builder=builder, final_vars=["transformed_sum"])
    return pipelines
