from typing import Any

from hamilton.driver import Builder

from hamilton_composer import Pipeline


def create_pipelines(config: dict[str, Any] | None = None) -> dict[str, Pipeline]:
    """Create and return the pipelines."""
    from . import functions

    pipelines = {}
    builder = Builder().with_modules(functions)

    if config and "method" in config:
        config = {"method": config["method"]}
        builder = builder.with_config(config)

    pipelines["simple_pipeline"] = Pipeline(builder, final_vars=["sum_doubled"])
    pipelines["branched_pipeline"] = Pipeline(builder=builder, final_vars=["transformed_sum"])
    return pipelines
