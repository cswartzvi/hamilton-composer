import pytest
import yaml

from hamilton_composer.pipeline import Pipeline


def A() -> int:
    return 1


def B() -> int:
    return 2


def C() -> int:
    return 3


def D(A: int, B: int, C: int, factor: int) -> int:
    return (A + B + C) * factor


class TestPipeline:
    def test_validation_with_valid_pipeline(self) -> None:
        """Test validation of pipeline inputs and outputs."""

        from hamilton.ad_hoc_utils import create_temporary_module
        from hamilton.driver import Builder

        module = create_temporary_module(A, B, C, D)
        builder = Builder().with_modules(module)

        pipeline = Pipeline(builder, final_vars=["D"], public=True)
        pipeline.validate_execution(inputs={"factor": 2})

    def test_validation_with_invalid_pipeline(self) -> None:
        """Test validation of pipeline inputs and outputs."""

        from hamilton.ad_hoc_utils import create_temporary_module
        from hamilton.driver import Builder

        module = create_temporary_module(A, B, C, D)
        builder = Builder().with_modules(module)

        pipeline = Pipeline(builder, final_vars=["D"], public=True)
        with pytest.raises(ValueError, match="Required input .* not provided"):
            pipeline.validate_execution()

    def test_export(self) -> None:
        """Test exporting a pipeline."""

        from hamilton.ad_hoc_utils import create_temporary_module
        from hamilton.driver import Builder

        module = create_temporary_module(A, B, C, D)
        builder = Builder().with_modules(module)
        pipeline = Pipeline(builder, final_vars=["D"], public=True)

        exported_pipeline = pipeline.export_execution(inputs={"factor": 2})
        assert exported_pipeline is not None, "Exported pipeline should not be None."

        content = yaml.safe_load(exported_pipeline)
        nodes = {node["name"] for node in content["nodes"]}
        assert nodes == {"A", "B", "C", "D", "factor"}, (
            "Exported pipeline nodes do not match expected nodes."
        )

    def test_visualization(self) -> None:
        """Test visualization of a pipeline."""

        from graphviz.graphs import Digraph
        from hamilton.ad_hoc_utils import create_temporary_module
        from hamilton.driver import Builder

        module = create_temporary_module(A, B, C, D)
        builder = Builder().with_modules(module)
        pipeline = Pipeline(builder, final_vars=["D"], public=True)

        visualization = pipeline.visualize_execution(inputs={"factor": 2})
        assert isinstance(visualization, Digraph), "Visualization should return a Digraph object."

    def test_invalid_builder(self) -> None:
        """Test handling of invalid builder in pipeline."""

        with pytest.raises(TypeError, match="to be a Hamilton Builder instance"):
            Pipeline(builder=None, final_vars=["D"], public=True)  # pyright: ignore[reportArgumentType]

    def test_invalid_final_vars(self) -> None:
        """Test handling of invalid final_vars in pipeline."""

        from hamilton.ad_hoc_utils import create_temporary_module
        from hamilton.driver import Builder

        module = create_temporary_module(A, B, C, D)
        builder = Builder().with_modules(module)

        with pytest.raises(TypeError, match="to be a list instance."):
            Pipeline(builder, final_vars=None, public=True)  # type: ignore
