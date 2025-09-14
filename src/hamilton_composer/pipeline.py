from typing import TYPE_CHECKING, Any, Callable, Iterable

if TYPE_CHECKING:
    from hamilton.driver import Builder
    from hamilton.driver import Driver
    from hamilton.graph_types import HamiltonNode
    from hamilton.lifecycle import LifecycleAdapter
else:
    Builder = object
    Driver = object
    HamiltonNode = object
    LifecycleAdapter = object


class Pipeline:
    """
    Predefined execution pipeline for Hamilton Directed Acyclic Graphs (DAGs).

    This class encapsulates a `hamilton.driver.Driver` with final variables, making it easy to
    execute the configured DAG multiple times, with different inputs, in a consistent manner.


    Args:
        builder (hamilton.driver.Builder):
            Hamilton builder that will be executed to create the driver. Note that this is done
            lazily to avoid costly imports and execution.

            For more information on the Hamilton `Builder` class, see the following
            - https://hamilton.dagworks.io/en/latest/concepts/builder/
            - https://hamilton.dagworks.io/en/latest/reference/drivers/Driver/#hamilton.driver.Builder
        final_vars (list[str | Callable | hamilton.graph_types.HamiltonNode]):
            Final variables that the driver will compute. These can be strings representing node
            names, callable functions, or HamiltonNode instances.
        config (dict[str, Any], optional):
            Configuration data used to build branching DAGs (via the `@config` decorator).
        public (bool, optional):
            If True (default), the pipeline is considered public and can be accessed by the CLI.
            Pipelines that are not public are considered internal and are not exposed to the CLI
            interface, but can still be used programmatically.

    Example:
        >>> from hamilton import driver
        >>> from hamilton.builder import Builder
        >>>
        >>> # Create a Hamilton driver
        >>> builder = Builder().with_modules(my_module)
        >>>
        >>> # Create a public pipeline
        >>> pipeline = Pipeline(builder, ["output_var1", "output_var2"], public=True)
        >>>
        >>> # Execute the pipeline
        >>> results = pipeline.execute(inputs={"input_param": 42})
    """

    def __init__(
        self,
        builder: Builder,
        final_vars: list[str | Callable | HamiltonNode],
        *,
        description: str | None = None,
        tags: Iterable[str] | None = None,
        public: bool = True,
    ) -> None:
        if not builder or not isinstance(builder, Builder):
            name = type(builder).__name__
            raise TypeError(f"Expected 'builder' to be a Hamilton Builder instance, got {name}.")
        self._builder = builder
        if not isinstance(final_vars, list):
            raise TypeError(f"Expected 'final_vars' to be a list instance, got {final_vars}.")
        self._final_vars = final_vars
        self.description = description
        self.tags = tags if tags is not None else []
        self._public = public

    @property
    def public(self) -> bool:
        """Returns whether this pipeline is considered public."""
        return self._public

    def _build_driver(self, adapters: Iterable[LifecycleAdapter] | None = None) -> Driver:
        """Builds the driver with optionally configuration and adapters."""
        intermediate = self._builder.with_adapters(*adapters) if adapters else self._builder
        return intermediate.build()

    def _process_inputs(
        self, driver: Driver, inputs: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        """Processes inputs key-value pairs that were previously passed to driver configuration."""
        if not inputs:
            return None
        return {key: value for key, value in inputs.items() if key not in driver.config}

    def execute(
        self,
        inputs: dict[str, Any] | None = None,
        overrides: dict[str, Any] | None = None,
        adapters: Iterable[LifecycleAdapter] | None = None,
    ) -> Any:
        """
        Executes the Hamilton  with the provided parameters.

        Args:
            inputs (dict[str, Any], optional):
                Inputs for the pipeline. Required if the pipeline has inputs.
            overrides (dict[str, Any], optional):
                Node overrides for the pipeline - will replace nodes with precomputed values.
            adapters (Iterable[LifecycleAdapter], optional):
                Additional lifecycle adapters to apply to the execution. These can be used to
                customize the execution behavior, such as logging or monitoring.
        """
        driver = self._build_driver(adapters)
        inputs = self._process_inputs(driver, inputs)
        result = driver.execute(
            final_vars=self._final_vars,
            overrides=overrides,  # pyright: ignore[reportArgumentType]
            inputs=inputs,  # pyright: ignore[reportArgumentType]
        )
        return result

    def export_execution(
        self,
        inputs: dict[str, Any] | None = None,
        overrides: dict[str, Any] | None = None,
        adapters: Iterable[LifecycleAdapter] | None = None,
    ) -> str:
        """
        Exports the execution of the Hamilton driver with the provided parameters to json.

        Args:
            inputs (dict[str, Any], optional):
                Inputs for the pipeline. Required if the pipeline has inputs.
            overrides (dict[str, Any], optional):
                Node overrides for the pipeline - will replace nodes with precomputed values.
            adapters (Iterable[LifecycleAdapter], optional):
                Additional lifecycle adapters to apply to the execution. These can be used to
                customize the execution behavior, such as logging or monitoring.

        Returns:
            str: The exported execution result as a string.
        """
        driver = self._build_driver(adapters)
        inputs = self._process_inputs(driver, inputs)
        return driver.export_execution(
            final_vars=self._final_vars,
            inputs=inputs,
            overrides=overrides,
        )

    def validate_execution(
        self,
        inputs: dict[str, Any] | None = None,
        overrides: dict[str, Any] | None = None,
        adapters: Iterable[LifecycleAdapter] | None = None,
    ) -> None:
        """
        Validates the execution of the Hamilton driver with the provided parameters.

        Args:
            inputs (dict[str, Any], optional):
                Inputs for the pipeline. Required if the pipeline has inputs.
            overrides (dict[str, Any], optional):
                Node overrides for the pipeline - will replace nodes with precomputed values.
            adapters (Iterable[LifecycleAdapter], optional):
                Additional lifecycle adapters to apply to the execution. These can be used to
                customize the execution behavior, such as logging or monitoring.
        """
        driver = self._build_driver(adapters)
        inputs = self._process_inputs(driver, inputs)
        driver.validate_execution(
            final_vars=self._final_vars,
            overrides=overrides,  # pyright: ignore[reportArgumentType]
            inputs=inputs,  # pyright: ignore[reportArgumentType]
        )

    def visualize_execution(
        self,
        output_file_path: str | None = None,
        render_kwargs: dict | None = None,
        inputs: dict[str, Any] | None = None,
        graphviz_kwargs: dict | None = None,
        overrides: dict[str, Any] | None = None,
        show_legend: bool = True,
        orient: str = "LR",
        hide_inputs: bool = False,
        deduplicate_inputs: bool = False,
        show_schema: bool = True,
        custom_style_function: Callable | None = None,
        bypass_validation: bool = False,
        keep_dot: bool = False,
    ) -> Any:
        """
        Visualizes the execution of the Hamilton DAG as a graph using Graphviz.

        See the `hamilton.driver.Driver.visualize_execution` method for details on the parameters:
        - https://hamilton.dagworks.io/en/latest/reference/drivers/Driver/
        """
        driver = self._build_driver()
        inputs = self._process_inputs(driver, inputs)
        return driver.visualize_execution(
            final_vars=self._final_vars,
            output_file_path=output_file_path,
            render_kwargs=render_kwargs,
            inputs=inputs,
            graphviz_kwargs=graphviz_kwargs,
            overrides=overrides,
            show_legend=show_legend,
            orient=orient,
            hide_inputs=hide_inputs,
            deduplicate_inputs=deduplicate_inputs,
            show_schema=show_schema,
            custom_style_function=custom_style_function,
            bypass_validation=bypass_validation,
            keep_dot=keep_dot,
        )
