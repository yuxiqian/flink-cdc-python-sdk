"""
Pipeline API for Flink CDC Python SDK.

Provides a fluent API for building Flink CDC pipelines.
Pipeline is a pure YAML generator, completely decoupled from environment.
Use Env.execute(pipeline) to run the pipeline.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

from .compose.definition import (
    ModelDef,
    PipelineDef,
    RouteDef,
    SinkDef,
    SourceDef,
    TransformDef,
    UdfDef,
)
from .compose.yaml_serializer import save_yaml, to_yaml

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

_OptionTypes = str | int | float | bool


class Pipeline:
    """
    Fluent API for building Flink CDC pipelines.

    Pipeline is a pure YAML generator. It does not depend on any environment
    configuration. Use Env.execute(pipeline) to run the pipeline.

    Example:
        ```python
        # Create pipeline (pure data structure)
        pipeline = (
            Pipeline.create("MySQL to Doris", parallelism=2)
            .from_source("mysql", hostname="localhost", tables="app_db\\\\..*")
            .to_sink("doris", fenodes="127.0.0.1:8030")
        )

        # Create environment and execute
        env = Env.create(
            flink_cdc_version="3.3.0",
            flink_home="/path/to/flink",
            jars=["/path/to/mysql-connector.jar"],
        )
        env.setup()
        env.add_connector("flink-cdc-pipeline-connector-mysql")
        env.execute(pipeline)
        ```
    """

    _pipeline_config: dict[str, Any]
    _source: SourceDef | None
    _sink: SinkDef | None
    _routes: list[RouteDef]
    _transforms: list[TransformDef]
    _udfs: list[UdfDef]
    _models: list[ModelDef]

    def __init__(self) -> None:
        """Initialize an empty pipeline."""
        self._pipeline_config = {}
        self._source = None
        self._sink = None
        self._routes = []
        self._transforms = []
        self._udfs = []
        self._models = []

    @staticmethod
    def create(
        pipeline_name: str,
        **pipeline_options: _OptionTypes,
    ) -> _PipelineStart:
        """
        Create a new pipeline with the given name.

        Args:
            pipeline_name: Name of the pipeline (will be the Flink job name)
            **pipeline_options: Pipeline configuration options

        Returns:
            _PipelineStart for chaining source definition

        Example:
            ```python
            pipeline = Pipeline.create("My Pipeline", parallelism=2)
            ```
        """
        pipeline = _PipelineStart()
        pipeline._pipeline_config = {'name': pipeline_name, **pipeline_options}
        return pipeline

    def with_java_udf(self, udf_name: str, classpath: str, **options: str) -> Self:
        """
        Register a Java UDF.

        Args:
            udf_name: Name of the UDF function
            classpath: Fully-qualified class path
            **options: Additional UDF options

        Returns:
            Self for chaining
        """
        self._udfs.append(UdfDef(name=udf_name, classpath=classpath, options=options))
        return self

    def with_python_udf(self, udf_name: str, func: Callable[..., Any]) -> Self:
        """
        Register a Python UDF.

        Note: Python UDF support requires additional setup and is not
        currently supported in this version.

        Args:
            udf_name: Name of the UDF function
            func: Python function

        Returns:
            Self for chaining
        """
        _ = func
        logger.warning(
            "Python UDF '%s' registered but Python UDF support is not yet implemented. "
            'Consider using Java UDFs for now.',
            udf_name,
        )
        return self

    def with_ai_model(
        self,
        model_name: str,
        class_name: str,
        **parameters: str,
    ) -> Self:
        """
        Register an AI model.

        Args:
            model_name: Name for the model function
            class_name: Model class name (e.g., "OpenAIChatModel")
            **parameters: Model configuration parameters

        Returns:
            Self for chaining
        """
        self._models.append(ModelDef(model_name=model_name, class_name=class_name, parameters=parameters))
        return self

    def transform(
        self,
        source_table: str,
        *,
        projection: str | None = None,
        filtering: str | None = None,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        table_options: dict[str, str] | None = None,
        description: str | None = None,
    ) -> Self:
        """
        Add a transformation rule.

        Args:
            source_table: Regex pattern for matching source tables
            projection: Column projection expression
            filtering: Filter expression (WHERE clause)
            primary_keys: List of primary key columns
            partition_keys: List of partition key columns
            table_options: Table options as key-value pairs
            description: Description of the transformation

        Returns:
            Self for chaining
        """
        self._transforms.append(
            TransformDef(
                source_table=source_table,
                projection=projection,
                filtering=filtering,
                primary_keys=primary_keys,
                partition_keys=partition_keys,
                table_options=table_options,
                description=description,
            )
        )
        return self

    def route(
        self,
        source_table: str,
        sink_table: str,
        description: str | None = None,
    ) -> Self:
        """
        Add a routing rule.

        Args:
            source_table: Source table name or regex pattern
            sink_table: Target sink table name
            description: Optional description

        Returns:
            Self for chaining
        """
        self._routes.append(RouteDef(source_table=source_table, sink_table=sink_table, description=description))
        return self

    def to_yaml(self) -> str:
        """
        Generate YAML representation of the pipeline.

        Returns:
            YAML string
        """
        return to_yaml(self._build_definition())

    def save_yaml(self, file_path: str | Path) -> Path:
        """
        Save the pipeline YAML to a file.

        Args:
            file_path: Path to save the YAML file

        Returns:
            Path to the saved file
        """
        result = save_yaml(self._build_definition(), file_path)
        return Path(result)

    def to_dict(self) -> dict[str, Any]:
        """
        Get a dictionary representation of the pipeline.

        Returns:
            Dictionary copy of the pipeline definition
        """
        return self._build_definition().to_dict()

    def _build_definition(self) -> PipelineDef:
        """Build the pipeline definition."""
        if not self._source:
            raise ValueError('Pipeline must have a source. Call from_source() first.')
        if not self._sink:
            raise ValueError('Pipeline must have a sink. Call to_sink() first.')

        return PipelineDef(
            source=self._source,
            sink=self._sink,
            config=self._pipeline_config,
            routes=self._routes,
            transforms=self._transforms,
            udfs=self._udfs,
            models=self._models,
        )


class _PipelineStart(Pipeline):
    """Pipeline builder for defining the source."""

    def from_values(self, data: list[tuple[Any, ...]]) -> _PipelineIntermediate:
        """
        Create a source from Python values.

        Note: This is a placeholder for future implementation.
        """
        _ = data
        raise NotImplementedError('from_values() is not yet implemented. Use from_source() with a supported connector.')

    def from_csv(self, file: Path | str) -> _PipelineIntermediate:
        """
        Create a source from a CSV file.

        Note: This is a placeholder for future implementation.
        """
        _ = file
        raise NotImplementedError('from_csv() is not yet implemented. Use from_source() with a supported connector.')

    def from_source(self, name: str, **source_options: _OptionTypes) -> _PipelineIntermediate:
        """
        Define the data source.

        Args:
            name: Connector type (e.g., "mysql", "postgres", "mongodb")
            **source_options: Source connector configuration options

        Returns:
            _PipelineIntermediate for chaining transformations and sink

        Example:
            ```python
            pipeline.from_source(
                "mysql",
                hostname="localhost",
                port=3306,
                username="root",
                password="password",
                tables="app_db\\\\..*",
            )
            ```
        """
        self._source = SourceDef(type=name, config=source_options)
        return _PipelineIntermediate._from(self)


class _PipelineIntermediate(Pipeline):
    """Pipeline builder after source is defined."""

    @classmethod
    def _from(cls, other: Pipeline) -> _PipelineIntermediate:
        """Create from another pipeline instance."""
        pipeline = cls()
        pipeline._pipeline_config = other._pipeline_config
        pipeline._source = other._source
        pipeline._sink = other._sink
        pipeline._routes = other._routes
        pipeline._transforms = other._transforms
        pipeline._udfs = other._udfs
        pipeline._models = other._models
        return pipeline

    def to_sink(self, name: str, **sink_options: _OptionTypes) -> Self:
        """
        Define the data sink.

        Args:
            name: Connector type (e.g., "doris", "starrocks", "kafka")
            **sink_options: Sink connector configuration options

        Returns:
            Self for chaining

        Example:
            ```python
            pipeline.to_sink(
                "doris",
                fenodes="127.0.0.1:8030",
                username="root",
                password="",
            )
            ```
        """
        self._sink = SinkDef(type=name, config=sink_options)
        return self
