"""
Pipeline definition data structures for Flink CDC.

These classes represent the components of a Flink CDC pipeline YAML configuration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SourceDef:
    """
    Definition of a data source.

    Attributes:
        type: Connector type of the source (e.g., "mysql", "postgres")
        config: Configuration options for the source connector
        name: Optional name of the source
    """

    type: str
    config: dict[str, Any] = field(default_factory=dict)
    name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for YAML serialization."""
        result: dict[str, Any] = {'type': self.type}
        if self.name:
            result['name'] = self.name
        result.update(self.config)
        return result


@dataclass
class SinkDef:
    """
    Definition of a data sink.

    Attributes:
        type: Connector type of the sink (e.g., "doris", "kafka")
        config: Configuration options for the sink connector
        name: Optional name of the sink
    """

    type: str
    config: dict[str, Any] = field(default_factory=dict)
    name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for YAML serialization."""
        result: dict[str, Any] = {'type': self.type}
        if self.name:
            result['name'] = self.name
        result.update(self.config)
        return result


@dataclass
class TransformDef:
    """
    Definition of a transformation.

    Attributes:
        source_table: Regex pattern for matching input table IDs
        projection: Projection expression for selecting/transforming columns
        filtering: Filter expression (note: YAML key is "filter")
        primary_keys: Primary key columns (comma-separated)
        partition_keys: Partition key columns (comma-separated)
        table_options: Table options string
        table_options_delimiter: Delimiter for table options (default: ",")
        description: Description of the transformation
        post_transform_converter: Post-transform converter class
    """

    source_table: str
    projection: str | None = None
    filtering: str | None = None
    primary_keys: list[str] | None = None
    partition_keys: list[str] | None = None
    table_options: dict[str, str] | None = None
    table_options_delimiter: str = ','
    description: str | None = None
    post_transform_converter: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for YAML serialization."""
        result: dict[str, Any] = {'source-table': self.source_table}

        if self.projection:
            result['projection'] = self.projection

        if self.filtering:
            result['filter'] = self.filtering  # Note: YAML uses "filter" not "filtering"

        if self.primary_keys:
            result['primary-keys'] = ','.join(self.primary_keys)

        if self.partition_keys:
            result['partition-keys'] = ','.join(self.partition_keys)

        if self.table_options:
            options_str = self.table_options_delimiter.join(f'{k}={v}' for k, v in self.table_options.items())
            result['table-options'] = options_str

        if self.description:
            result['description'] = self.description

        if self.post_transform_converter:
            result['post-transform-converter'] = self.post_transform_converter

        return result


@dataclass
class RouteDef:
    """
    Definition of a route.

    Attributes:
        source_table: Regex pattern for matching input table IDs
        sink_table: Replacement table name for matched tables
        replace_symbol: Symbol for replacement
        description: Description of the route
    """

    source_table: str
    sink_table: str
    replace_symbol: str | None = None
    description: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for YAML serialization."""
        result: dict[str, Any] = {
            'source-table': self.source_table,
            'sink-table': self.sink_table,
        }

        if self.replace_symbol:
            result['replace-symbol'] = self.replace_symbol

        if self.description:
            result['description'] = self.description

        return result


@dataclass
class UdfDef:
    """
    Definition of a user-defined function.

    Attributes:
        name: Function name
        classpath: Fully-qualified class path
        options: Configuration options for the UDF
    """

    name: str
    classpath: str
    options: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for YAML serialization."""
        result: dict[str, Any] = {
            'name': self.name,
            'classpath': self.classpath,
        }

        if self.options:
            result['options'] = self.options

        return result


@dataclass
class ModelDef:
    """
    Definition of an AI model.

    Attributes:
        model_name: Name of the model function
        class_name: Model class name
        parameters: Configuration parameters for the model
    """

    model_name: str
    class_name: str
    parameters: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for YAML serialization."""
        return {
            'name': self.model_name,
            'model': self.class_name,
            'parameters': self.parameters,
        }


# Pipeline configuration option keys
PIPELINE_OPTIONS_MAP: dict[str, str] = {
    'name': 'name',
    'parallelism': 'parallelism',
    'local_time_zone': 'local-time-zone',
    'execution_runtime_mode': 'execution.runtime-mode',
    'route_mode': 'route-mode',
    'schema_change_behavior': 'schema.change.behavior',
    'schema_operator_uid': 'schema.operator.uid',
    'schema_operator_rpc_timeout': 'schema-operator.rpc-timeout',
    'operator_uid_prefix': 'operator.uid.prefix',
}


@dataclass
class PipelineDef:
    """
    Definition of a complete Flink CDC pipeline.

    A pipeline consists of:
    - Source: Required data source
    - Sink: Required data destination
    - Routes: Optional routing rules
    - Transforms: Optional data transformations
    - UDFs: Optional user-defined functions
    - Models: Optional AI models
    - Config: Pipeline configuration options

    Note: JAR dependencies are not part of YAML definition,
    they are passed via --jar argument to flink-cdc.sh
    """

    source: SourceDef
    sink: SinkDef
    config: dict[str, Any] = field(default_factory=dict)
    routes: list[RouteDef] = field(default_factory=list)
    transforms: list[TransformDef] = field(default_factory=list)
    udfs: list[UdfDef] = field(default_factory=list)
    models: list[ModelDef] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for YAML serialization."""
        result: dict[str, Any] = {}

        # Source section
        result['source'] = self.source.to_dict()

        # Sink section
        result['sink'] = self.sink.to_dict()

        # Transform section (optional)
        if self.transforms:
            result['transform'] = [t.to_dict() for t in self.transforms]

        # Route section (optional)
        if self.routes:
            result['route'] = [r.to_dict() for r in self.routes]

        # Pipeline section
        pipeline_section: dict[str, Any] = {}
        for key, value in self.config.items():
            yaml_key = PIPELINE_OPTIONS_MAP.get(key, key)
            pipeline_section[yaml_key] = value

        # Add UDFs to pipeline section
        if self.udfs:
            pipeline_section['user-defined-function'] = [u.to_dict() for u in self.udfs]

        # Add models to pipeline section
        if self.models:
            pipeline_section['model'] = [m.to_dict() for m in self.models]

        result['pipeline'] = pipeline_section

        return result
