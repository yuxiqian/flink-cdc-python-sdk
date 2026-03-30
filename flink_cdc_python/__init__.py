"""
Flink CDC Python SDK

A Pythonic SDK for developing Apache Flink CDC Pipeline data integration jobs
with an elegant fluent API.
"""

from .compose import (
    ModelDef,
    PipelineDef,
    RouteDef,
    SinkDef,
    SourceDef,
    TransformDef,
    UdfDef,
    save_yaml,
    to_yaml,
)
from .env import Env, FlinkCdcVersion
from .pipeline import Pipeline

__all__ = [
    # Core API
    'Pipeline',
    # Environment
    'Env',
    'FlinkCdcVersion',
    # Compose (for advanced usage)
    'PipelineDef',
    'SourceDef',
    'SinkDef',
    'TransformDef',
    'RouteDef',
    'UdfDef',
    'ModelDef',
    'to_yaml',
    'save_yaml',
]

__version__ = '0.1.0'
