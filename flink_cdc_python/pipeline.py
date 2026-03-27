from __future__ import annotations

from pathlib import Path
from typing import Union, Self, Any, Callable

_OptionTypes = Union[str, int, float, bool]


class Pipeline:

    @staticmethod
    def create(pipeline_name: str, **pipeline_options: _OptionTypes) -> _PipelineStart:
        ...

    def with_java_udf(self, udf_name, class_path: str) -> Self:
        ...

    def with_python_udf(self, udf_name, func: Callable[..., Any]) -> Self:
        ...

    def with_ai_model(self, model_name, class_name: str, **options: _OptionTypes) -> Self:
        ...

    def describe(self) -> dict[str, Any]:
        ...

    def execute(self, *, with_local_cluster: bool = False):
        ...


class _PipelineStart(Pipeline):

    def from_values(self, data: list[tuple[Any]]) -> _PipelineIntermediate:
        ...

    def from_csv(self, file: Path | str) -> _PipelineIntermediate:
        ...

    def from_source(self, name: str, **source_options: _OptionTypes) -> _PipelineIntermediate:
        ...


class _PipelineIntermediate(Pipeline):

    def transform(self, source_table: str, *,
                  projection: str | None = None,
                  filtering: str | None = None,
                  primary_keys: list[str] | None = None,
                  partition_keys: list[str] | None = None,
                  table_options: dict[str, str] | None = None) -> _PipelineIntermediate:
        ...

    def route(self, source_table: str, sink_table: str, *, replace_symbol: str | None = None) -> _PipelineIntermediate:
        ...

    def to_sink(self, name: str, **sink_options: _OptionTypes) -> Pipeline:
        ...

    def to_csv(self, file: Path | str) -> Pipeline:
        ...

    def collect(self) -> Pipeline:
        ...

    def print(self) -> Pipeline:
        return self.to_sink('values', print_enabled=True)
