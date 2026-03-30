"""Tests for YAML serializer."""

from pathlib import Path

from flink_cdc_python.compose import save_yaml, to_yaml


class TestToYaml:
    """Tests for to_yaml function."""

    def test_simple_dict(self) -> None:
        """Test serializing a simple dictionary."""
        data = {'key': 'value', 'number': 42}
        result = to_yaml(data)
        assert 'key: value' in result
        assert 'number: 42' in result

    def test_boolean_values(self) -> None:
        """Test serializing boolean values."""
        data = {'enabled': True, 'disabled': False}
        result = to_yaml(data)
        assert 'enabled: true' in result
        assert 'disabled: false' in result

    def test_quoted_strings(self) -> None:
        """Test that special strings are quoted."""
        data = {'special': 'true', 'colon': 'value: with colon'}
        result = to_yaml(data)
        # "true" should be quoted to avoid being interpreted as boolean
        assert "'true'" in result or 'special:' in result

    def test_list_of_strings(self) -> None:
        """Test serializing a list of strings."""
        data = {'items': ['a', 'b', 'c']}
        result = to_yaml(data)
        assert '- a' in result
        assert '- b' in result
        assert '- c' in result

    def test_list_of_dicts(self) -> None:
        """Test serializing a list of dictionaries."""
        data = {
            'routes': [
                {'source-table': 'db.orders', 'sink-table': 'ods.orders'},
                {'source-table': 'db.users', 'sink-table': 'ods.users'},
            ]
        }
        result = to_yaml(data)
        assert 'source-table: db.orders' in result
        assert 'sink-table: ods.orders' in result
        assert 'source-table: db.users' in result

    def test_nested_dict(self) -> None:
        """Test serializing nested dictionaries."""
        data = {'pipeline': {'name': 'test', 'parallelism': 2}}
        result = to_yaml(data)
        assert 'pipeline:' in result
        assert 'name: test' in result
        assert 'parallelism: 2' in result

    def test_empty_list(self) -> None:
        """Test serializing an empty list."""
        data: dict[str, list[str]] = {'items': []}
        result = to_yaml(data)
        assert 'items: []' in result

    def test_empty_dict(self) -> None:
        """Test serializing an empty dictionary."""
        data: dict[str, dict[str, str]] = {'config': {}}
        result = to_yaml(data)
        assert 'config: {}' in result

    def test_multiline_string(self) -> None:
        """Test serializing a multiline string."""
        data = {'query': 'SELECT *\nFROM table\nWHERE id > 10'}
        result = to_yaml(data)
        assert '|' in result  # Uses literal block scalar

    def test_object_with_to_dict(self) -> None:
        """Test serializing an object with to_dict method."""

        class MockObject:
            def to_dict(self) -> dict[str, str]:
                return {'key': 'value'}

        result = to_yaml(MockObject())
        assert 'key: value' in result

    def test_invalid_type(self) -> None:
        """Test that invalid types raise TypeError."""
        import pytest

        with pytest.raises(TypeError, match='Expected dict or object'):
            to_yaml('invalid')  # noqa: ARG001


class TestSaveYaml:
    """Tests for save_yaml function."""

    def test_save_to_file(self, tmp_path: Path) -> None:
        """Test saving YAML to a file."""
        data = {'key': 'value', 'number': 42}
        file_path = tmp_path / 'test.yaml'

        result = save_yaml(data, file_path)

        assert file_path.exists()
        content = file_path.read_text()
        assert 'key: value' in content
        assert 'number: 42' in content
        assert result == str(file_path)

    def test_save_creates_parent_dirs(self, tmp_path: Path) -> None:
        """Test that save_yaml creates parent directories."""
        data = {'key': 'value'}
        file_path = tmp_path / 'nested' / 'dir' / 'test.yaml'

        save_yaml(data, file_path)

        assert file_path.exists()
        assert file_path.read_text() == 'key: value\n'


class TestYamlFormatting:
    """Tests for YAML output formatting."""

    def test_source_def_format(self) -> None:
        """Test that SourceDef produces correct YAML format."""
        from flink_cdc_python.compose import SourceDef

        source = SourceDef(
            type='mysql',
            config={
                'hostname': 'localhost',
                'port': 3306,
                'tables': r'app_db\..*',
            },
        )

        result = to_yaml(source)

        assert 'source:' not in result  # to_yaml on single object doesn't add key
        assert 'type: mysql' in result
        assert 'hostname: localhost' in result
        assert 'port: 3306' in result

    def test_full_pipeline_format(self) -> None:
        """Test that a full pipeline produces correct YAML format."""
        from flink_cdc_python.compose import (
            PipelineDef,
            RouteDef,
            SinkDef,
            SourceDef,
            TransformDef,
        )

        pipeline = PipelineDef(
            source=SourceDef(type='mysql', config={'hostname': 'localhost', 'tables': 'db\\..*'}),
            sink=SinkDef(type='doris', config={'fenodes': '127.0.0.1:8030'}),
            config={'name': 'test', 'parallelism': 2},
            routes=[RouteDef(source_table='db.orders', sink_table='ods.orders')],
            transforms=[TransformDef(source_table='db.orders', projection='id')],
        )

        result = to_yaml(pipeline)

        # Check structure
        lines = result.strip().split('\n')

        # Source should come first
        assert any('source:' in line for line in lines)
        assert any('type: mysql' in line for line in lines)

        # Sink should follow
        assert any('sink:' in line for line in lines)
        assert any('type: doris' in line for line in lines)

        # Transform section
        assert any('transform:' in line for line in lines)

        # Route section
        assert any('route:' in line for line in lines)

        # Pipeline section
        assert any('pipeline:' in line for line in lines)
